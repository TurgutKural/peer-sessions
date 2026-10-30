"""Delivery paths: steer into a running turn, next turn, wake, held/retry, expiry, exactly-once,
and the multi-process rules (only the hosting process wakes a local session)."""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time

KEY_B = "agent:test:local:dm:local-bbb222"


def _two(home):
    home.add_session("local-aaa111", "mmdh2")
    home.add_session("local-bbb222", "mmdh3")


def test_idle_recipient_is_woken_through_inject(service, home, injector):
    _two(home)
    injector.live.add(KEY_B)
    out = service.send("local-aaa111", "mmdh3", "CODE_HI is the .rsrc VA")
    assert out["ok"] and out["status"] == "delivered_new_turn"
    content, key = injector.calls[-1]
    assert key == KEY_B
    assert '[PEER SESSION MESSAGE from "mmdh2" (session local-aaa111)' in content
    assert "CODE_HI is the .rsrc VA" in content and content.rstrip().endswith("[/PEER SESSION MESSAGE]")
    assert service.box.pending() == []


def test_busy_recipient_gets_it_after_its_next_tool_call(service, home, injector):
    _two(home)
    home.set_busy("local-bbb222")
    out = service.send("local-aaa111", "mmdh3", "stop searching CORE 85")
    assert out["status"] == "queued_next_tool_call"
    assert injector.calls == []
    assert service.deliverer.take("local-aaa111") == []          # other sessions get nothing
    frames = service.deliverer.take("local-bbb222")
    assert len(frames) == 1 and "stop searching CORE 85" in frames[0]
    assert service.deliverer.take("local-bbb222") == []           # exactly once
    assert service.box.pending() == []
    assert service.box.read_status(out["id"])["status"] == "delivered"


def test_hooks_deliver_in_a_process_that_cannot_wake(core, home, injector, make):
    """Hooks run wherever the agent loop runs; they need no injector and no ownership."""
    _two(home)
    home.set_busy("local-bbb222")
    sender = make(local=False, remote=False)
    out = sender.send("local-aaa111", "mmdh3", "x")
    assert out["status"] == "queued" and out["to"] == "mmdh3"     # nobody ticked: honest answer
    assert len(sender.deliverer.take("local-bbb222")) == 1


def test_turn_end_wakes_when_the_tool_boundary_never_came(service, home, injector):
    _two(home)
    home.set_busy("local-bbb222")
    out = service.send("local-aaa111", "mmdh3", "hello")
    assert out["status"] == "queued_next_tool_call"
    injector.live.add(KEY_B)
    home.set_busy("local-bbb222", False)
    service.deliverer.turn_ended("local-bbb222")
    service.deliverer.tick()
    assert len(injector.calls) == 1 and service.box.pending() == []


def test_interrupted_turn_is_not_woken_immediately(service, home, injector):
    _two(home)
    injector.live.add(KEY_B)
    service.deliverer.turn_ended("local-bbb222", interrupted=True)
    out = service.send("local-aaa111", "mmdh3", "later")
    assert out["status"] == "queued_next_turn" and injector.calls == []
    assert len(service.deliverer.take("local-bbb222")) == 1       # pre_llm_call of its next turn


def test_only_the_hosting_process_wakes_a_local_session(core, home, injector, make):
    home.add_session("local-aaa111", "mmdh2")
    home.add_session("local-bbb222", "mmdh3", host_pid=1)         # hosted by another live process
    injector.live.add(KEY_B)
    other_tui = make(local=True)
    gateway = make(local=False, remote=True)
    out = gateway.send("local-aaa111", "mmdh3", "x")
    assert out["status"] == "queued"
    other_tui.deliverer.tick()
    gateway.deliverer.tick()
    assert injector.calls == [] and len(other_tui.box.pending()) == 1  # neither asked an injector


def test_gateway_wakes_messaging_sessions(core, home, injector, make):
    home.add_session("local-aaa111", "mmdh2")
    home.add_session("tg-555", "telegram chat", live=False, source="telegram")
    injector.live.add("agent:test:local:dm:tg-555")
    gateway = make(local=False, remote=True)
    out = gateway.send("local-aaa111", "telegram chat", "x")
    assert out["status"] == "delivered_new_turn"


def test_closed_recipient_is_held_and_retried(service, home, injector):
    home.add_session("local-aaa111", "mmdh2")
    home.add_session("local-ccc333", "old-lane", live=False)
    out = service.send("local-aaa111", "old-lane", "for when you are back")
    assert out["status"] == "held" and len(service.box.pending()) == 1
    home.open("local-ccc333")
    injector.live.add("agent:test:local:dm:local-ccc333")
    service.deliverer.tick()                                     # before the retry time: nothing
    assert injector.calls == []
    service.deliverer._next_try.clear()
    service.deliverer.tick()
    assert len(injector.calls) == 1 and service.box.pending() == []


def test_held_message_expires(service, home, core):
    home.add_session("local-aaa111", "mmdh2")
    home.add_session("local-ccc333", "old-lane", live=False)
    service.send("local-aaa111", "old-lane", "x")
    path, msg = service.box.pending()[0]
    msg["created_at"] -= core.HOLD_TTL_S + 1
    path.write_text(json.dumps(msg))
    service.deliverer.tick()
    assert service.box.pending() == []
    assert any(e["event"] == "expired" for e in service.box.tail_log())


def test_terminal_send_is_delivered_by_a_ticking_process(core, home, injector, service):
    """A CLI process cannot wake: it writes the queue and waits for the host's status."""
    _two(home)
    injector.live.add(KEY_B)
    cli_side = core.PeerService(home.path)
    ticker = threading.Thread(target=lambda: (time.sleep(0.5), service.deliverer.tick()))
    ticker.start()
    out = cli_side.send("local-aaa111", "mmdh3", "from the terminal")
    ticker.join()
    assert out["status"] == "delivered_new_turn"


def test_exactly_once_under_concurrent_takers(service, home):
    _two(home)
    home.set_busy("local-bbb222")
    for i in range(10):  # within RATE_MAX
        service.send("local-aaa111", "mmdh3", f"m{i}")
    got: list[str] = []
    threads = [threading.Thread(target=lambda: got.extend(service.deliverer.take("local-bbb222")))
               for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(int(f.split("\n\n")[1].split("\n")[0][1:]) for f in got) == list(range(10))


def test_a_claimed_message_is_out_of_the_hooks_reach_and_recovered_if_stranded(service, home):
    _two(home)
    home.set_busy("local-bbb222")
    service.send("local-aaa111", "mmdh3", "m")
    path, _ = service.box.pending()[0]
    claimed = service.box.claim(path)
    assert service.deliverer.take("local-bbb222") == []
    dead = claimed.with_name(claimed.name.replace(f"-{os.getpid()}-", "-999999999-", 1))
    os.rename(claimed, dead)                                     # its process died mid-wake
    service.box.recover_claims()
    assert len(service.deliverer.take("local-bbb222")) == 1


def test_take_respects_the_size_budget(service, home):
    _two(home)
    home.set_busy("local-bbb222")
    service.send("local-aaa111", "mmdh3", "a" * 3000)
    assert service.deliverer.take("local-bbb222", budget=1000) == []
    assert len(service.deliverer.take("local-bbb222")) == 1


def test_messages_follow_compression(service, home):
    home.add_session("local-aaa111", "mmdh2")
    home.add_session("local-bbb222", "mmdh3", end_reason="compression")
    home.add_session("child-ccc", "mmdh3 #2", live=False, parent="local-bbb222")
    home.set_busy("child-ccc")
    out = service.send("local-aaa111", "local-bbb222", "after compaction")
    assert out["status"] == "queued_next_tool_call" and out["to_id"] == "child-ccc"
    assert len(service.deliverer.take("child-ccc")) == 1


def test_queued_message_is_retargeted_when_the_recipient_compacts(service, home):
    _two(home)
    home.set_busy("local-bbb222")
    service.send("local-aaa111", "mmdh3", "before compaction")
    conn = sqlite3.connect(home.path / "state.db")
    conn.execute("UPDATE sessions SET end_reason = 'compression' WHERE id = 'local-bbb222'")
    conn.commit()
    conn.close()
    home.add_session("child-ddd", "mmdh3 #2", live=False, parent="local-bbb222")
    home.set_busy("child-ddd")
    service.deliverer.tick()
    assert service.deliverer.take("local-bbb222") == []
    assert len(service.deliverer.take("child-ddd")) == 1


def test_broadcast_reaches_every_other_open_session(service, home, injector):
    _two(home)
    home.add_session("local-ddd444", "sinq")
    injector.live |= {KEY_B, "agent:test:local:dm:local-ddd444"}
    out = service.send("local-aaa111", "*", "VM restarting in 5 min")
    assert out["broadcast"] and {r["to"] for r in out["results"]} == {"mmdh3", "sinq"}
    assert all("broadcast to every open session" in c for c, _ in injector.calls)


def test_no_injection_grant_falls_back_to_next_turn(core, home, injector, make):
    _two(home)
    svc = make(allowed=False)
    out = svc.send("local-aaa111", "mmdh3", "x")
    assert out["status"] == "queued_next_turn" and injector.calls == []
    assert len(svc.deliverer.take("local-bbb222")) == 1
