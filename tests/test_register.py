"""``register(ctx)`` against a recording context: tool, hooks, prompt section and CLI are wired,
and the hooks drive delivery end to end."""
from __future__ import annotations

import json
import threading
import time
from types import SimpleNamespace


class RecordingCtx:
    def __init__(self, home_path, injector):
        self._manager = SimpleNamespace(home_path=home_path, has_gateway_message_injector=True,
                                        has_tui_message_injector=False)
        self.tools, self.hooks, self.sections, self.cli = {}, {}, {}, {}
        self._injector = injector

    def register_tool(self, name, toolset, schema, handler, **kw):
        self.tools[name] = (toolset, schema, handler)

    def register_hook(self, name, cb, **kw):
        self.hooks.setdefault(name, []).append(cb)

    def register_system_prompt_section(self, sid, content, **kw):
        self.sections[sid] = content

    def register_cli_command(self, name, help, setup_fn, handler_fn=None, description=""):
        self.cli[name] = (setup_fn, handler_fn)

    def inject_message(self, content, role="user", *, session_key=None, origin=None):
        return self._injector(content, session_key)

    def _gateway_injection_allowed(self):
        return True

    def on_unload(self, cb):
        self.unload = cb


def test_register_wires_everything_and_hooks_deliver(pkg, home, injector):
    home.add_session("local-aaa111", "mmdh2")
    home.add_session("local-bbb222", "mmdh3")
    ctx = RecordingCtx(home.path, injector)
    pkg.register(ctx)
    assert set(ctx.tools) == {"peer_message"} and ctx.tools["peer_message"][0] == "peer-sessions"
    assert {"transform_tool_result", "pre_llm_call", "on_session_end"} <= set(ctx.hooks)
    assert "peer-sessions.channel" in ctx.sections and "peer-sessions" in ctx.cli

    handler = ctx.tools["peer_message"][2]
    listed = json.loads(handler({"action": "list"}, session_id="local-aaa111"))
    assert [s["name"] for s in listed["sessions"]] == ["mmdh3"]

    (on_tool,) = ctx.hooks["transform_tool_result"]
    home.set_busy("local-bbb222")                                # mmdh3 is mid-turn
    sent = json.loads(handler({"action": "send", "to": "mmdh3", "message": "ping"}, session_id="local-aaa111"))
    assert sent["status"] == "queued_next_tool_call", sent
    out = on_tool(result='{"output": "ls"}', session_id="local-bbb222")
    assert out.startswith('{"output": "ls"}') and "[PEER SESSION MESSAGE" in out and "ping" in out
    assert on_tool(result="{}", session_id="local-bbb222") is None

    (on_end,) = ctx.hooks["on_session_end"]
    on_end(session_id="local-bbb222", completed=True, interrupted=False)
    (on_start,) = ctx.hooks["pre_llm_call"]
    assert on_start(session_id="local-bbb222") is None
    ctx.unload()                                                 # reload/unload ends the tick thread
    deadline = time.monotonic() + 3
    while any(t.name == "peer-sessions-deliverer" for t in threading.enumerate()):
        assert time.monotonic() < deadline, "deliverer thread survived unload"
        time.sleep(0.05)


def test_huge_tool_results_are_left_alone(pkg, home, injector, core):
    home.add_session("local-aaa111", "mmdh2")
    home.add_session("local-bbb222", "mmdh3")
    ctx = RecordingCtx(home.path, injector)
    pkg.register(ctx)
    (on_tool,) = ctx.hooks["transform_tool_result"]
    home.set_busy("local-bbb222")
    ctx.tools["peer_message"][2]({"action": "send", "to": "mmdh3", "message": "m"}, session_id="local-aaa111")
    assert on_tool(result="x" * core.ATTACH_RESULT_MAX_CHARS, session_id="local-bbb222") is None
    assert "m" in on_tool(result="small", session_id="local-bbb222")
    ctx.unload()
