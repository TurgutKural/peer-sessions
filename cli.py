"""``hermes peer-sessions``: the terminal twin of the ``peer_message`` tool.

Sessions that were already open when the plugin loaded keep their tool list, so they message
peers from their terminal tool. The sender is the session that runs the command
(``HERMES_SESSION_ID``, which Hermes sets on every terminal command); outside a session the
message is labelled as coming from a terminal.
"""
from __future__ import annotations

import json
import os
import sys
import time


def setup_parser(parser) -> None:
    sub = parser.add_subparsers(dest="peer_command", metavar="<command>")
    send = sub.add_parser("send", help="Send a message to an open session",
                          description="Send a message to another session of this profile. "
                                      "Use '-' as the message to read it from stdin.")
    send.add_argument("to", help="Session title, id, or '*' for every open session")
    send.add_argument("message", nargs="+", help="Message text ('-' reads stdin)")
    send.add_argument("--json", action="store_true", help="Print the raw result")
    lst = sub.add_parser("list", help="List open sessions of this profile")
    lst.add_argument("--json", action="store_true", help="Print the raw result")
    log = sub.add_parser("log", help="Show recent message events")
    log.add_argument("-n", type=int, default=20, help="How many events (default 20)")


def make_handler(service):
    def handle(args) -> int:
        command = getattr(args, "peer_command", None)
        self_id = os.environ.get("HERMES_SESSION_ID", "")
        if command == "send":
            text = " ".join(args.message)
            if text.strip() == "-":
                text = sys.stdin.read()
            result = service.send(self_id, args.to, text)
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            else:
                _print_send(result)
            return 1 if result.get("error") or not result.get("ok", True) else 0
        if command == "list":
            result = service.list(self_id)
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
                return 0
            if result.get("you"):
                print(f"you: {result['you']}")
            if not result["sessions"]:
                print("no other open session in this profile")
            for s in result["sessions"]:
                print(f"  {s['name']:<28} {s['state']:<5} {s['surface']:<6} {s['last_active']:<9} {s['id']}")
            return 0
        if command == "log":
            for e in service.box.tail_log(max(1, args.n)):
                when = time.strftime("%m-%d %H:%M:%S", time.localtime(e.get("at", 0)))
                extra = e.get("via") or e.get("reason") or ""
                print(f"{when}  {e.get('event', ''):<9} {e.get('from', '')} -> {e.get('to', '')}  "
                      f"{e.get('chars', 0)} chars  {extra}".rstrip())
            return 0
        print("usage: hermes peer-sessions {send <to> <message>|list|log}")
        return 0

    return handle


def _print_send(result: dict) -> None:
    if result.get("error"):
        print(f"✗ {result['error']}", file=sys.stderr)
        return
    for r in result.get("results") or [result]:
        mark = "✓" if r.get("ok", True) else "✗"
        print(f"{mark} {r.get('to', '?')}: {r.get('status', '?')} — {r.get('detail', '')}")
