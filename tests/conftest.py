"""Fixtures: an isolated profile home with a minimal ``state.db`` and ``active_sessions.json``,
and the plugin package imported from this checkout (the directory name has a dash)."""
from __future__ import annotations

import importlib
import importlib.util
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load_package():
    name = "peer_sessions_under_test"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, ROOT / "__init__.py",
                                                  submodule_search_locations=[str(ROOT)])
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def pkg():
    return _load_package()


@pytest.fixture(scope="session")
def core(pkg):
    return importlib.import_module(f"{pkg.__name__}.core")


class Home:
    def __init__(self, path: Path):
        self.path = path
        (path / "runtime").mkdir(parents=True)
        conn = sqlite3.connect(path / "state.db")
        conn.executescript(
            "CREATE TABLE sessions (id TEXT PRIMARY KEY, source TEXT, title TEXT, session_key TEXT,"
            " parent_session_id TEXT, end_reason TEXT, started_at REAL, last_activity_at REAL);"
            "CREATE TABLE session_turn_leases (conversation_id TEXT PRIMARY KEY, holder TEXT,"
            " acquired_at REAL, expires_at REAL);")
        conn.commit()
        conn.close()
        self.active: list[dict] = []

    def add_session(self, sid: str, title: str, *, live: bool = True, parent: str = None,
                    end_reason: str = None, started: float = None, source: str = "local",
                    host_pid: int = None) -> None:
        now = time.time()
        conn = sqlite3.connect(self.path / "state.db")
        conn.execute("INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?)",
                     (sid, source, title, f"agent:test:local:dm:{sid}", parent, end_reason,
                      started or now, now))
        conn.commit()
        conn.close()
        if live:
            self.open(sid, host_pid=host_pid)

    def open(self, sid: str, host_pid: int = None) -> None:
        self.active.append({"session_id": sid, "pid": host_pid or os.getpid(), "updated_at": time.time()})
        (self.path / "runtime" / "active_sessions.json").write_text(json.dumps({"entries": self.active}))

    def set_busy(self, sid: str, busy: bool = True) -> None:
        conn = sqlite3.connect(self.path / "state.db")
        conn.execute("DELETE FROM session_turn_leases WHERE conversation_id = ?", (sid,))
        if busy:
            conn.execute("INSERT INTO session_turn_leases VALUES (?,?,?,?)",
                         (sid, "test", time.time(), time.time() + 300))
        conn.commit()
        conn.close()


@pytest.fixture(autouse=True)
def _short_cli_wait(core, monkeypatch):
    monkeypatch.setattr(core, "CLI_WAIT_S", 1.5)


@pytest.fixture
def home(tmp_path):
    return Home(tmp_path / "profile")


class FakeInjector:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []
        self.live: set[str] = set()  # session keys that accept

    def __call__(self, content: str, session_key: str) -> bool:
        self.calls.append((content, session_key))
        return session_key in self.live


@pytest.fixture
def injector():
    return FakeInjector()


def make_service(core, home, injector, *, local=True, remote=False, allowed=True):
    """A PeerService whose deliverer stands for one Hermes process: ``local`` = it hosts
    Desktop/TUI sessions (TUI injector), ``remote`` = it is the messaging gateway."""
    svc = core.PeerService(home.path)
    svc.deliverer = core.Deliverer(home.path, svc.box, svc.dir, inject=injector,
                                   can_wake_local=lambda: local, can_wake_remote=lambda: remote,
                                   injection_allowed=lambda: allowed)
    return svc


@pytest.fixture
def make(core, home, injector):
    """``make(local=..., remote=..., allowed=...)`` -> a PeerService for one simulated process."""
    return lambda **kw: make_service(core, home, injector, **kw)


@pytest.fixture
def service(make):
    return make()
