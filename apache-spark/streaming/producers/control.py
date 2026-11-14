"""Pause / resume / stop a running replay producer, or show its saved offsets.

    python -m streaming.producers.control pause|resume|stop|status|reset
"""
from __future__ import annotations

import json
import sys

from common.config import load_config, path_of
from streaming.producers.replay_producer import Control


def main(cmd: str) -> None:
    state_dir = path_of(load_config(), "state_dir")
    ctl = Control(state_dir)
    if cmd in ("pause", "resume", "stop"):
        ctl.set({"pause": "PAUSED", "resume": "RUNNING", "stop": "STOP"}[cmd])
        print(f"producer control -> {ctl.state()}")
    elif cmd == "reset":
        (state_dir / "producer_offsets.json").unlink(missing_ok=True)
        print("offsets cleared: next run replays from row 0")
    else:
        offs = state_dir / "producer_offsets.json"
        print(json.dumps({"control": ctl.state(), "offsets": json.loads(offs.read_text()) if offs.exists() else {}}, indent=2))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "status")
