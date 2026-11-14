"""Helpers to load the hyphenated ml-service package in tests."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "ml-service" / "app.py"


def load_adapter_app(service=None, token: str = "test-token"):
    """Load ml-service/app.py as a module and configure it for tests."""
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    # Ensure a fresh module load so global state is controllable.
    module_name = "ml_service_app_under_test"
    if module_name in sys.modules:
        del sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, APP_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load ML adapter from {APP_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    module.INTERNAL_TOKEN = token
    module._scoring_service = service
    return module.app
