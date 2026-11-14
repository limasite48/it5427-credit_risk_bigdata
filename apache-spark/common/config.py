"""Configuration loading: YAML files + environment overrides (CRB_<SECTION>_<KEY>)."""
from __future__ import annotations

import copy
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "configs"


def _coerce(value: str, current: Any) -> Any:
    if isinstance(current, bool):
        return value.lower() in {"1", "true", "yes", "y"}
    if isinstance(current, int):
        return int(value)
    if isinstance(current, float):
        return float(value)
    return value


def _apply_env_overrides(cfg: dict) -> dict:
    for section, values in cfg.items():
        if not isinstance(values, dict):
            continue
        for key, current in values.items():
            env_name = f"CRB_{section}_{key}".upper()
            if env_name in os.environ and not isinstance(current, dict):
                values[key] = _coerce(os.environ[env_name], current)
    return cfg


@lru_cache(maxsize=1)
def _load_raw() -> dict:
    with open(CONFIG_DIR / "pipeline.yaml", encoding="utf-8") as f:
        pipeline = yaml.safe_load(f)
    with open(CONFIG_DIR / "sources.yaml", encoding="utf-8") as f:
        sources = yaml.safe_load(f)
    pipeline["sources"] = sources["sources"]
    pipeline["reference_files"] = sources.get("reference_files", {})
    return pipeline


def load_config() -> dict:
    """Return a fresh, env-overridden copy of the configuration."""
    return _apply_env_overrides(copy.deepcopy(_load_raw()))


def resolve_path(p: str | os.PathLike) -> Path:
    """Resolve a config path relative to the project root (absolute paths are kept)."""
    path = Path(p)
    return path if path.is_absolute() else (PROJECT_ROOT / path)


def path_of(cfg: dict, key: str) -> Path:
    return resolve_path(cfg["paths"][key])
