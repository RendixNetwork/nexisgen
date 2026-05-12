"""Nexisgen package."""

from pathlib import Path

__all__ = ["__version__", "REPO_ROOT", "MODELS_DIR", "CONFIG_JSON_PATH"]

__version__ = "0.2.0"

# Repo-root resolution: this file lives at <repo>/nexis/__init__.py.
REPO_ROOT: Path = Path(__file__).resolve().parent.parent

# Trainer assets bundled at the repo root.
MODELS_DIR: Path = REPO_ROOT / "models"
CONFIG_JSON_PATH: Path = REPO_ROOT / "config.json"
