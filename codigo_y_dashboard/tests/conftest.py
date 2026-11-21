import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def _sin_claves(monkeypatch):
    """Ningun test depende de claves reales ni de servicios externos."""
    for v in ("ANTHROPIC_API_KEY", "BEDROCK_MODEL_ID", "GENAI_MODE", "S3_BUCKET", "MONGO_STRICT",
              "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_PROFILE"):
        monkeypatch.delenv(v, raising=False)


@pytest.fixture
def datos(tmp_path, monkeypatch):
    """Copia de data/raw en un directorio temporal; SIG_DATA_DIR y SIG_OUT_DIR apuntan ahi."""
    shutil.copytree(ROOT / "data" / "raw", tmp_path / "data" / "raw")
    monkeypatch.setenv("SIG_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("SIG_OUT_DIR", str(tmp_path / "out"))
    return tmp_path
