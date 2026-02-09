from importlib.util import find_spec
from pathlib import Path
import sys


if find_spec("pybibliometric_analysis") is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
