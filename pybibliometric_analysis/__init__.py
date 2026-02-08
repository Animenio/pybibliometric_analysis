from pkgutil import extend_path
from pathlib import Path

__path__ = extend_path(__path__, __name__)
_src_pkg = Path(__file__).resolve().parent.parent / "src" / "pybibliometric_analysis"
if _src_pkg.is_dir():
    __path__.append(str(_src_pkg))
