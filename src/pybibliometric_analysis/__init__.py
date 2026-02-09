"""Project package for Scopus extraction.

This package exposes a small, stable surface for the rest of the project and
re-exports commonly used submodules so test-suite imports like
`from pybibliometric_analysis import io_utils` continue to work after the
changes introduced by the PR.
"""

__version__ = "0.1.0"

# Re-export commonly used submodules for convenience and backward compatibility
from . import io_utils, settings

__all__ = [
	"__version__",
	"io_utils",
	"settings",
]
