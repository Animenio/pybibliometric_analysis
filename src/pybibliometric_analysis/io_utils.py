from __future__ import annotations

import logging
from importlib.util import find_spec
from pathlib import Path
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def detect_parquet_engine() -> Optional[str]:
    if find_spec("pyarrow"):
        return "pyarrow"
    if find_spec("fastparquet"):
        return "fastparquet"
    return None


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _lazy_pandas():
    import pandas as pd

    return pd


def write_table(df: "pd.DataFrame", path: Path) -> Path:
    logger = logging.getLogger("pybibliometric_analysis")
    ensure_parent_dir(path)

    if path.suffix != ".parquet":
        csv_path = path.with_suffix(".csv")
        df.to_csv(csv_path, index=False)
        return csv_path

    engine = detect_parquet_engine()
    if engine is None:
        csv_path = path.with_suffix(".csv")
        logger.warning("Parquet engine unavailable; wrote CSV instead: %s", csv_path)
        df.to_csv(csv_path, index=False)
        return csv_path

    try:
        df.to_parquet(path, index=False, engine=engine)
        return path
    except (ImportError, ValueError, AttributeError) as exc:
        csv_path = path.with_suffix(".csv")
        logger.warning(
            "Parquet write failed (%s). Wrote CSV instead: %s", exc.__class__.__name__, csv_path
        )
        df.to_csv(csv_path, index=False)
        return csv_path


def read_table(path: Path) -> "pd.DataFrame":
    pd = _lazy_pandas()
    logger = logging.getLogger("pybibliometric_analysis")

    if path.suffix == ".csv":
        return pd.read_csv(path)

    if path.suffix == ".parquet":
        engine = detect_parquet_engine()
        if engine is None:
            csv_path = path.with_suffix(".csv")
            if csv_path.exists():
                logger.warning("Parquet engine unavailable; reading CSV instead: %s", csv_path)
                return pd.read_csv(csv_path)
            raise RuntimeError("Parquet engine unavailable and no CSV fallback found.")
        return pd.read_parquet(path, engine=engine)

    raise ValueError(f"Unsupported file extension: {path.suffix}")
