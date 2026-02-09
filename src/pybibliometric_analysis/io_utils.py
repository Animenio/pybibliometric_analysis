from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd


def detect_parquet_support() -> bool:
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        return False
    return True


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def ensure_parent_dir(path: Path) -> None:
    ensure_dir(path.parent)


def _lazy_pandas():
    import pandas as pd

    return pd


def write_table(df: "pd.DataFrame", path_base: Path, prefer_parquet: bool = True) -> dict:
    logger = logging.getLogger("pybibliometric_analysis")
    forced_suffix = path_base.suffix if path_base.suffix in {".parquet", ".csv"} else None
    base = _normalize_base_path(path_base)
    ensure_parent_dir(base)

    if forced_suffix == ".csv":
        csv_path = base.with_suffix(".csv")
        df.to_csv(csv_path, index=False)
        return {"path": str(csv_path), "format": "csv"}

    if not prefer_parquet or not detect_parquet_support():
        csv_path = base.with_suffix(".csv")
        logger.warning("Parquet engine unavailable; wrote CSV instead: %s", csv_path)
        df.to_csv(csv_path, index=False)
        return {"path": str(csv_path), "format": "csv"}

    try:
        parquet_path = base.with_suffix(".parquet")
        df.to_parquet(parquet_path, index=False, engine="pyarrow")
        return {"path": str(parquet_path), "format": "parquet"}
    except (ImportError, ValueError, AttributeError) as exc:
        csv_path = base.with_suffix(".csv")
        logger.warning(
            "Parquet write failed (%s). Wrote CSV instead: %s", exc.__class__.__name__, csv_path
        )
        df.to_csv(csv_path, index=False)
        return {"path": str(csv_path), "format": "csv"}


def read_table(path_base: Path) -> "pd.DataFrame":
    pd = _lazy_pandas()
    logger = logging.getLogger("pybibliometric_analysis")
    path = _normalize_base_path(path_base)

    if path.suffix == ".csv" and path.exists():
        return pd.read_csv(path)

    if path.suffix == ".parquet" and path.exists():
        if not detect_parquet_support():
            csv_path = path.with_suffix(".csv")
            if csv_path.exists():
                logger.warning("Parquet engine unavailable; reading CSV instead: %s", csv_path)
                return pd.read_csv(csv_path)
            raise RuntimeError("Parquet engine unavailable and no CSV fallback found.")
        return pd.read_parquet(path, engine="pyarrow")

    if path.suffix:
        raise FileNotFoundError(f"File not found: {path}")

    parquet_path = path.with_suffix(".parquet")
    csv_path = path.with_suffix(".csv")
    if parquet_path.exists():
        if not detect_parquet_support() and csv_path.exists():
            logger.warning("Parquet engine unavailable; reading CSV instead: %s", csv_path)
            return pd.read_csv(csv_path)
        if not detect_parquet_support():
            raise RuntimeError("Parquet engine unavailable and no CSV fallback found.")
        return pd.read_parquet(parquet_path, engine="pyarrow")
    if csv_path.exists():
        return pd.read_csv(csv_path)

    raise FileNotFoundError(f"No parquet/csv file found for base path: {path}")


def write_json(data: Any, path: Path) -> None:
    ensure_parent_dir(path)
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_base_path(path: Path) -> Path:
    if path.suffix in {".parquet", ".csv"}:
        return path.with_suffix("")
    return path
