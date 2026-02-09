from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd


def detect_parquet_support() -> bool:
    """Return True only when a parquet engine is importable.

    We treat *any* import-time exception as lack of support, since ABI issues can
    raise non-ImportError exceptions.
    """

    try:
        import pyarrow  # noqa: F401
    except Exception:
        return False
    return True


def _lazy_pandas():
    import pandas as pd

    return pd


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def ensure_parent_dir(path: Path) -> None:
    ensure_dir(path.parent)


def write_table(base_path: Path, df: "pd.DataFrame", preferred_format: str = "parquet") -> dict:
    """Write a table to parquet or csv with automatic fallback.

    Args:
        base_path: Path without suffix (preferred). If a suffix is provided, it is ignored.
        df: DataFrame to write.
        preferred_format: "parquet" (default) or "csv".

    Returns:
        Metadata dict with at least: {"path": str, "format": "csv"|"parquet"}.
    """

    logger = logging.getLogger("pybibliometric_analysis")
    base = base_path
    if base.suffix in {".parquet", ".csv"}:
        base = base.with_suffix("")
    ensure_parent_dir(base)

    preferred_format = preferred_format.lower()
    if preferred_format not in {"parquet", "csv"}:
        raise ValueError("preferred_format must be 'parquet' or 'csv'")

    if preferred_format == "parquet" and detect_parquet_support():
        parquet_path = base.with_suffix(".parquet")
        try:
            df.to_parquet(parquet_path, index=False, engine="pyarrow")
            return {"path": str(parquet_path), "format": "parquet"}
        except Exception as exc:
            logger.warning(
                "Parquet write failed (%s). Falling back to CSV.", exc.__class__.__name__
            )

    csv_path = base.with_suffix(".csv")
    df.to_csv(csv_path, index=False)
    return {"path": str(csv_path), "format": "csv"}


def read_table(path_or_base: Path) -> "pd.DataFrame":
    """Read a table from parquet/csv.

    - If a suffix is provided (.csv/.parquet), read exactly that file.
    - If no suffix is provided, try {base}.parquet then {base}.csv.
    """

    pd = _lazy_pandas()
    logger = logging.getLogger("pybibliometric_analysis")

    if path_or_base.suffix in {".csv", ".parquet"}:
        if not path_or_base.exists():
            raise FileNotFoundError(f"File not found: {path_or_base}")
        if path_or_base.suffix == ".csv":
            return pd.read_csv(path_or_base)
        if not detect_parquet_support():
            raise RuntimeError("Parquet engine unavailable; install the 'parquet' extra.")
        return pd.read_parquet(path_or_base, engine="pyarrow")

    parquet_path = path_or_base.with_suffix(".parquet")
    csv_path = path_or_base.with_suffix(".csv")

    if parquet_path.exists():
        if not detect_parquet_support():
            if csv_path.exists():
                logger.warning("Parquet engine unavailable; reading CSV instead: %s", csv_path)
                return pd.read_csv(csv_path)
            raise RuntimeError("Parquet engine unavailable and no CSV fallback found.")
        try:
            return pd.read_parquet(parquet_path, engine="pyarrow")
        except Exception as exc:
            if csv_path.exists():
                logger.warning(
                    "Parquet read failed (%s). Reading CSV instead: %s",
                    exc.__class__.__name__,
                    csv_path,
                )
                return pd.read_csv(csv_path)
            raise

    if csv_path.exists():
        return pd.read_csv(csv_path)

    raise FileNotFoundError(f"No parquet/csv file found for base path: {path_or_base}")


def write_json(data: Any, path: Path) -> None:
    ensure_parent_dir(path)
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))
