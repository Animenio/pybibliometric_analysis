from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from pybibliometric_analysis.io_utils import read_table, write_json, write_table

if TYPE_CHECKING:
    import pandas as pd


def _lazy_pandas():
    import pandas as pd

    return pd


def setup_logging(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pybibliometric_analysis.clean")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.handlers = []
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def _latest_raw_file(base_dir: Path) -> Path:
    raw_dir = base_dir / "data" / "raw"
    candidates = list(raw_dir.glob("scopus_search_*.parquet")) + list(
        raw_dir.glob("scopus_search_*.csv")
    )
    if not candidates:
        raise FileNotFoundError("No raw scopus_search_* files found in data/raw.")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _extract_run_id(path: Path) -> str:
    name = path.stem
    if name.startswith("scopus_search_"):
        return name.replace("scopus_search_", "", 1)
    if name.startswith("scopus_clean_"):
        return name.replace("scopus_clean_", "", 1)
    return name


def _coerce_year(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 4 and text[:4].isdigit():
        return int(text[:4])
    return None


def _derive_pub_year(df: "pd.DataFrame") -> "pd.DataFrame":
    pd = _lazy_pandas()
    if "pub_year" not in df.columns:
        df["pub_year"] = pd.NA
    cover_col = "coverDate" if "coverDate" in df.columns else "cover_date"
    if cover_col in df.columns:
        cover_years = df[cover_col].apply(_coerce_year)
    else:
        cover_years = pd.Series([None] * len(df))
    df["pub_year"] = df["pub_year"].fillna(cover_years)
    return df


def _fill_author_names(df: "pd.DataFrame") -> "pd.DataFrame":
    pd = _lazy_pandas()
    if "author_names" not in df.columns:
        df["author_names"] = pd.NA
    fallback_col = None
    for candidate in ("creator", "dc:creator"):
        if candidate in df.columns:
            fallback_col = candidate
            break
    if fallback_col:
        df["author_names"] = df["author_names"].fillna(df[fallback_col])
    return df


def _normalize_journal(df: "pd.DataFrame") -> "pd.DataFrame":
    col = None
    for candidate in ("prism:publicationName", "publicationName", "journal", "sourceTitle"):
        if candidate in df.columns:
            col = candidate
            break
    if col:
        df[col] = df[col].astype(str).str.strip()
    return df


def _split_and_count(series: "pd.Series", sep: str = ";") -> "pd.DataFrame":
    pd = _lazy_pandas()
    items = []
    for value in series.dropna().astype(str):
        parts = [part.strip() for part in value.split(sep) if part.strip()]
        items.extend(parts)
    if not items:
        return pd.DataFrame(columns=["item", "count"])
    counts = pd.Series(items).value_counts().reset_index()
    counts.columns = ["item", "count"]
    return counts


def run_clean(
    *,
    run_id: Optional[str],
    base_dir: Path,
    input_path: Optional[Path],
    force: bool,
    write_format: str,
) -> None:
    base_dir = base_dir.resolve()
    log_path = base_dir / "logs" / f"clean_{run_id or 'latest'}.log"
    logger = setup_logging(log_path)

    if not run_id:
        raise ValueError("--run-id is required for clean.")

    if input_path:
        raw_path = input_path
        if not raw_path.exists():
            raise FileNotFoundError(f"Raw file not found: {raw_path}")
    else:
        raw_path = base_dir / "data" / "raw" / f"scopus_search_{run_id}"
        if not (raw_path.with_suffix(".parquet").exists() or raw_path.with_suffix(".csv").exists()):
            raise FileNotFoundError(f"Raw file not found for run_id={run_id}: {raw_path}")

    resolved_run_id = run_id or _extract_run_id(raw_path)
    logger.info("Cleaning run_id=%s input=%s", resolved_run_id, raw_path)

    df = read_table(raw_path)
    original_rows = len(df)

    if "eid" in df.columns:
        df = df.drop_duplicates(subset=["eid"])
    deduped_rows = len(df)

    df = _derive_pub_year(df)
    df = _fill_author_names(df)
    df = _normalize_journal(df)

    output_base = base_dir / "data" / "processed" / f"scopus_clean_{resolved_run_id}"

    parquet_path = output_base.with_suffix(".parquet")
    csv_path = output_base.with_suffix(".csv")
    if (parquet_path.exists() or csv_path.exists()) and not force:
        raise FileExistsError(f"Output already exists: {parquet_path} or {csv_path}")

    if write_format == "csv":
        cleaned_path = write_table(df, output_base.with_suffix(".csv"))
    elif write_format == "parquet":
        cleaned_path = write_table(df, output_base.with_suffix(".parquet"))
    else:
        cleaned_path = write_table(df, output_base)

    analysis_dir = base_dir / "outputs" / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)

    pubs_by_year = (
        df.dropna(subset=["pub_year"])
        .groupby("pub_year")
        .size()
        .reset_index(name="count")
        .sort_values("pub_year")
    )
    pubs_by_year.to_csv(analysis_dir / f"pubs_by_year_{resolved_run_id}.csv", index=False)

    journal_col = next(
        (
            c
            for c in ("prism:publicationName", "publicationName", "journal", "sourceTitle")
            if c in df.columns
        ),
        None,
    )
    if journal_col:
        journals = (
            df[journal_col]
            .dropna()
            .astype(str)
            .str.strip()
            .value_counts()
            .reset_index()
        )
        journals.columns = ["journal", "count"]
    else:
        journals = _lazy_pandas().DataFrame(columns=["journal", "count"])
        logger.warning("Journal column not found; top_journals will be empty.")
    journals.to_csv(analysis_dir / f"top_journals_{resolved_run_id}.csv", index=False)

    authors = _split_and_count(df["author_names"]) if "author_names" in df.columns else None
    if authors is None or authors.empty:
        authors = _lazy_pandas().DataFrame(columns=["item", "count"])
        logger.warning("Author names missing; top_authors will be empty.")
    authors.to_csv(analysis_dir / f"top_authors_{resolved_run_id}.csv", index=False)

    keyword_col = next(
        (c for c in ("authkeywords", "keywords", "author_keywords") if c in df.columns),
        None,
    )
    if keyword_col:
        keywords = _split_and_count(df[keyword_col])
    else:
        keywords = _lazy_pandas().DataFrame(columns=["item", "count"])
        logger.warning("Keyword column not found; keyword_freq will be empty.")
    keywords.to_csv(analysis_dir / f"keyword_freq_{resolved_run_id}.csv", index=False)

    derived_fields = ["pub_year", "author_names"]
    coverage = {
        "pub_year": _coverage_stats(df, "pub_year"),
        "journal": _coverage_stats(df, journal_col)
        if journal_col
        else {"n_nonnull": 0, "n_total": len(df), "pct_nonnull": 0.0},
        "authors": _coverage_stats(df, "author_names"),
        "keywords": _coverage_stats(df, keyword_col)
        if keyword_col
        else {"n_nonnull": 0, "n_total": len(df), "pct_nonnull": 0.0},
    }
    manifest = {
        "schema_version": "1.0",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": resolved_run_id,
        "input_path": str(raw_path),
        "output_path": cleaned_path["path"],
        "output_format": cleaned_path["format"],
        "derived_fields": derived_fields,
        "duplicates_removed": original_rows - deduped_rows,
        "coverage": coverage,
        "output_tables": {
            "pubs_by_year": str(analysis_dir / f"pubs_by_year_{resolved_run_id}.csv"),
            "top_journals": str(analysis_dir / f"top_journals_{resolved_run_id}.csv"),
            "top_authors": str(analysis_dir / f"top_authors_{resolved_run_id}.csv"),
            "keyword_freq": str(analysis_dir / f"keyword_freq_{resolved_run_id}.csv"),
        },
        "notes": [],
    }
    if not keyword_col:
        manifest["notes"].append("Keyword column not found; keyword_freq is empty.")
    manifest_path = base_dir / "outputs" / "methods" / f"cleaning_manifest_{resolved_run_id}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(manifest, manifest_path)

    logger.info("Wrote cleaned data to %s", cleaned_path["path"])


def _coverage_stats(df: "pd.DataFrame", column: Optional[str]) -> dict:
    if not column or column not in df.columns:
        return {"n_nonnull": 0, "n_total": len(df), "pct_nonnull": 0.0}
    n_total = len(df)
    n_nonnull = int(df[column].notna().sum())
    pct_nonnull = (n_nonnull / n_total) if n_total else 0.0
    return {"n_nonnull": n_nonnull, "n_total": n_total, "pct_nonnull": pct_nonnull}
