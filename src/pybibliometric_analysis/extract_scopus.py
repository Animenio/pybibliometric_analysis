from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from importlib import import_module
from importlib.util import find_spec
from pathlib import Path
from typing import Any, Iterable, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from pybibliometric_analysis.io_utils import detect_parquet_engine, write_table
from pybibliometric_analysis.settings import build_manifest, init_pybliometrics, load_search_config, write_manifest

ScopusSearch = None


def _get_scopus_search_cls() -> Any:
    global ScopusSearch
    if ScopusSearch is not None:
        return ScopusSearch
    from pybliometrics.scopus import ScopusSearch as _ScopusSearch

    ScopusSearch = _ScopusSearch
    return ScopusSearch


def _lazy_pandas():
    import pandas as pd

    return pd


@dataclass
class ExtractPaths:
    raw_path: Path
    manifest_path: Path
    log_path: Path


def run_extract(
    *,
    run_id: str,
    config_path: Path,
    pybliometrics_config_dir: Path,
    scopus_api_key_file: Path,
    inst_token_file: Optional[Path],
    view: Optional[str],
    force_slicing: bool,
    base_dir: Path = Path("."),
) -> None:
    paths = build_paths(base_dir, run_id)
    logger = setup_logging(paths.log_path)

    if paths.raw_path.exists():
        raise FileExistsError(f"Raw dataset already exists: {paths.raw_path}")

    logger.info("Initializing pybliometrics configuration")
    init_pybliometrics(
        pybliometrics_config_dir,
        scopus_api_key_file,
        inst_token_file=inst_token_file,
        logger=logger,
    )

    config = load_search_config(config_path)
    logger.info("Loaded search config for database %s", config.database)

    estimate_search = retry_scopus_search(
        config.query,
        view=view,
        download=False,
        subscriber=False,
    )
    n_results = estimate_search.get_results_size()
    logger.info("Estimated %s results", n_results)

    if force_slicing:
        planned_strategy = "slicing"
    elif n_results > 5000 and config.use_cursor_preferred:
        planned_strategy = "cursor"
    else:
        planned_strategy = "normal"

    logger.info(
        "Extract run_id=%s query=%s n_results=%s strategy=%s raw_path=%s manifest_path=%s log_path=%s",
        run_id,
        config.query,
        n_results,
        planned_strategy,
        paths.raw_path,
        paths.manifest_path,
        paths.log_path,
    )

    if force_slicing:
        strategy = "slicing"
        records, years_covered = run_slicing(config.query, view)
    elif n_results > 5000 and config.use_cursor_preferred:
        records, years_covered, strategy = run_cursor_with_fallback(config.query, view)
    else:
        strategy = "normal"
        records = run_standard(config.query, view)
        years_covered = None

    if records.empty:
        logger.warning("No records downloaded for query")

    columns_present = list(records.columns)
    raw_output_path = write_table(records, paths.raw_path)
    logger.info("Saved raw data to %s", raw_output_path)

    manifest = build_manifest(
        run_id=run_id,
        query=config.query,
        database=config.database,
        n_results_estimated=n_results,
        n_records_downloaded=len(records),
        strategy_used=strategy,
        years_covered=years_covered,
        columns_present=columns_present,
    )
    manifest["raw_output_path"] = str(raw_output_path)
    manifest["parquet_engine"] = detect_parquet_engine()
    write_manifest(paths.manifest_path, manifest)
    logger.info("Wrote manifest to %s", paths.manifest_path)


def build_paths(base_dir: Path, run_id: str) -> ExtractPaths:
    raw_path = base_dir / "data" / "raw" / f"scopus_search_{run_id}.parquet"
    manifest_path = base_dir / "outputs" / "methods" / f"search_manifest_{run_id}.json"
    log_path = base_dir / "logs" / f"extract_{run_id}.log"

    raw_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    return ExtractPaths(raw_path=raw_path, manifest_path=manifest_path, log_path=log_path)


def setup_logging(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pybibliometric_analysis")
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


def run_standard(query: str, view: Optional[str]) -> pd.DataFrame:
    search = retry_scopus_search(query, view=view, subscriber=False)
    return to_frame(search.results or [])


def run_cursor_with_fallback(
    query: str,
    view: Optional[str],
) -> Tuple[pd.DataFrame, Optional[List[int]], str]:
    try:
        cursor_search = retry_scopus_search(query, view=view, subscriber=True)
        results = to_frame(cursor_search.results or [])
        if results.empty:
            raise RuntimeError("Cursor search returned no results")
        return results, None, "cursor"
    except Exception:
        slicing_results, years = run_slicing(query, view)
        return slicing_results, years, "slicing"


def run_slicing(query: str, view: Optional[str]) -> Tuple[pd.DataFrame, List[int]]:
    pd = _lazy_pandas()
    current_year = time.gmtime().tm_year
    years = list(range(1900, current_year + 1))
    frames = []
    covered_years = []
    for year in years:
        year_query = f"{query} AND PUBYEAR = {year}"
        precheck = retry_scopus_search(year_query, view=view, download=False, subscriber=True)
        if precheck.get_results_size() == 0:
            continue
        covered_years.append(year)
        results = retry_scopus_search(year_query, view=view, subscriber=True)
        frames.append(to_frame(results.results or []))

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not combined.empty and "eid" in combined.columns:
        combined = combined.drop_duplicates(subset=["eid"])
    return combined, covered_years


def retry_scopus_search(
    query: str,
    *,
    view: Optional[str],
    download: bool = True,
    subscriber: bool = False,
    retries: int = 3,
    delay: float = 2.0,
) -> Any:
    last_error: Optional[Exception] = None
    for attempt in range(retries):
        try:
            cls = _get_scopus_search_cls()
            return cls(query, view=view, download=download, subscriber=subscriber)
        except Exception as exc:
            _raise_actionable_scopus_error(exc)
            last_error = exc
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise
    raise RuntimeError("Scopus search failed") from last_error


def to_frame(records: Iterable[object]) -> pd.DataFrame:
    pd = _lazy_pandas()
    logger = logging.getLogger("pybibliometric_analysis")
    rows = []
    for record in records:
        if hasattr(record, "_asdict"):
            rows.append(record._asdict())
        elif isinstance(record, dict):
            rows.append(record)
        else:
            logger.warning("Unexpected record type %s; passing through to pandas", type(record))
            rows.append(record)
    return pd.DataFrame(rows)


def _raise_actionable_scopus_error(exc: Exception) -> None:
    error_name = exc.__class__.__name__
    actionable = {
        "Scopus401Error": "Unauthorized: check SCOPUS_API_KEY and optional INST_TOKEN entitlement.",
        "Scopus403Error": "Forbidden: verify API key entitlement or add INST_TOKEN if required.",
        "Scopus429Error": "Rate limited: too many requests; reduce request rate or retry later.",
    }
    if error_name in actionable:
        raise RuntimeError(actionable[error_name]) from exc

    if hasattr(exc, "status_code"):
        status_code = getattr(exc, "status_code")
        if status_code in {401, 403, 429}:
            raise RuntimeError(actionable.get(f"Scopus{status_code}Error", "Scopus API error.")) from exc

    if find_spec("pybliometrics.scopus.exception"):
        exc_mod = import_module("pybliometrics.scopus.exception")
        for name, message in actionable.items():
            exc_cls = getattr(exc_mod, name, None)
            if exc_cls and isinstance(exc, exc_cls):
                raise RuntimeError(message) from exc
