from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from importlib import import_module
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Tuple

from pybibliometric_analysis.io_utils import detect_parquet_support, write_json, write_table
from pybibliometric_analysis.settings import (
    build_manifest,
    ensure_pybliometrics_cfg,
    get_git_commit,
    get_package_versions,
    init_pybliometrics,
    load_scopus_api_key_with_source,
    load_scopus_insttoken_with_source,
    load_search_config,
    sha256_file,
)

if TYPE_CHECKING:
    import pandas as pd

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


def _is_query_limit_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "more than 5000" in message
        or "5000 entries" in message
        or "fails to return more than 5000" in message
    )


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
    dry_run: bool = False,
    base_dir: Path = Path("."),
) -> None:
    paths = build_paths(base_dir, run_id)
    logger = setup_logging(paths.log_path)

    if paths.raw_path.with_suffix(".parquet").exists() or paths.raw_path.with_suffix(
        ".csv"
    ).exists():
        raise FileExistsError(f"Raw dataset already exists: {paths.raw_path}")

    config = load_search_config(config_path)
    logger.info("Loaded search config for database %s", config.database)
    config_hash = sha256_file(config_path)

    api_key, api_source = load_scopus_api_key_with_source(scopus_api_key_file)
    inst_token, inst_source = load_scopus_insttoken_with_source(inst_token_file)
    cfg_path = ensure_pybliometrics_cfg(pybliometrics_config_dir, api_key, inst_token)
    logger.info("Using pybliometrics config: %s", cfg_path)
    logger.info("SCOPUS_API_KEY present: %s (source=%s)", bool(api_key), api_source or "none")
    logger.info("InstToken present: %s (source=%s)", bool(inst_token), inst_source or "none")

    if dry_run:
        raw_expected = _expected_raw_path(paths.raw_path)
        output_format = raw_expected.suffix.lstrip(".")
        manifest = _build_search_manifest(
            run_id=run_id,
            query=config.query,
            database=config.database,
            config_path=config_path,
            config_hash=config_hash,
            strategy_used="dry-run",
            subscriber_mode=False,
            n_results_estimated=0,
            n_records_downloaded=0,
            columns_present=[],
            raw_output_path=str(raw_expected),
            output_format=output_format,
            view=view,
            dry_run=True,
            log_path=str(paths.log_path),
            manifest_path=str(paths.manifest_path),
        )
        write_json(manifest, paths.manifest_path)
        logger.info("Dry run complete. Manifest written to %s", paths.manifest_path)
        return

    logger.info("Initializing pybliometrics configuration")
    init_pybliometrics(
        pybliometrics_config_dir,
        scopus_api_key_file,
        inst_token_file=inst_token_file,
        logger=logger,
    )


    threshold = 5000
    try:
        estimate_search = retry_scopus_search(
            config.query,
            view=view,
            download=False,
            subscriber=config.subscriber_mode,
        )
        n_results = estimate_search.get_results_size()
        logger.info("Estimated %s results", n_results)
    except Exception as exc:
        if force_slicing and _is_query_limit_error(exc):
            logger.warning(
                "Estimate failed due to result limit; proceeding with slicing. Error=%s",
                exc,
            )
            n_results = threshold + 1
        else:
            raise

    if force_slicing:
        planned_strategy = "slicing"
    elif n_results > threshold and config.subscriber_mode and config.use_cursor_preferred:
        planned_strategy = "cursor"
    elif n_results > threshold:
        planned_strategy = "slicing"
    else:
        planned_strategy = "single"

    logger.info(
        "Extract run_id=%s query=%s n_results=%s strategy=%s raw_base=%s "
        "manifest_path=%s log_path=%s",
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
        records, years_covered = run_slicing(
            config.query,
            view,
            start_year=config.start_year,
            end_year=config.end_year,
            max_years_back=config.max_years_back,
            subscriber=config.subscriber_mode,
        )
    elif n_results > threshold and config.subscriber_mode and config.use_cursor_preferred:
        records, years_covered, strategy = run_cursor_with_fallback(
            config.query,
            view,
            start_year=config.start_year,
            end_year=config.end_year,
            max_years_back=config.max_years_back,
            subscriber=config.subscriber_mode,
        )
    elif n_results > threshold:
        strategy = "slicing"
        records, years_covered = run_slicing(
            config.query,
            view,
            start_year=config.start_year,
            end_year=config.end_year,
            max_years_back=config.max_years_back,
            subscriber=config.subscriber_mode,
        )
    else:
        strategy = "single"
        records = run_standard(config.query, view, subscriber=config.subscriber_mode)
        years_covered = None

    if records.empty:
        logger.warning("No records downloaded for query")

    columns_present = list(records.columns)
    raw_output = write_table(records, paths.raw_path)
    logger.info("Saved raw data to %s", raw_output["path"])

    manifest = _build_search_manifest(
        run_id=run_id,
        query=config.query,
        database=config.database,
        config_path=config_path,
        config_hash=config_hash,
        strategy_used=strategy,
        subscriber_mode=strategy in {"cursor", "slicing"},
        n_results_estimated=n_results,
        n_records_downloaded=len(records),
        columns_present=columns_present,
        raw_output_path=raw_output["path"],
        output_format=raw_output["format"],
        view=view,
        dry_run=False,
        log_path=str(paths.log_path),
        manifest_path=str(paths.manifest_path),
        years_covered=years_covered,
    )
    write_json(manifest, paths.manifest_path)
    logger.info("Wrote manifest to %s", paths.manifest_path)


def build_paths(base_dir: Path, run_id: str) -> ExtractPaths:
    raw_path = base_dir / "data" / "raw" / f"scopus_search_{run_id}"
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


def run_standard(query: str, view: Optional[str], subscriber: bool) -> pd.DataFrame:
    search = retry_scopus_search(query, view=view, subscriber=subscriber)
    return to_frame(search.results or [])


def run_cursor_with_fallback(
    query: str,
    view: Optional[str],
    *,
    start_year: Optional[int],
    end_year: Optional[int],
    max_years_back: Optional[int],
    subscriber: bool,
) -> Tuple[pd.DataFrame, Optional[List[int]], str]:
    try:
        cursor_search = retry_scopus_search(query, view=view, subscriber=True)
        results = to_frame(cursor_search.results or [])
        if results.empty:
            raise RuntimeError("Cursor search returned no results")
        return results, None, "cursor"
    except Exception:
        slicing_results, years = run_slicing(
            query,
            view,
            start_year=start_year,
            end_year=end_year,
            max_years_back=max_years_back,
            subscriber=subscriber,
        )
        return slicing_results, years, "slicing"


def run_slicing(
    query: str,
    view: Optional[str],
    *,
    start_year: Optional[int],
    end_year: Optional[int],
    max_years_back: Optional[int],
    subscriber: bool,
) -> Tuple[pd.DataFrame, List[int]]:
    pd = _lazy_pandas()
    if start_year is None or end_year is None:
        if max_years_back is None:
            raise ValueError(
                "start_year/end_year or max_years_back must be set for slicing strategy."
            )
        current_year = time.gmtime().tm_year
        end_year = current_year
        start_year = current_year - int(max_years_back) + 1
    years = list(range(int(start_year), int(end_year) + 1))
    frames = []
    covered_years = []
    for year in years:
        year_query = f"{query} AND PUBYEAR = {year}"
        precheck = retry_scopus_search(year_query, view=view, download=False, subscriber=subscriber)
        if precheck.get_results_size() == 0:
            continue
        covered_years.append(year)
        results = retry_scopus_search(year_query, view=view, subscriber=subscriber)
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
        "Scopus401Error": "Unauthorized (401): SCOPUS_API_KEY is invalid or disabled.",
        "Scopus403Error": (
            "Forbidden (403): access requires INST_TOKEN or an institutional subscription."
        ),
        "Scopus429Error": "Rate limited: too many requests; reduce request rate or retry later.",
    }
    if error_name in actionable:
        raise RuntimeError(actionable[error_name]) from exc

    if hasattr(exc, "status_code"):
        status_code = getattr(exc, "status_code")
        if status_code in {401, 403, 429}:
            raise RuntimeError(
                actionable.get(f"Scopus{status_code}Error", "Scopus API error.")
            ) from exc

    if find_spec("pybliometrics.scopus.exception"):
        exc_mod = import_module("pybliometrics.scopus.exception")
        for name, message in actionable.items():
            exc_cls = getattr(exc_mod, name, None)
            if exc_cls and isinstance(exc, exc_cls):
                raise RuntimeError(message) from exc


def _expected_raw_path(raw_base: Path) -> Path:
    suffix = ".parquet" if detect_parquet_support() else ".csv"
    return raw_base.with_suffix(suffix)


def _build_search_manifest(
    *,
    run_id: str,
    query: str,
    database: str,
    config_path: Path,
    config_hash: str,
    strategy_used: str,
    subscriber_mode: bool,
    n_results_estimated: int,
    n_records_downloaded: int,
    columns_present: List[str],
    raw_output_path: str,
    output_format: str,
    view: Optional[str],
    dry_run: bool,
    log_path: str,
    manifest_path: str,
    years_covered: Optional[List[int]] = None,
) -> dict:
    manifest = build_manifest(
        run_id=run_id,
        query=query,
        database=database or "Scopus",
        n_results_estimated=n_results_estimated,
        n_records_downloaded=n_records_downloaded,
        strategy_used=strategy_used,
        years_covered=years_covered,
        columns_present=columns_present,
    )
    manifest.update(
        {
            "schema_version": "1.0",
            "dry_run": dry_run,
            "config_path": str(config_path),
            "config_hash": config_hash,
            "strategy_used": strategy_used,
            "subscriber_mode": subscriber_mode,
            "view": view,
            "output_raw_path": raw_output_path,
            "output_format": output_format,
            "output_paths": {
                "raw_data": raw_output_path,
                "log_file": log_path,
                "manifest": manifest_path,
            },
            "python_version": manifest["python_version"],
            "package_versions": get_package_versions(),
            "git_commit": get_git_commit(),
        }
    )
    return manifest
