import json
import os
import platform
import subprocess
from importlib import import_module
from importlib.util import find_spec
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass(frozen=True)
class SearchConfig:
    query: str
    database: str
    notes: str
    use_cursor_preferred: bool
    subscriber_mode: bool
    start_year: Optional[int]
    end_year: Optional[int]


def load_search_config(path: Path) -> SearchConfig:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    start_year = data.get("start_year")
    end_year = data.get("end_year")
    return SearchConfig(
        query=str(data.get("query", "")),
        database=str(data.get("database", "")),
        notes=str(data.get("notes", "")),
        use_cursor_preferred=bool(data.get("use_cursor_preferred", False)),
        subscriber_mode=bool(data.get("subscriber_mode", False)),
        start_year=int(start_year) if start_year is not None else None,
        end_year=int(end_year) if end_year is not None else None,
    )


def generate_run_id(prefix: str = "smoke") -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{timestamp}"


def _read_first_token(path: Path) -> Optional[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    for line in lines:
        cleaned = line.split("#", 1)[0].strip()
        if not cleaned:
            continue
        return cleaned
    return None


def load_scopus_api_key(api_key_file: Optional[Path]) -> Optional[str]:
    api_key, _source = load_scopus_api_key_with_source(api_key_file)
    return api_key


def load_scopus_api_key_with_source(api_key_file: Optional[Path]) -> tuple[Optional[str], Optional[str]]:
    default_file = Path("config/scopus_api_key.txt")
    if api_key_file and api_key_file.exists():
        cleaned = _read_first_token(api_key_file)
        if cleaned and cleaned != "YOUR_SCOPUS_API_KEY_HERE":
            return cleaned, "file"
    env_key = os.getenv("SCOPUS_API_KEY")
    if env_key:
        return env_key.strip() or None, "env"
    if default_file.exists():
        cleaned = _read_first_token(default_file)
        if cleaned and cleaned != "YOUR_SCOPUS_API_KEY_HERE":
            return cleaned, "default_file"
    return None, None


def load_scopus_insttoken(inst_token_file: Optional[Path] = None) -> Optional[str]:
    inst_token, _source = load_scopus_insttoken_with_source(inst_token_file)
    return inst_token


def load_scopus_insttoken_with_source(
    inst_token_file: Optional[Path] = None,
) -> tuple[Optional[str], Optional[str]]:
    default_file = Path("config/inst_token.txt")
    if inst_token_file and inst_token_file.exists():
        cleaned = _read_first_token(inst_token_file)
        if cleaned and cleaned != "YOUR_INST_TOKEN_HERE":
            return cleaned, "file"
    env_token = os.getenv("INST_TOKEN")
    if env_token:
        return env_token.strip() or None, "env"
    env_token = os.getenv("INSTTOKEN")
    if env_token:
        return env_token.strip() or None, "env_compat"
    if default_file.exists():
        cleaned = _read_first_token(default_file)
        if cleaned and cleaned != "YOUR_INST_TOKEN_HERE":
            return cleaned, "default_file"
    return None, None


def ensure_pybliometrics_cfg(
    config_dir: Path,
    api_key: Optional[str],
    inst_token: Optional[str],
) -> Path:
    config_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = config_dir / "pybliometrics.cfg"

    if not cfg_path.exists() and api_key:
        cache_root = (Path.cwd() / ".cache" / "pybliometrics").resolve()
        cache_root.mkdir(parents=True, exist_ok=True)
        cfg_text = _render_pybliometrics_cfg(cache_root, api_key, inst_token)
        cfg_path.write_text(cfg_text, encoding="utf-8")

    if not cfg_path.exists() and not api_key:
        raise RuntimeError(
            f"SCOPUS_API_KEY is not set and {cfg_path} is missing. "
            "Set the SCOPUS_API_KEY environment variable, provide a key file, or create the config file. "
            "See config/pybliometrics/pybliometrics.cfg.example for reference."
        )

    return cfg_path


def ensure_pybliometrics_config(
    config_dir: Path,
    api_key_file: Optional[Path] = None,
    inst_token_file: Optional[Path] = None,
) -> Path:
    api_key, _api_source = load_scopus_api_key_with_source(api_key_file)
    inst_token, _inst_source = load_scopus_insttoken_with_source(inst_token_file)
    return ensure_pybliometrics_cfg(config_dir, api_key, inst_token)


def init_pybliometrics(
    config_dir: Path,
    api_key_file: Optional[Path] = None,
    inst_token_file: Optional[Path] = None,
    logger: Optional[Any] = None,
) -> None:
    api_key, api_source = load_scopus_api_key_with_source(api_key_file)
    inst_token, inst_source = load_scopus_insttoken_with_source(inst_token_file)
    cfg_path = ensure_pybliometrics_cfg(config_dir, api_key, inst_token)
    if logger:
        logger.info("Using pybliometrics config: %s", cfg_path)
        logger.info("SCOPUS_API_KEY present: %s (source=%s)", bool(api_key), api_source or "none")
        logger.info("InstToken present: %s (source=%s)", bool(inst_token), inst_source or "none")

    init_func = _resolve_pybliometrics_init()
    if init_func is None:
        return

    for kwargs in ({"config_path": str(cfg_path)}, {"config_dir": str(config_dir)}):
        try:
            init_func(**kwargs)
            return
        except TypeError:
            continue


def _resolve_pybliometrics_init():
    """Resolve init() across pybliometrics versions without import-time prompts."""
    if find_spec("pybliometrics.utils"):
        utils_mod = import_module("pybliometrics.utils")
        pb_init = getattr(utils_mod, "init", None)
        if pb_init is not None:
            return pb_init

    if find_spec("pybliometrics"):
        pb_mod = import_module("pybliometrics")
        pb_init = getattr(pb_mod, "init", None)
        if pb_init is not None:
            return pb_init

    # pybliometrics 3.x may not have an explicit init function
    # and will auto-load the config file, so return None instead of raising
    return None


def _render_pybliometrics_cfg(cache_root: Path, api_key: str, insttoken: Optional[str]) -> str:
    sections = {
        "AbstractRetrieval": cache_root / "abstractretrieval",
        "AffiliationRetrieval": cache_root / "affiliationretrieval",
        "AffiliationSearch": cache_root / "affiliationsearch",
        "AuthorRetrieval": cache_root / "authorretrieval",
        "AuthorSearch": cache_root / "authorsearch",
        "CitationOverview": cache_root / "citationoverview",
        "PlumXMetrics": cache_root / "plumxmetrics",
        "ScopusSearch": cache_root / "scopussearch",
        "SerialTitleSearch": cache_root / "serialtitlesearch",
        "SerialTitleISSN": cache_root / "serialtitleissn",
        "SubjectClassifications": cache_root / "subjectclassifications",
    }
    lines = ["[Directories]"]
    for name, path in sections.items():
        path.mkdir(parents=True, exist_ok=True)
        lines.append(f"{name} = {path}")
    lines.append("")
    lines.append("[Authentication]")
    lines.append(f"APIKey = {api_key}")
    if insttoken:
        lines.append(f"InstToken = {insttoken}")
    lines.append("")
    lines.append("[Requests]")
    lines.append("Timeout = 20")
    lines.append("Retries = 5")
    lines.append("")
    return "\n".join(lines)


def get_package_versions() -> Dict[str, Optional[str]]:
    packages = ["pybliometrics", "pandas", "numpy", "pyarrow"]
    versions: Dict[str, Optional[str]] = {}
    for package in packages:
        try:
            versions[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            versions[package] = None
    return versions


def get_git_commit() -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def build_manifest(
    *,
    run_id: str,
    query: str,
    database: str,
    n_results_estimated: int,
    n_records_downloaded: int,
    strategy_used: str,
    years_covered: Optional[List[int]],
    columns_present: List[str],
) -> Dict[str, Any]:
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "query": query,
        "database": database,
        "n_results_estimated": n_results_estimated,
        "n_records_downloaded": n_records_downloaded,
        "strategy_used": strategy_used,
        "years_covered": years_covered,
        "python_version": platform.python_version(),
        "package_versions": get_package_versions(),
        "columns_present": columns_present,
        "git_commit": get_git_commit(),
    }


def compute_file_hash(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return compute_file_hash(path)


def write_manifest(path: Path, manifest: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
