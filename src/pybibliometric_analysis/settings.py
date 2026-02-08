import json
import os
import platform
import subprocess
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


def load_search_config(path: Path) -> SearchConfig:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return SearchConfig(
        query=str(data.get("query", "")),
        database=str(data.get("database", "")),
        notes=str(data.get("notes", "")),
        use_cursor_preferred=bool(data.get("use_cursor_preferred", False)),
    )


def generate_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_scopus_api_key(api_key_file: Optional[Path]) -> Optional[str]:
    env_key = os.getenv("SCOPUS_API_KEY")
    if env_key:
        return env_key.strip() or None
    if api_key_file and api_key_file.exists():
        lines = api_key_file.read_text(encoding="utf-8").splitlines()
        for line in lines:
            cleaned = line.split("#", 1)[0].strip()
            if not cleaned:
                continue
            if cleaned == "YOUR_SCOPUS_API_KEY_HERE":
                return None
            return cleaned
    return None


def ensure_pybliometrics_config(config_dir: Path, api_key_file: Optional[Path] = None) -> Path:
    config_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = config_dir / "pybliometrics.cfg"
    api_key = load_scopus_api_key(api_key_file)

    if not cfg_path.exists() and api_key:
        cache_root = (Path.cwd() / ".cache" / "pybliometrics").resolve()
        cache_root.mkdir(parents=True, exist_ok=True)
        cfg_text = _render_pybliometrics_cfg(cache_root, api_key)
        cfg_path.write_text(cfg_text, encoding="utf-8")

    if not cfg_path.exists() and not api_key:
        raise RuntimeError(
            f"SCOPUS_API_KEY is not set and {cfg_path} is missing. "
            "Set the SCOPUS_API_KEY environment variable, provide a key file, or create the config file. "
            "See config/pybliometrics/pybliometrics.cfg.example for reference."
        )

    return cfg_path


def init_pybliometrics(config_dir: Path, api_key_file: Optional[Path] = None) -> None:
    cfg_path = ensure_pybliometrics_config(config_dir, api_key_file)
    init_func = _resolve_pybliometrics_init()
    init_func(config_path=str(cfg_path))


def _resolve_pybliometrics_init():
    """Resolve the init function across pybliometrics versions."""
    try:
        from pybliometrics.utils import init as pb_init  # type: ignore

        return pb_init
    except Exception:
        pass

    try:
        import pybliometrics  # type: ignore

        pb_init = getattr(pybliometrics, "init", None)
        if pb_init is not None:
            return pb_init
    except Exception:
        pass

    raise ImportError(
        "Cannot resolve pybliometrics init(). Expected pybliometrics.utils.init (v3.x) "
        "or pybliometrics.init (newer)."
    )


def _render_pybliometrics_cfg(cache_root: Path, api_key: str) -> str:
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
        "timestamp_iso": datetime.now(timezone.utc).isoformat(),
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


def write_manifest(path: Path, manifest: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
