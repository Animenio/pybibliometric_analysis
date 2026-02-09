from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from importlib import import_module
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from pybibliometric_analysis.io_utils import read_table

if TYPE_CHECKING:
    import pandas as pd


def _lazy_pandas():
    import pandas as pd

    return pd


def setup_logging(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pybibliometric_analysis.analyze")
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


def _latest_clean_file(base_dir: Path) -> Path:
    processed_dir = base_dir / "data" / "processed"
    candidates = list(processed_dir.glob("scopus_clean_*.parquet")) + list(
        processed_dir.glob("scopus_clean_*.csv")
    )
    if not candidates:
        raise FileNotFoundError("No scopus_clean_* files found in data/processed.")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _extract_run_id(path: Path) -> str:
    name = path.stem
    if name.startswith("scopus_clean_"):
        return name.replace("scopus_clean_", "", 1)
    return name


def _filter_years(
    df: "pd.DataFrame",
    min_year: Optional[int],
    max_year: Optional[int],
) -> "pd.DataFrame":
    if "pub_year" not in df.columns:
        return df
    filtered = df.dropna(subset=["pub_year"]).copy()
    if min_year is not None:
        filtered = filtered[filtered["pub_year"] >= min_year]
    if max_year is not None:
        filtered = filtered[filtered["pub_year"] <= max_year]
    return filtered


def _compute_yoy(pubs_by_year: "pd.DataFrame") -> "pd.DataFrame":
    pd = _lazy_pandas()
    pubs_by_year = pubs_by_year.sort_values("pub_year")
    pubs_by_year["prev_count"] = pubs_by_year["count"].shift(1)
    pubs_by_year["yoy_abs"] = pubs_by_year["count"] - pubs_by_year["prev_count"]
    pubs_by_year["yoy_pct"] = pd.NA
    mask = pubs_by_year["prev_count"] > 0
    pubs_by_year.loc[mask, "yoy_pct"] = (
        (pubs_by_year.loc[mask, "yoy_abs"] / pubs_by_year.loc[mask, "prev_count"]) * 100
    )
    return pubs_by_year.rename(columns={"pub_year": "year"})[
        ["year", "count", "yoy_abs", "yoy_pct"]
    ]


def _compute_cagr(pubs_by_year: "pd.DataFrame") -> dict:
    nonzero = pubs_by_year[pubs_by_year["count"] > 0].sort_values("pub_year")
    if nonzero.empty:
        return {
            "start_year": None,
            "end_year": None,
            "start_count": None,
            "end_count": None,
            "cagr": 0.0,
        }
    start_year = int(nonzero.iloc[0]["pub_year"])
    end_year = int(nonzero.iloc[-1]["pub_year"])
    start_count = float(nonzero.iloc[0]["count"])
    end_count = float(nonzero.iloc[-1]["count"])
    years = end_year - start_year
    if years <= 0 or start_count <= 0:
        cagr = 0.0
    else:
        cagr = (end_count / start_count) ** (1 / years) - 1
    return {
        "start_year": start_year,
        "end_year": end_year,
        "start_count": start_count,
        "end_count": end_count,
        "cagr": cagr,
    }


def _compute_avg_last5_vs_prev5(pubs_by_year: "pd.DataFrame") -> dict:
    sorted_years = pubs_by_year.sort_values("pub_year")
    counts = sorted_years["count"].tolist()
    if len(counts) < 10:
        return {"avg_last5": None, "avg_prev5": None, "avg_last5_vs_prev5": None}
    avg_prev5 = sum(counts[-10:-5]) / 5
    avg_last5 = sum(counts[-5:]) / 5
    if avg_prev5 == 0:
        ratio = None
    else:
        ratio = avg_last5 / avg_prev5
    return {"avg_last5": avg_last5, "avg_prev5": avg_prev5, "avg_last5_vs_prev5": ratio}


def _maybe_plot(
    figures: bool,
    pubs_by_year: "pd.DataFrame",
    yoy: "pd.DataFrame",
    run_id: str,
    base_dir: Path,
    logger: logging.Logger,
) -> None:
    if not figures:
        return
    if not find_spec("matplotlib"):
        logger.warning("matplotlib not installed; skipping figure generation.")
        return
    plt = import_module("matplotlib.pyplot")
    figures_dir = base_dir / "outputs" / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)

    plt.figure()
    plt.plot(pubs_by_year["pub_year"], pubs_by_year["count"], marker="o")
    plt.title("Publications by Year")
    plt.xlabel("Year")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(figures_dir / f"pubs_by_year_{run_id}.png")
    plt.close()

    plt.figure()
    plt.plot(yoy["year"], yoy["yoy_pct"], marker="o")
    plt.title("Year-over-Year Growth")
    plt.xlabel("Year")
    plt.ylabel("YoY %")
    plt.tight_layout()
    plt.savefig(figures_dir / f"yoy_growth_{run_id}.png")
    plt.close()


def run_analyze(
    *,
    run_id: Optional[str],
    base_dir: Path,
    input_path: Optional[Path],
    figures: bool,
    min_year: Optional[int],
    max_year: Optional[int],
) -> None:
    base_dir = base_dir.resolve()
    log_path = base_dir / "logs" / f"analyze_{run_id or 'latest'}.log"
    logger = setup_logging(log_path)

    if not run_id:
        raise ValueError("--run-id is required for analyze.")

    if input_path:
        clean_path = input_path
        if not clean_path.exists():
            raise FileNotFoundError(f"Clean file not found: {clean_path}")
    else:
        clean_path = base_dir / "data" / "processed" / f"scopus_clean_{run_id}"
        if not (
            clean_path.with_suffix(".parquet").exists()
            or clean_path.with_suffix(".csv").exists()
        ):
            raise FileNotFoundError(f"Clean file not found for run_id={run_id}: {clean_path}")

    resolved_run_id = run_id or _extract_run_id(clean_path)
    logger.info("Analyzing run_id=%s input=%s", resolved_run_id, clean_path)

    df = read_table(clean_path)
    df = _filter_years(df, min_year, max_year)

    pubs_by_year = (
        df.dropna(subset=["pub_year"])
        .groupby("pub_year")
        .size()
        .reset_index(name="count")
        .sort_values("pub_year")
    )
    analysis_dir = base_dir / "outputs" / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)
    pubs_by_year_path = analysis_dir / f"pubs_by_year_{resolved_run_id}.csv"
    pubs_by_year.to_csv(pubs_by_year_path, index=False)

    yoy = _compute_yoy(pubs_by_year)
    yoy_path = analysis_dir / f"yoy_growth_{resolved_run_id}.csv"
    yoy.to_csv(yoy_path, index=False)

    cagr_summary = _compute_cagr(pubs_by_year)
    cagr_summary.update(_compute_avg_last5_vs_prev5(pubs_by_year))
    cagr_summary["timestamp_utc"] = datetime.now(timezone.utc).isoformat()
    cagr_summary["run_id"] = resolved_run_id
    cagr_path = analysis_dir / f"cagr_{resolved_run_id}.csv"
    _lazy_pandas().DataFrame([cagr_summary]).to_csv(cagr_path, index=False)

    analysis_manifest = {
        "schema_version": "1.0",
        "timestamp_utc": cagr_summary["timestamp_utc"],
        "run_id": resolved_run_id,
        "period": {
            "start_year": cagr_summary.get("start_year"),
            "end_year": cagr_summary.get("end_year"),
        },
        "cagr": cagr_summary.get("cagr"),
        "output_paths": {
            "pubs_by_year": str(pubs_by_year_path),
            "yoy_growth": str(yoy_path),
            "cagr": str(cagr_path),
        },
        "yoy_pct_units": "percent",
    }
    (base_dir / "outputs" / "methods").mkdir(parents=True, exist_ok=True)
    (base_dir / "outputs" / "methods" / f"analysis_manifest_{resolved_run_id}.json").write_text(
        json.dumps(analysis_manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    _maybe_plot(figures, pubs_by_year, yoy, resolved_run_id, base_dir, logger)
    logger.info("Analysis outputs written to %s", analysis_dir)
