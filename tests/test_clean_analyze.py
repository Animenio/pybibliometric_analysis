import shutil
from pathlib import Path
import shutil

import pandas as pd

from pybibliometric_analysis.analyze_bibliometrics import run_analyze
from pybibliometric_analysis.clean_scopus import run_clean


def _copy_fixture(base_dir: Path, run_id: str) -> Path:
    raw_dir = base_dir / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    fixture = Path(__file__).parent / "fixtures" / "raw_scopus_sample.csv"
    target = raw_dir / f"scopus_search_{run_id}.csv"
    shutil.copy(fixture, target)
    return target


def test_clean_and_analyze(tmp_path):
    run_id = "trend-001"
    _copy_fixture(tmp_path, run_id)

    run_clean(
        run_id=run_id,
        base_dir=tmp_path,
        input_path=None,
        force=True,
        write_format="csv",
    )

    cleaned_path = tmp_path / "data" / "processed" / f"scopus_clean_{run_id}.csv"
    assert cleaned_path.exists()
    cleaned = pd.read_csv(cleaned_path)
    assert cleaned["pub_year"].dropna().astype(int).min() == 2020
    assert cleaned["author_names"].notna().any()
    keyword_path = tmp_path / "outputs" / "analysis" / f"keyword_freq_{run_id}.csv"
    assert keyword_path.exists()

    run_analyze(
        run_id=run_id,
        base_dir=tmp_path,
        input_path=None,
        figures=False,
        min_year=None,
        max_year=None,
    )

    yoy_path = tmp_path / "outputs" / "analysis" / f"yoy_growth_{run_id}.csv"
    cagr_path = tmp_path / "outputs" / "analysis" / f"cagr_{run_id}.csv"
    analysis_manifest = tmp_path / "outputs" / "methods" / f"analysis_manifest_{run_id}.json"
    assert yoy_path.exists()
    assert cagr_path.exists()
    assert analysis_manifest.exists()
