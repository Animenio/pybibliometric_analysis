from pathlib import Path

from pybibliometric_analysis.scopus_full_analysis import run_full_scopus_csv_analysis


def test_run_full_scopus_csv_analysis_creates_outputs(tmp_path: Path):
    run_id = "full-001"
    csv_path = Path(__file__).parent / "fixtures" / "raw_scopus_sample.csv"

    run_full_scopus_csv_analysis(
        run_id=run_id,
        csv_path=csv_path,
        base_dir=tmp_path,
        min_year=1983,
        max_year=2025,
    )

    output_root = tmp_path / "outputs" / "full_analysis" / run_id
    assert (output_root / "tables" / "dataset_scope.csv").exists()
    assert (output_root / "tables" / "pubs_by_year.csv").exists()
    assert (output_root / "meta" / "manifest.json").exists()
    assert (tmp_path / "outputs" / "full_analysis" / f"{run_id}.zip").exists()
