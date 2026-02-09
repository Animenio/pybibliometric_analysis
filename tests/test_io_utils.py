import pandas as pd

from pybibliometric_analysis import io_utils


def test_write_table_falls_back_to_csv(tmp_path, monkeypatch):
    df = pd.DataFrame({"a": [1, 2]})
    target = tmp_path / "data.parquet"

    def _raise_import(*_args, **_kwargs):
        raise ImportError("no engine")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", _raise_import, raising=True)

    output_path = io_utils.write_table(df, target)
    assert output_path.suffix == ".csv"
    assert output_path.exists()


def test_read_table_parquet_fallback(tmp_path, monkeypatch):
    csv_path = tmp_path / "data.csv"
    df = pd.DataFrame({"a": [3, 4]})
    df.to_csv(csv_path, index=False)

    monkeypatch.setattr(io_utils, "detect_parquet_engine", lambda: None)
    parquet_path = tmp_path / "data.parquet"
    loaded = io_utils.read_table(parquet_path)
    assert loaded["a"].tolist() == [3, 4]
