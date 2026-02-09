from pathlib import Path

import pandas as pd

from pybibliometric_analysis import io_utils


def test_write_table_falls_back_to_csv(tmp_path, monkeypatch):
    df = pd.DataFrame({"a": [1, 2]})
    target_base = tmp_path / "data"

    def _raise_import(*_args, **_kwargs):
        raise ImportError("no engine")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", _raise_import, raising=True)

    output = io_utils.write_table(target_base, df, preferred_format="parquet")
    assert output["format"] == "csv"
    assert output["path"].endswith(".csv")
    assert Path(output["path"]).exists()


def test_read_table_parquet_fallback(tmp_path, monkeypatch):
    csv_path = tmp_path / "data.csv"
    df = pd.DataFrame({"a": [3, 4]})
    df.to_csv(csv_path, index=False)

    monkeypatch.setattr(io_utils, "detect_parquet_support", lambda: False)
    base_path = tmp_path / "data"
    loaded = io_utils.read_table(base_path)
    assert loaded["a"].tolist() == [3, 4]
