from collections import namedtuple
from pathlib import Path
import pickle
import sys
import types

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

force_stub = False
try:
    import pyarrow  # noqa: F401
except Exception:
    force_stub = True

if force_stub:
    pandas_stub = types.ModuleType("pandas")

    class DataFrame:
        def __init__(self, rows=None):
            self._rows = list(rows or [])

        @property
        def empty(self):
            return len(self._rows) == 0

        @property
        def columns(self):
            if not self._rows:
                return []
            if isinstance(self._rows[0], dict):
                return list(self._rows[0].keys())
            return []

        def to_parquet(self, path, index=False):
            with open(path, "wb") as handle:
                pickle.dump(self._rows, handle)

        def drop_duplicates(self, subset):
            if not subset:
                return self
            seen = set()
            deduped = []
            for row in self._rows:
                if not isinstance(row, dict):
                    continue
                key = tuple(row.get(field) for field in subset)
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(row)
            return DataFrame(deduped)

        def __len__(self):
            return len(self._rows)

    def concat(frames, ignore_index=False):
        rows = []
        for frame in frames:
            rows.extend(frame._rows)
        return DataFrame(rows)

    def read_parquet(path):
        with open(path, "rb") as handle:
            return DataFrame(pickle.load(handle))

    pandas_stub.DataFrame = DataFrame
    pandas_stub.concat = concat
    pandas_stub.read_parquet = read_parquet
    sys.modules["pandas"] = pandas_stub
    pd = pandas_stub
else:
    import pandas as pd

from pybibliometric_analysis import settings
from pybibliometric_analysis.extract_scopus import run_extract


Document = namedtuple("Document", ["eid", "title"])


class DummySearch:
    def __init__(self, query, view=None, download=True, subscriber=True):
        self.query = query
        self.view = view
        self.download = download
        self.subscriber = subscriber
        self.results = [Document(eid="1", title="Test")] if download else []

    def get_results_size(self):
        return 1


def test_load_yaml(tmp_path):
    config_path = tmp_path / "search.yaml"
    config_path.write_text(
        "query: 'TITLE-ABS-KEY(\"islamic finance\")'\n"
        "database: 'Scopus'\n"
        "notes: 'no limits'\n"
        "use_cursor_preferred: true\n",
        encoding="utf-8",
    )
    config = settings.load_search_config(config_path)
    assert config.query
    assert config.database == "Scopus"
    assert config.use_cursor_preferred is True


def test_run_id_generation():
    run_id = settings.generate_run_id()
    assert run_id.endswith("Z")
    assert "T" in run_id


def test_no_overwrite_raw(tmp_path, monkeypatch):
    raw_path = tmp_path / "data" / "raw" / "scopus_search_20200101T000000Z.parquet"
    raw_path.parent.mkdir(parents=True)
    raw_path.write_text("existing", encoding="utf-8")
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "pybibliometric_analysis.extract_scopus.build_paths",
        lambda base_dir, run_id: type(
            "Paths",
            (),
            {
                "raw_path": raw_path,
                "manifest_path": tmp_path / "outputs" / "methods" / "manifest.json",
                "log_path": tmp_path / "logs" / "extract.log",
            },
        )(),
    )

    with pytest.raises(FileExistsError):
        run_extract(
            run_id="20200101T000000Z",
            config_path=tmp_path / "search.yaml",
            pybliometrics_config_dir=tmp_path / "config",
            scopus_api_key_file=tmp_path / "scopus_api_key.txt",
            view=None,
            force_slicing=False,
            base_dir=tmp_path,
        )


def test_manifest_keys(monkeypatch, tmp_path):
    manifest = settings.build_manifest(
        run_id="20200101T000000Z",
        query="TITLE-ABS-KEY(\"islamic finance\")",
        database="Scopus",
        n_results_estimated=10,
        n_records_downloaded=10,
        strategy_used="normal",
        years_covered=None,
        columns_present=["eid"],
    )
    expected_keys = {
        "timestamp_iso",
        "run_id",
        "query",
        "database",
        "n_results_estimated",
        "n_records_downloaded",
        "strategy_used",
        "years_covered",
        "python_version",
        "package_versions",
        "columns_present",
        "git_commit",
    }
    assert expected_keys.issubset(manifest.keys())


def test_extract_uses_mocked_scopus(monkeypatch, tmp_path):
    config_path = tmp_path / "search.yaml"
    config_path.write_text(
        "query: 'TITLE-ABS-KEY(\"islamic finance\")'\n"
        "database: 'Scopus'\n"
        "notes: 'no limits'\n"
        "use_cursor_preferred: false\n",
        encoding="utf-8",
    )

    monkeypatch.setattr("pybibliometric_analysis.extract_scopus.ScopusSearch", DummySearch)
    monkeypatch.setattr(
        "pybibliometric_analysis.extract_scopus.init_pybliometrics",
        lambda *_args, **_kwargs: None,
    )

    run_extract(
        run_id="20200101T000000Z",
        config_path=config_path,
        pybliometrics_config_dir=tmp_path / "config",
        scopus_api_key_file=tmp_path / "scopus_api_key.txt",
        view=None,
        force_slicing=False,
        base_dir=tmp_path,
    )

    raw_path = tmp_path / "data" / "raw" / "scopus_search_20200101T000000Z.parquet"
    assert raw_path.exists()
    df = pd.read_parquet(raw_path)
    assert not df.empty


def test_load_scopus_api_key_from_file(tmp_path):
    api_key_file = tmp_path / "scopus_api_key.txt"
    api_key_file.write_text(
        "# comment line\n\nTEST_KEY_123\n",
        encoding="utf-8",
    )
    assert settings.load_scopus_api_key(api_key_file) == "TEST_KEY_123"


def test_load_scopus_api_key_placeholder_returns_none(tmp_path):
    api_key_file = tmp_path / "scopus_api_key.txt"
    api_key_file.write_text(
        "# comment line\nYOUR_SCOPUS_API_KEY_HERE\n",
        encoding="utf-8",
    )
    assert settings.load_scopus_api_key(api_key_file) is None


def test_load_scopus_api_key_env_overrides_file(tmp_path, monkeypatch):
    api_key_file = tmp_path / "scopus_api_key.txt"
    api_key_file.write_text("FILE_KEY\n", encoding="utf-8")
    monkeypatch.setenv("SCOPUS_API_KEY", "ENV_KEY")
    assert settings.load_scopus_api_key(api_key_file) == "ENV_KEY"
