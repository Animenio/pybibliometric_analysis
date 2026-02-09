import json
from collections import namedtuple

import pytest

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
    assert run_id.startswith("smoke-")


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
            inst_token_file=None,
            view=None,
            force_slicing=False,
            base_dir=tmp_path,
        )


def test_dry_run_manifest(tmp_path, monkeypatch):
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)
    config_path = tmp_path / "search.yaml"
    config_path.write_text(
        "query: 'TITLE-ABS-KEY(\"islamic finance\")'\n" "database: 'Scopus'\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("SCOPUS_API_KEY", "TEST_KEY")

    # prevent network: stub ScopusSearch to a dummy object
    monkeypatch.setattr(
        "pybibliometric_analysis.extract_scopus._get_scopus_search_cls",
        lambda: DummySearch,
    )

    run_extract(
        run_id="dryrun-001",
        config_path=config_path,
        pybliometrics_config_dir=tmp_path / "config" / "pybliometrics",
        scopus_api_key_file=tmp_path / "scopus_api_key.txt",
        inst_token_file=None,
        view=None,
        force_slicing=False,
        dry_run=True,
        base_dir=tmp_path,
    )

    manifest_path = tmp_path / "outputs" / "methods" / "search_manifest_dryrun-001.json"
    assert manifest_path.exists()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["dry_run"] is True
    assert payload["output_format"] in {"csv", "parquet"}
