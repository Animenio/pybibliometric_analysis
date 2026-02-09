import builtins

from pybibliometric_analysis import settings


def test_init_pybliometrics_passes_keys_and_config_path(monkeypatch, tmp_path):
    recorded = {}

    def stub_init(**kwargs):
        recorded.update(kwargs)

    def fail_input(*_args, **_kwargs):
        raise AssertionError("input() should not be called during init")

    monkeypatch.setenv("SCOPUS_API_KEY", "TEST_KEY")
    monkeypatch.setattr(settings, "_resolve_pybliometrics_init", lambda: stub_init)
    monkeypatch.setattr(builtins, "input", fail_input)

    config_dir = tmp_path / "cfg"
    settings.init_pybliometrics(config_dir=config_dir)

    assert recorded["keys"] == ["TEST_KEY"]
    assert recorded["inst_tokens"] is None
    assert recorded["config_path"] == str(config_dir / "pybliometrics.cfg")
