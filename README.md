# pybibliometric_analysis

Scopus extraction utilities for bibliometric analysis with a reproducible, non-interactive workflow.

## Prerequisites

- Python **3.10+**
- A valid Scopus API key (`SCOPUS_API_KEY`)
- Optional institutional token (`INSTTOKEN`) if your institution requires it

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
pytest -q
python -m pybibliometric_analysis extract --help
```

## Install (editable + dev)

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

If you hit build isolation or proxy issues during install, retry with:

```bash
python -m pip install -e ".[dev]" --no-build-isolation
```

## Scopus credentials

You can provide credentials via environment variables or a local key file.

**Environment variables (preferred for automation):**

```bash
export SCOPUS_API_KEY="your-api-key"
export INSTTOKEN="optional-institution-token"
```

**Local key file:**

```bash
cp config/scopus_api_key.txt.example config/scopus_api_key.txt
```

Replace the placeholder line with your API key.

## Pybliometrics configuration

The extractor expects `pybliometrics.cfg` inside a directory (default: `config/pybliometrics`).

- If `config/pybliometrics/pybliometrics.cfg` **exists**, it will be used as-is.
- If it **does not exist** and `SCOPUS_API_KEY` is set, a config file is created automatically in
  `config/pybliometrics/pybliometrics.cfg` with cache directories under `.cache/pybliometrics`.

Override the config directory with:

```bash
python -m pybibliometric_analysis extract \
  --pybliometrics-config-dir /path/to/pybliometrics
```

Use the example template if you prefer manual setup:

```bash
cp config/pybliometrics/pybliometrics.cfg.example config/pybliometrics/pybliometrics.cfg
```

## Phase 1 / 2 / 3 commands

### Phase 1: Sanity checks

```bash
python -m pip install -e ".[dev]"
pytest -q
python -m pybibliometric_analysis extract --help
```

### Phase 2: Extract (Scopus)

Edit `config/search.yaml` or provide your own config file.

```yaml
# config/search.yaml
query: 'TITLE-ABS-KEY("islamic finance")'
database: 'Scopus'
notes: 'no limits'
use_cursor_preferred: true
```

Run extract:

```bash
python -m pybibliometric_analysis extract \
  --config config/search.yaml \
  --pybliometrics-config-dir config/pybliometrics
```

### Phase 3: Cleaning (example)

There is no dedicated cleaning script yet. The recommended pattern is to read from `data/raw`
and write to `data/processed`. Example (copy-paste):

```bash
python - <<'PY'
from pathlib import Path
import pandas as pd

raw_path = sorted(Path("data/raw").glob("scopus_search_*.parquet"))[-1]
processed_path = Path("data/processed") / raw_path.name.replace("scopus_search_", "scopus_clean_")
processed_path.parent.mkdir(parents=True, exist_ok=True)

(df := pd.read_parquet(raw_path))
(df.drop_duplicates(subset=["eid"]) if "eid" in df.columns else df).to_parquet(processed_path, index=False)
print(f"Wrote {processed_path}")
PY
```

## Output structure

- `data/raw/` — raw Scopus parquet files (`scopus_search_<run_id>.parquet`)
- `data/processed/` — cleaned or transformed data (user-defined)
- `outputs/methods/` — run manifest (`search_manifest_<run_id>.json`)
- `outputs/analysis/` — downstream analysis artifacts (user-defined)
- `logs/` — extraction logs (`extract_<run_id>.log`)

## Troubleshooting

- **401/403 from Scopus**: verify `SCOPUS_API_KEY`, and if your institution needs it, `INSTTOKEN`.
- **Subscriber-only cursor results**: if cursor queries fail, set `--force-slicing` to force
  year slicing or set `use_cursor_preferred: false` in the config.
- **pyarrow/numpy ABI errors**: reinstall the environment (prefer a clean venv) and re-run
  `python -m pip install -e ".[dev]"`.
- **Proxy/pip build isolation**: re-run install with `--no-build-isolation` or configure
  `PIP_INDEX_URL`/`PIP_EXTRA_INDEX_URL`/`HTTPS_PROXY` for your environment.

## End-to-end (copy-paste)

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
export SCOPUS_API_KEY="your-api-key"
export INSTTOKEN="optional-institution-token"
python -m pybibliometric_analysis extract \
  --run-id smoke-001 \
  --config config/search.yaml \
  --pybliometrics-config-dir config/pybliometrics
```
