# pybibliometric_analysis

Scopus bibliometric pipeline with a reproducible, non-interactive workflow for extraction, cleaning, and analysis.

## Pipeline overview (Phase 1–4)

- **Phase 1 — sanity**: install, run tests offline, verify CLI help
- **Phase 2 — extract**: query Scopus and store raw data + manifest
- **Phase 3 — clean**: normalize, deduplicate, generate base analysis tables + manifest
- **Phase 4 — analyze**: compute YoY/CAGR trends + figures

## Installation

Requires Python **>=3.10**.

Recommended (parquet + dev tools + figures):

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,parquet,viz]"
```

Minimal (CSV fallback; no parquet engine):

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

If your environment uses a proxy or build isolation fails, retry with:

```bash
python -m pip install -e . --no-build-isolation
```

## Security note (credentials)

**Never commit credentials.** Secret files are gitignored. Prefer environment variables in automation.

## Authentication (no prompts)

Set credentials via environment variables (preferred for automation):

```bash
export SCOPUS_API_KEY="your-api-key"
export INST_TOKEN="optional-institution-token"
```

`INSTTOKEN` is also accepted for backward compatibility, but `INST_TOKEN` is preferred.

Or use local files (gitignored) and pass paths explicitly:

```bash
cp config/scopus_api_key.txt.example config/scopus_api_key.txt
cp config/inst_token.txt.example config/inst_token.txt

python -m pybibliometric_analysis extract \
  --config config/search.yaml \
  --scopus-api-key-file config/scopus_api_key.txt \
  --inst-token-file config/inst_token.txt
```

Never commit files containing real credentials.

The extractor writes `config/pybliometrics/pybliometrics.cfg` automatically if missing and
`SCOPUS_API_KEY` is present. If you want to manage it manually, copy the example:

```bash
cp config/pybliometrics/pybliometrics.cfg.example config/pybliometrics/pybliometrics.cfg
```

## Phase 1 → Phase 2 (scripted, stop on failure)

```bash
set -euo pipefail

echo "=== PHASE 1: sanity ==="
python --version
python -m pip --version

# Install (choose one)
python -m pip install -e ".[dev,parquet]"   # recommended (parquet)
# python -m pip install -e ".[dev]"         # minimal

pytest -q

# CLI availability checks (keep only commands that exist in this repo)
python -m pybibliometric_analysis extract --help
python -m pybibliometric_analysis clean --help
python -m pybibliometric_analysis analyze --help

git status -sb
echo "✅ PHASE 1 OK"

echo "=== PHASE 2: extract (smoke) ==="
: "${SCOPUS_API_KEY:?Set SCOPUS_API_KEY in your environment}"
# export INST_TOKEN="..."  # optional (institution token)

if [ -f config/search_trend.yaml ]; then
  CONFIG_PATH=config/search_trend.yaml
elif [ -f config/search.yaml ]; then
  CONFIG_PATH=config/search.yaml
else
  echo "ERROR: no config/search_trend.yaml or config/search.yaml found" >&2
  exit 1
fi

if [ -f config/search_trend.yaml ]; then
  CONFIG_PATH=config/search_trend.yaml
elif [ -f config/search.yaml ]; then
  CONFIG_PATH=config/search.yaml
else
  echo "ERROR: no config/search_trend.yaml or config/search.yaml found" >&2
  exit 1
fi

RUN_ID="smoke-$(date -u +%Y%m%dT%H%M%SZ)"

echo "RUN_ID=$RUN_ID"

echo "Using config: $CONFIG_PATH"
python -m pybibliometric_analysis extract \
  --run-id "$RUN_ID" \
  --config "$CONFIG_PATH" \
  --pybliometrics-config-dir config/pybliometrics

echo "✅ PHASE 2 OK (smoke)"
```

## Phase 2 — extract (Scopus)

Example trend query:

```bash
python -m pybibliometric_analysis extract \
  --run-id trend-001 \
  --config config/search_trend.yaml \
  --pybliometrics-config-dir config/pybliometrics \
  --scopus-api-key-file config/scopus_api_key.txt \
  --inst-token-file config/inst_token.txt
```

Dry-run (no network; validates config/credentials and writes a manifest):

```bash
python -m pybibliometric_analysis extract \
  --run-id dryrun-001 \
  --config config/search.yaml \
  --dry-run
```

If `--run-id` is omitted, the extractor prints the auto-generated value as `RUN_ID=smoke-<UTC timestamp>`.

If your account is not a subscriber, set `subscriber_mode: false` and `use_cursor_preferred: false`
in the config or use `--force-slicing` to avoid cursor mode. Slicing requires `start_year` and
`end_year` in the YAML config.

## Phase 3 — clean

```bash
python -m pybibliometric_analysis clean --run-id trend-001
```

Outputs include cleaned data, manifests, and base analysis tables.

## Phase 4 — analyze

```bash
python -m pybibliometric_analysis analyze --run-id trend-001 --figures
```

Figures are optional; if matplotlib is missing, analysis still runs (no crash).

## Phase 2 verification (manifest check)

```bash
RUN_ID="smoke-YYYYMMDDTHHMMSSZ"
python - "$RUN_ID" <<'PY'
import json, pathlib, sys

run_id = sys.argv[1]
m = pathlib.Path("outputs/methods") / f"search_manifest_{run_id}.json"
if not m.exists():
    raise SystemExit(f"ERROR: manifest not found: {m}")
data = json.loads(m.read_text(encoding="utf-8"))
print("schema_version:", data.get("schema_version"))
print("database:", data.get("database"))
print("run_id:", data.get("run_id"))
print("timestamp_utc:", data.get("timestamp_utc"))
print("n_results_estimated:", data.get("n_results_estimated"))
print("n_records_downloaded:", data.get("n_records_downloaded"))
print("strategy_used:", data.get("strategy_used"))
cols = data.get("columns_present") or []
print("columns_present (n):", len(cols))
raw_parquet = pathlib.Path("data/raw") / f"scopus_search_{run_id}.parquet"
raw_csv = pathlib.Path("data/raw") / f"scopus_search_{run_id}.csv"
print("raw parquet exists:", raw_parquet.exists())
print("raw csv exists:", raw_csv.exists())
PY
```

## Output structure

- `data/raw/` — raw Scopus results (`scopus_search_<RUN_ID>.parquet|csv`)
- `data/processed/` — cleaned dataset (`scopus_clean_<RUN_ID>.parquet|csv`)
- `outputs/methods/` — manifests (`search_manifest_<RUN_ID>.json`, `cleaning_manifest_<RUN_ID>.json`)
- `outputs/analysis/` — tables (`pubs_by_year_<RUN_ID>.csv`, `top_journals_<RUN_ID>.csv`, `top_authors_<RUN_ID>.csv`, `keyword_freq_<RUN_ID>.csv`, `yoy_growth_<RUN_ID>.csv`, `cagr_<RUN_ID>.csv`)
- `outputs/figures/` — plots (`pubs_by_year_<RUN_ID>.png`, `yoy_growth_<RUN_ID>.png`)
- `logs/` — log files

All outputs are deterministic by `RUN_ID`; reusing the same `--run-id` reproduces the same filenames.

## Full trend (growth over time)

A single extraction with a multi-year query yields a full trend series:

```bash
RUN_ID="trend-$(date -u +%Y%m%dT%H%M%SZ)"
python -m pybibliometric_analysis extract --run-id "$RUN_ID" --config config/search_trend.yaml
python -m pybibliometric_analysis clean --run-id "$RUN_ID"
python -m pybibliometric_analysis analyze --run-id "$RUN_ID"

python - <<'PY'
import pandas as pd
from pathlib import Path

run_id = Path("outputs/analysis").glob("cagr_*.csv")
run_id = sorted(run_id)[-1].stem.replace("cagr_", "")
summary = pd.read_csv(f"outputs/analysis/cagr_{run_id}.csv")
print(summary)
PY
```

If only one publication year exists, CAGR is set to `0.0` by design.

## Reproducibility checklist

All items below are recorded in `outputs/methods/search_manifest_<RUN_ID>.json` and
`outputs/methods/cleaning_manifest_<RUN_ID>.json`:

- `run_id`
- extraction `timestamp_utc`
- exact Scopus `query`
- `config_path` and `config_hash`
- strategy (`strategy_used`) and `subscriber_mode`
- `n_results_estimated` and `n_records_downloaded`
- output paths (`output_paths`)
- git commit hash (`git_commit`)
- Python + package versions (`python_version`, `package_versions`)
- **Note**: tokens are never stored or logged.

## Methods reporting snippet (paper-ready)

> We queried Scopus using the exact query recorded in `search_manifest_<RUN_ID>.json` and cleaned
> the results following `cleaning_manifest_<RUN_ID>.json`. The annual publication series
> (`pubs_by_year_<RUN_ID>.csv`) was analyzed for growth using `yoy_growth_<RUN_ID>.csv` and
> `cagr_<RUN_ID>.csv`, which support the claim that scientific interest in Islamic finance
> is increasing over time.

## Troubleshooting

- **401**: `SCOPUS_API_KEY` is invalid or disabled.
- **403**: access requires `INST_TOKEN` or an institutional subscription.
- **429 rate limit**: reduce request volume or retry later.
- **Parquet engine missing or ABI mismatch**: install `pyarrow` via `.[parquet]` or use the CSV fallback.
- **Proxy/build isolation**: retry install with `python -m pip install -e . --no-build-isolation`.
