# pybibliometric_analysis

Scopus bibliometric pipeline with a reproducible, non-interactive workflow for extraction, cleaning, and analysis.

## Pipeline overview (Phase 1–4)

- **Phase 1 — sanity**: install, run tests offline, verify CLI help
- **Phase 2 — extract**: query Scopus and store raw data + manifest
- **Phase 3 — clean**: normalize, deduplicate, generate base analysis tables + manifest
- **Phase 4 — analyze**: compute YoY/CAGR trends + figures

## Installation

Recommended (parquet + dev tools):

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,parquet]"
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

## Authentication (no prompts)

Set credentials via environment variables (preferred for automation):

```bash
export SCOPUS_API_KEY="your-api-key"
export INST_TOKEN="optional-institution-token"
```

`INSTTOKEN` is also accepted for backward compatibility, but `INST_TOKEN` is preferred.

Or use local files:

```bash
cp config/scopus_api_key.txt.example config/scopus_api_key.txt
cp config/inst_token.txt.example config/inst_token.txt
```

The extractor writes `config/pybliometrics/pybliometrics.cfg` automatically if missing and
`SCOPUS_API_KEY` is present. If you want to manage it manually, copy the example:

```bash
cp config/pybliometrics/pybliometrics.cfg.example config/pybliometrics/pybliometrics.cfg
```

## Quickstart (end-to-end)

```bash
python -m pybibliometric_analysis extract \
  --config config/search_trend.yaml \
  --pybliometrics-config-dir config/pybliometrics

python -m pybibliometric_analysis clean --run-id <RUN_ID>

python -m pybibliometric_analysis analyze --run-id <RUN_ID> --figures
```

Use `config/search.yaml` for a minimal query or `config/search_trend.yaml` for journal articles.

## Phase 1 — sanity

```bash
python -m pip install -e ".[dev]"
pytest -q
python -m pybibliometric_analysis extract --help
python -m pybibliometric_analysis clean --help
python -m pybibliometric_analysis analyze --help
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

If your account is not a subscriber, set `use_cursor_preferred: false` in the config or use
`--force-slicing` to avoid cursor mode.

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

## Output structure

- `data/raw/` — raw Scopus results (`scopus_search_<RUN_ID>.parquet|csv`)
- `data/processed/` — cleaned dataset (`scopus_clean_<RUN_ID>.parquet|csv`)
- `outputs/methods/` — manifests (`search_manifest_*.json`, `cleaning_manifest_*.json`)
- `outputs/analysis/` — tables (`pubs_by_year_*.csv`, `top_journals_*.csv`, `yoy_growth_*.csv`)
- `outputs/figures/` — plots (`pubs_by_year_*.png`, `yoy_growth_*.png`)
- `logs/` — log files

## Reproducibility

- Use a deterministic `--run-id` to reproduce the same filenames and manifests.
- Manifests record package versions, git commit (if available), and output paths.
- If parquet is unavailable, outputs fall back to CSV automatically.

## Troubleshooting

- **401/403**: validate `SCOPUS_API_KEY`, and add `INST_TOKEN` if your institution requires it.
- **429 rate limit**: reduce request volume or retry later.
- **Parquet engine missing or ABI mismatch**: install `pyarrow` via `.[parquet]` or use the CSV fallback.
- **Proxy/build isolation**: retry install with `python -m pip install -e . --no-build-isolation`.

## Paper-ready snippet

Use this template to describe growth based on outputs in `outputs/analysis/`:

> We queried Scopus on `<DATE>` using the query `<QUERY>` and extracted `<N>` records. The annual
> publication series shows a compound annual growth rate (CAGR) of `<CAGR>` between `<START_YEAR>`
> and `<END_YEAR>`, with year-over-year changes reported in `yoy_growth_<RUN_ID>.csv`.

Replace `<DATE>`, `<QUERY>`, `<N>`, and the metrics using the generated manifests and analysis outputs.
