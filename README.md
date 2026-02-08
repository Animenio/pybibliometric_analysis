# pybibliometric_analysis

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Setup Scopus API key

1. Copy the example config:

   ```bash
   cp config/pybliometrics/pybliometrics.cfg.example config/pybliometrics/pybliometrics.cfg
   ```

2. Add your Scopus API key to `config/pybliometrics/pybliometrics.cfg`, or set the environment variable:

   ```bash
   export SCOPUS_API_KEY="your-api-key"
   ```

If `SCOPUS_API_KEY` is set and `config/pybliometrics/pybliometrics.cfg` is missing, the extractor will
create the config automatically with repo-local cache directories.

## Run extract

```bash
python -m pybibliometric_analysis extract --config config/search.yaml \
  --pybliometrics-config-dir config/pybliometrics
```

## Tests

```bash
pytest -q
```
