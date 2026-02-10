#!/usr/bin/env python3
"""Genera un report HTML completo dell'analisi bibliometrica."""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


def _find_latest_run_id(base_dir: Path) -> Optional[str]:
    methods_dir = base_dir / "outputs" / "methods"
    candidates = list(methods_dir.glob("search_manifest_*.json"))
    if candidates:
        latest = max(candidates, key=lambda p: p.stat().st_mtime)
        return latest.stem.replace("search_manifest_", "", 1)
    analysis_dir = base_dir / "outputs" / "analysis"
    candidates = list(analysis_dir.glob("pubs_by_year_*.csv"))
    if candidates:
        latest = max(candidates, key=lambda p: p.stat().st_mtime)
        return latest.stem.replace("pubs_by_year_", "", 1)
    return None


def _read_csv(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    return pd.read_csv(path).to_dict(orient="records")


def _safe_int(value, default=0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _resolve_existing_file(base_dir: Path, relative_base: str) -> Optional[Path]:
    base_path = base_dir / relative_base
    for suffix in (".parquet", ".csv"):
        candidate = base_path.with_suffix(suffix)
        if candidate.exists():
            return candidate
    return None


def create_html_report(run_id: Optional[str], base_dir: Path) -> Path:
    """Crea un report HTML con i risultati dell'analisi."""
    output_dir = base_dir / "results_complete"
    output_dir.mkdir(exist_ok=True)

    resolved_run_id = run_id or _find_latest_run_id(base_dir)
    if not resolved_run_id:
        raise SystemExit("Nessun run_id trovato. Esegui prima l'analisi.")

    analysis_files = {
        "top_authors": base_dir / "outputs" / "analysis" / f"top_authors_{resolved_run_id}.csv",
        "top_journals": base_dir / "outputs" / "analysis" / f"top_journals_{resolved_run_id}.csv",
        "pubs_by_year": base_dir / "outputs" / "analysis" / f"pubs_by_year_{resolved_run_id}.csv",
        "keyword_freq": base_dir / "outputs" / "analysis" / f"keyword_freq_{resolved_run_id}.csv",
        "yoy_growth": base_dir / "outputs" / "analysis" / f"yoy_growth_{resolved_run_id}.csv",
        "cagr": base_dir / "outputs" / "analysis" / f"cagr_{resolved_run_id}.csv",
    }

    data = {key: _read_csv(path) for key, path in analysis_files.items()}

    manifest_path = (
        base_dir / "outputs" / "methods" / f"search_manifest_{resolved_run_id}.json"
    )
    manifest = {}
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    pubs_by_year = data.get("pubs_by_year", [])
    total_pubs = sum(_safe_int(d.get("count", 0)) for d in pubs_by_year)

    def fmt_year(value):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return "N/A"

    html = f"""<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Analisi Bibliometrica - Report Completo</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }}

        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            overflow: hidden;
        }}

        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }}

        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}

        .header p {{
            font-size: 1.1em;
            opacity: 0.9;
        }}

        .content {{
            padding: 40px;
        }}

        .info-box {{
            background: #f8f9fa;
            border-left: 4px solid #667eea;
            padding: 20px;
            margin-bottom: 30px;
            border-radius: 5px;
        }}

        .info-box h2 {{
            color: #667eea;
            font-size: 1.3em;
            margin-bottom: 10px;
        }}

        .info-box p {{
            color: #555;
            margin: 5px 0;
        }}

        .section {{
            margin-bottom: 40px;
        }}

        .section h2 {{
            color: #667eea;
            font-size: 1.8em;
            margin-bottom: 20px;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }}

        table th {{
            background: #667eea;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: 600;
        }}

        table td {{
            padding: 12px;
            border-bottom: 1px solid #eee;
        }}

        table tr:hover {{
            background: #f5f5f5;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}

        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}

        .stat-card .number {{
            font-size: 2.5em;
            font-weight: bold;
            margin-bottom: 10px;
        }}

        .stat-card .label {{
            font-size: 1em;
            opacity: 0.9;
        }}

        .footer {{
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            color: #666;
            border-top: 1px solid #eee;
        }}

        .empty {{
            color: #999;
            font-style: italic;
            padding: 20px;
            text-align: center;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>?? Analisi Bibliometrica</h1>
            <p>Report Completo - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
            <p>Run ID: {resolved_run_id}</p>
        </div>

        <div class="content">
            <div class="info-box">
                <h2>?? Informazioni Estrazione</h2>
                <p><strong>Query:</strong> {manifest.get('query', 'N/A')}</p>
                <p><strong>Database:</strong> {manifest.get('database', 'N/A')}</p>
                <p><strong>Run ID:</strong> {manifest.get('run_id', resolved_run_id)}</p>
                <p><strong>Runtime:</strong> {manifest.get('timestamp_utc', 'N/A')}</p>
                <p><strong>Python:</strong> {manifest.get('python_version', 'N/A')}</p>
            </div>

            <div class="stats-grid">
                <div class="stat-card">
                    <div class="number">{len(data.get('top_authors', []))}</div>
                    <div class="label">Autori Unici</div>
                </div>
                <div class="stat-card">
                    <div class="number">{len(data.get('top_journals', []))}</div>
                    <div class="label">Riviste</div>
                </div>
                <div class="stat-card">
                    <div class="number">{len(pubs_by_year)}</div>
                    <div class="label">Anni Coperti</div>
                </div>
                <div class="stat-card">
                    <div class="number">{total_pubs}</div>
                    <div class="label">Pubblicazioni Totali</div>
                </div>
            </div>

            <div class="section">
                <h2>?? Top 10 Autori</h2>
                {('<table><thead><tr><th>Autore</th><th>Pubblicazioni</th></tr></thead><tbody>' + ''.join(
                    f'<tr><td>{d.get("item", "N/A")}</td><td>{d.get("count", 0)}</td></tr>'
                    for d in data.get('top_authors', [])
                ) + '</tbody></table>') if data.get('top_authors') else '<p class="empty">Nessun dato disponibile</p>'}
            </div>

            <div class="section">
                <h2>?? Top Riviste</h2>
                {('<table><thead><tr><th>Rivista</th><th>Pubblicazioni</th></tr></thead><tbody>' + ''.join(
                    f'<tr><td>{d.get("journal", "N/A")}</td><td>{d.get("count", 0)}</td></tr>'
                    for d in data.get('top_journals', [])
                ) + '</tbody></table>') if data.get('top_journals') else '<p class="empty">Nessun dato disponibile</p>'}
            </div>

            <div class="section">
                <h2>?? Pubblicazioni per Anno</h2>
                {('<table><thead><tr><th>Anno</th><th>Numero Pubblicazioni</th></tr></thead><tbody>' + ''.join(
                    f'<tr><td>{fmt_year(d.get("pub_year", "N/A"))}</td><td>{d.get("count", 0)}</td></tr>'
                    for d in sorted(pubs_by_year, key=lambda x: x.get('pub_year', 0))
                ) + '</tbody></table>') if pubs_by_year else '<p class="empty">Nessun dato disponibile</p>'}
            </div>

            <div class="section">
                <h2>?? Parole Chiave Principali</h2>
                {('<table><thead><tr><th>Parola Chiave</th><th>Frequenza</th></tr></thead><tbody>' + ''.join(
                    f'<tr><td>{d.get("keyword", "N/A")}</td><td>{d.get("frequency", 0)}</td></tr>'
                    for d in data.get('keyword_freq', [])
                ) + '</tbody></table>') if data.get('keyword_freq') else '<p class="empty">Nessun dato disponibile</p>'}
            </div>

            <div class="section">
                <h2>?? Crescita Anno su Anno (YoY)</h2>
                {('<table><thead><tr><th>Anno</th><th>Crescita %</th></tr></thead><tbody>' + ''.join(
                    f'<tr><td>{d.get("year", "N/A")}</td><td>{d.get("yoy_pct", "N/A")}</td></tr>'
                    for d in data.get('yoy_growth', [])
                ) + '</tbody></table>') if data.get('yoy_growth') else '<p class="empty">Nessun dato disponibile</p>'}
            </div>

            <div class="section">
                <h2>?? CAGR (Compound Annual Growth Rate)</h2>
                {('<table><thead><tr><th>Metrica</th><th>Valore</th></tr></thead><tbody>' + ''.join(
                    f'<tr><td>{d.get("metric", "N/A")}</td><td>{d.get("cagr", "N/A")}</td></tr>'
                    for d in data.get('cagr', [])
                ) + '</tbody></table>') if data.get('cagr') else '<p class="empty">Nessun dato disponibile</p>'}
            </div>
        </div>

        <div class="footer">
            <p>Report generato automaticamente da pybibliometric_analysis</p>
            <p>? 2026 - Analisi Bibliometrica</p>
        </div>
    </div>
</body>
</html>"""

    report_path = output_dir / "report.html"
    report_path.write_text(html, encoding="utf-8")

    for key, filepath in analysis_files.items():
        if filepath.exists():
            dest = output_dir / filepath.name
            dest.write_bytes(filepath.read_bytes())

    if manifest_path.exists():
        manifest_dest = output_dir / manifest_path.name
        manifest_dest.write_bytes(manifest_path.read_bytes())

    raw_file = _resolve_existing_file(base_dir, f"data/raw/scopus_search_{resolved_run_id}")
    processed_file = _resolve_existing_file(
        base_dir, f"data/processed/scopus_clean_{resolved_run_id}"
    )

    if raw_file:
        dest = output_dir / ("raw_data" + raw_file.suffix)
        dest.write_bytes(raw_file.read_bytes())

    if processed_file:
        dest = output_dir / ("clean_data" + processed_file.suffix)
        dest.write_bytes(processed_file.read_bytes())

    return report_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Genera il report HTML dell'analisi bibliometrica")
    parser.add_argument("--run-id", dest="run_id", default=None, help="Run ID da usare")
    parser.add_argument(
        "--base-dir", dest="base_dir", default=".", help="Base directory del progetto"
    )
    args = parser.parse_args()

    report_path = create_html_report(args.run_id, Path(args.base_dir))
    print(f"? Report HTML creato: {report_path}")


if __name__ == "__main__":
    main()
