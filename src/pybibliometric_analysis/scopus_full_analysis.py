from __future__ import annotations

import json
import logging
import re
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


def setup_logging(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pybibliometric_analysis.full_analysis")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    logger.handlers = []
    fh = logging.FileHandler(log_path)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger


def _norm_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _split_keywords(value: object) -> list[str]:
    if pd.isna(value):
        return []
    return [_norm_text(part) for part in str(value).split(";") if _norm_text(part)]


def _first_author(authors: object) -> str:
    if pd.isna(authors):
        return ""
    text = str(authors).strip()
    if not text:
        return ""
    return _norm_text(text.split(";")[0])


def _coalesce_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


def _top_counts(series: pd.Series, n: int = 25, out_col: str = "value") -> pd.DataFrame:
    if series.empty:
        return pd.DataFrame(columns=[out_col, "count"])
    values = series.fillna("").astype(str).str.strip()
    values = values[values != ""]
    out = values.value_counts().head(n).reset_index()
    out.columns = [out_col, "count"]
    return out


def _build_plotly_figure_exports(fig: object, output_base: Path) -> None:
    output_base.parent.mkdir(parents=True, exist_ok=True)
    try:
        fig.write_html(str(output_base) + ".html", include_plotlyjs="cdn")
        fig.write_image(str(output_base) + ".png", width=1600, height=900, scale=2)
    except Exception:
        # HTML-only fallback (or no output if plotly missing entirely).
        try:
            fig.write_html(str(output_base) + ".html", include_plotlyjs="cdn")
        except Exception:
            return


def _compute_region(country: str) -> str:
    country_map = {
        "united states": "North America",
        "canada": "North America",
        "united kingdom": "Europe & Central Asia",
        "italy": "Europe & Central Asia",
        "france": "Europe & Central Asia",
        "germany": "Europe & Central Asia",
        "spain": "Europe & Central Asia",
        "turkey": "Europe & Central Asia",
        "saudi arabia": "Middle East & North Africa",
        "united arab emirates": "Middle East & North Africa",
        "qatar": "Middle East & North Africa",
        "iran": "Middle East & North Africa",
        "iran, islamic republic of": "Middle East & North Africa",
        "egypt": "Middle East & North Africa",
        "pakistan": "South Asia",
        "india": "South Asia",
        "bangladesh": "South Asia",
        "china": "East Asia & Pacific",
        "japan": "East Asia & Pacific",
        "malaysia": "East Asia & Pacific",
        "indonesia": "East Asia & Pacific",
        "singapore": "East Asia & Pacific",
        "australia": "East Asia & Pacific",
        "nigeria": "Sub-Saharan Africa",
        "south africa": "Sub-Saharan Africa",
        "brazil": "Latin America & Caribbean",
        "mexico": "Latin America & Caribbean",
    }
    return country_map.get(country.strip().lower(), "Unknown")


def _extract_countries(affiliations: object) -> set[str]:
    if pd.isna(affiliations):
        return set()
    aliases = {
        "usa": "united states",
        "us": "united states",
        "uk": "united kingdom",
        "uae": "united arab emirates",
        "viet nam": "vietnam",
    }
    countries = set()
    for segment in str(affiliations).split(";"):
        segment = segment.strip()
        if not segment:
            continue
        token = segment.split(",")[-1].strip().lower()
        token = aliases.get(token, token)
        if token:
            countries.add(token)
    return countries


def run_full_scopus_csv_analysis(
    *,
    run_id: str,
    csv_path: Path,
    base_dir: Path,
    min_year: int,
    max_year: int,
) -> None:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    base_dir = base_dir.resolve()
    output_root = base_dir / "outputs" / "full_analysis" / run_id
    fig_dir = output_root / "figures"
    table_dir = output_root / "tables"
    net_dir = output_root / "networks"
    meta_dir = output_root / "meta"
    for path in (fig_dir, table_dir, net_dir, meta_dir):
        path.mkdir(parents=True, exist_ok=True)

    logger = setup_logging(base_dir / "logs" / f"full_analysis_{run_id}.log")

    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]

    col_year = _coalesce_col(df, ["Year", "pub_year"])
    col_title = _coalesce_col(df, ["Title", "dc:title", "title"])
    if not col_year:
        col_year = _coalesce_col(df, ["coverDate", "cover_date"])
    if not col_year:
        raise ValueError("CSV must include a Year (or coverDate/cover_date) column.")

    col_eid = _coalesce_col(df, ["EID", "eid"])
    col_doi = _coalesce_col(df, ["DOI", "doi"])
    col_authors = _coalesce_col(df, ["Authors", "author_names", "dc:creator", "creator"])
    col_source = _coalesce_col(df, ["Source title", "prism:publicationName", "publicationName"])
    col_doctype = _coalesce_col(df, ["Document Type", "subtypeDescription"])
    col_ak = _coalesce_col(df, ["Author Keywords", "authkeywords", "author_keywords"])
    col_ik = _coalesce_col(df, ["Index Keywords", "idxterms"])
    col_affils = _coalesce_col(df, ["Affiliations", "affilname"])

    df["_year"] = pd.to_numeric(df[col_year], errors="coerce")
    df = df[df["_year"].notna()].copy()
    df["_year"] = df["_year"].astype(int)

    df["_eid"] = df[col_eid].fillna("").astype(str).str.strip() if col_eid else ""
    df["_doi"] = (
        df[col_doi].fillna("").astype(str).str.strip().str.lower() if col_doi else ""
    )
    if col_title:
        df["_t"] = df[col_title].map(_norm_text)
    else:
        df["_t"] = ""
    df["_a1"] = df[col_authors].map(_first_author) if col_authors else ""

    def make_key(row: pd.Series) -> str:
        if row["_eid"]:
            return f"EID:{row['_eid']}"
        if row["_doi"]:
            return f"DOI:{row['_doi']}"
        return f"FALL:{row['_t']}|{row['_year']}|{row['_a1']}"

    n_raw = len(df)
    df["_dedup_key"] = df.apply(make_key, axis=1)
    df_clean = df.drop_duplicates(subset=["_dedup_key"]).copy()

    scoped = df_clean[(df_clean["_year"] >= min_year) & (df_clean["_year"] <= max_year)].copy()
    scoped.to_csv(table_dir / "dataset_scope.csv", index=False)

    pubs_by_year = (
        scoped.groupby("_year")
        .size()
        .rename("n_pubs")
        .reset_index()
        .rename(columns={"_year": "year"})
    )
    pubs_by_year = pubs_by_year.sort_values("year")
    pubs_by_year["yoy_pct"] = pubs_by_year["n_pubs"].pct_change() * 100.0
    pubs_by_year["ma3"] = pubs_by_year["n_pubs"].rolling(3).mean()
    pubs_by_year.to_csv(table_dir / "pubs_by_year.csv", index=False)

    cagr = np.nan
    if not pubs_by_year.empty:
        start_row = pubs_by_year.iloc[0]
        end_row = pubs_by_year.iloc[-1]
        span = max(1, int(end_row["year"] - start_row["year"]))
        if start_row["n_pubs"] > 0:
            cagr = (end_row["n_pubs"] / start_row["n_pubs"]) ** (1 / span) - 1
    pd.DataFrame(
        [{"docs": len(scoped), "year_min": min_year, "year_max": max_year, "cagr": cagr}]
    ).to_csv(table_dir / "kpi_interest.csv", index=False)

    if col_source:
        _top_counts(scoped[col_source], out_col="journal").to_csv(
            table_dir / "top_journals.csv", index=False
        )
    if col_doctype:
        _top_counts(scoped[col_doctype], out_col="doctype").to_csv(
            table_dir / "top_document_types.csv", index=False
        )

    if col_authors:
        author_counts = _top_counts(
            scoped[col_authors].fillna("").astype(str).str.split(";").explode(),
            out_col="author",
        )
        author_counts.to_csv(table_dir / "top_authors.csv", index=False)

    keyword_lists = pd.Series([[]] * len(scoped), index=scoped.index)
    if col_ak:
        keyword_lists = keyword_lists + scoped[col_ak].map(_split_keywords)
    if col_ik:
        keyword_lists = keyword_lists + scoped[col_ik].map(_split_keywords)
    keyword_lists = keyword_lists.map(lambda kws: sorted(set(kws)))

    keyword_freq = keyword_lists.explode().dropna()
    keyword_freq = keyword_freq[keyword_freq != ""].value_counts().reset_index()
    keyword_freq.columns = ["keyword", "count"]
    keyword_freq.to_csv(table_dir / "keyword_freq.csv", index=False)

    # Geographical bubble aggregation (2002-2025 bins)
    bins = ["2002-2006", "2007-2011", "2012-2016", "2017-2021", "2022-2025"]

    def year_to_bin(year: int) -> Optional[str]:
        if 2002 <= year <= 2006:
            return bins[0]
        if 2007 <= year <= 2011:
            return bins[1]
        if 2012 <= year <= 2016:
            return bins[2]
        if 2017 <= year <= 2021:
            return bins[3]
        if 2022 <= year <= 2025:
            return bins[4]
        return None

    geo_rows = []
    if col_affils:
        bubble_df = df_clean[(df_clean["_year"] >= 2002) & (df_clean["_year"] <= 2025)].copy()
        for _, row in bubble_df.iterrows():
            b = year_to_bin(int(row["_year"]))
            if not b:
                continue
            countries = _extract_countries(row[col_affils])
            if not countries:
                continue
            weight = 1 / len(countries)
            for country in countries:
                geo_rows.append({"bin": b, "region": _compute_region(country), "N_pub": weight})

    geo_table = pd.DataFrame(geo_rows)
    if not geo_table.empty:
        geo_agg = geo_table.groupby(["bin", "region"], as_index=False)["N_pub"].sum()
        geo_agg.to_csv(table_dir / "bubble_regions_over_time.csv", index=False)

    # Optional visual exports with plotly/pyvis/networkx
    try:
        import plotly.express as px
        import plotly.graph_objects as go

        if not pubs_by_year.empty:
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=pubs_by_year["year"],
                    y=pubs_by_year["n_pubs"],
                    mode="lines+markers",
                    name="Pubs",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=pubs_by_year["year"],
                    y=pubs_by_year["ma3"],
                    mode="lines",
                    name="3-yr MA",
                )
            )
            fig.update_layout(title=f"Publications per year ({min_year}-{max_year})")
            _build_plotly_figure_exports(fig, fig_dir / "fig_pubs_per_year")

            fig2 = px.line(pubs_by_year, x="year", y="yoy_pct", title="YoY Growth (%)")
            _build_plotly_figure_exports(fig2, fig_dir / "fig_yoy_growth")

        if not keyword_freq.empty:
            topk = keyword_freq.head(20).sort_values("count", ascending=True)
            fig3 = px.bar(topk, x="count", y="keyword", orientation="h", title="Top Keywords")
            _build_plotly_figure_exports(fig3, fig_dir / "fig_top_keywords")

        if not geo_table.empty:
            agg = geo_table.groupby(["bin", "region"], as_index=False)["N_pub"].sum()
            max_pub = max(float(agg["N_pub"].max()), 1.0)
            agg["marker_size"] = np.sqrt(agg["N_pub"] / max_pub) * 60
            bubble = go.Figure(
                go.Scatter(
                    x=agg["bin"],
                    y=agg["region"],
                    mode="markers",
                    marker={"size": agg["marker_size"], "opacity": 0.75},
                    customdata=np.stack([agg["N_pub"]], axis=-1),
                    hovertemplate=(
                        "Bin=%{x}<br>Region=%{y}<br>"
                        "Weighted pubs=%{customdata[0]:.2f}<extra></extra>"
                    ),
                )
            )
            bubble.update_layout(title="Geographic Expansion of Research (2002-2025)")
            _build_plotly_figure_exports(bubble, fig_dir / "bubble_regions_over_time")
    except Exception:
        logger.warning("Optional plotting skipped (plotly not available).")

    try:
        import networkx as nx
        from pyvis.network import Network

        if keyword_freq.empty:
            valid_keywords: set[str] = set()
        else:
            valid_keywords = set(keyword_freq[keyword_freq["count"] >= 5]["keyword"])
        edge_counts: Counter[tuple[str, str]] = Counter()
        for kws in keyword_lists:
            kws = sorted(set(k for k in kws if k in valid_keywords))
            for i in range(len(kws)):
                for j in range(i + 1, len(kws)):
                    edge_counts[(kws[i], kws[j])] += 1

        if edge_counts:
            edges = pd.DataFrame(
                [{"from": a, "to": b, "value": w} for (a, b), w in edge_counts.items()]
            ).sort_values("value", ascending=False).head(400)
            graph = nx.from_pandas_edgelist(edges, "from", "to", "value")
            net = Network(height="800px", width="100%", bgcolor="white", font_color="black")
            net.from_nx(graph)
            net.write_html(str(net_dir / "keyword_network.html"))
    except Exception:
        logger.warning("Optional keyword network skipped (networkx/pyvis not available).")

    manifest = {
        "run_id": run_id,
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "csv_path": str(csv_path),
        "output_root": str(output_root),
        "rows_raw": n_raw,
        "rows_deduplicated": int(len(df_clean)),
        "rows_scoped": int(len(scoped)),
        "year_scope": {"min_year": min_year, "max_year": max_year},
    }
    (meta_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    archive_path = shutil.make_archive(str(output_root), "zip", str(output_root))
    logger.info("Full analysis complete. ZIP archive: %s", archive_path)
