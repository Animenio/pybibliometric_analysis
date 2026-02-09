"""
Colab-ready script to run the Islamic finance bibliometric pipeline and post-analysis.

Usage in Google Colab:
1) Upload this file or copy the cells into a notebook.
2) Set SCOPUS_API_KEY in the environment before running extraction.
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
import seaborn as sns


# === Colab setup (run in a notebook cell) ===
# !git clone <YOUR_REPO_URL>
# %cd pybibliometric_analysis
# !pip -q install -e ".[parquet,viz]"


BASE_DIR = Path(".").resolve()
CONFIG_PATH = BASE_DIR / "config" / "search_islamic_finance.yaml"
RUN_ID = f"islamic-finance-{pd.Timestamp.utcnow():%Y%m%dT%H%M%SZ}"
END_YEAR = 2025


def run_pipeline(run_id: str) -> None:
    """Run extract -> clean -> analyze."""
    import os
    import subprocess

    scopus_api_key = os.environ.get("SCOPUS_API_KEY")
    if not scopus_api_key:
        raise RuntimeError("Set SCOPUS_API_KEY in the environment before running extraction.")

    subprocess.run(
        [
            "python",
            "-m",
            "pybibliometric_analysis",
            "extract",
            "--run-id",
            run_id,
            "--config",
            str(CONFIG_PATH),
            "--pybliometrics-config-dir",
            "config/pybliometrics",
        ],
        check=True,
    )
    subprocess.run(
        ["python", "-m", "pybibliometric_analysis", "clean", "--run-id", run_id],
        check=True,
    )
    subprocess.run(
        ["python", "-m", "pybibliometric_analysis", "analyze", "--run-id", run_id, "--figures"],
        check=True,
    )


def load_cleaned(run_id: str) -> pd.DataFrame:
    """Load cleaned dataset from the pipeline outputs."""
    base_path = BASE_DIR / "data" / "processed" / f"scopus_clean_{run_id}"
    parquet_path = base_path.with_suffix(".parquet")
    csv_path = base_path.with_suffix(".csv")
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if csv_path.exists():
        return pd.read_csv(csv_path)
    raise FileNotFoundError(f"No cleaned dataset found for run_id={run_id}.")


def resolve_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


def prepare_fields(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "pub_year" in df.columns:
        df["pub_year"] = pd.to_numeric(df["pub_year"], errors="coerce").astype("Int64")
    else:
        cover_col = resolve_column(df, ["coverDate", "cover_date"])
        if cover_col:
            df["pub_year"] = (
                df[cover_col].astype(str).str.slice(0, 4).where(df[cover_col].notna())
            )
            df["pub_year"] = pd.to_numeric(df["pub_year"], errors="coerce").astype("Int64")
        else:
            df["pub_year"] = pd.NA

    citations_col = resolve_column(
        df,
        [
            "citedby-count",
            "citedby_count",
            "citedby",
            "citation_count",
            "citedbyCount",
        ],
    )
    df["citations"] = pd.to_numeric(df[citations_col], errors="coerce") if citations_col else 0
    df["years_since_pub"] = (END_YEAR - df["pub_year"].astype("float") + 1).clip(lower=1)
    df["citations_per_year"] = df["citations"] / df["years_since_pub"]
    return df


def annual_production(df: pd.DataFrame) -> pd.DataFrame:
    annual = (
        df.dropna(subset=["pub_year"])
        .groupby("pub_year")
        .size()
        .rename("n_pubs")
        .reset_index()
        .sort_values("pub_year")
    )
    annual["rolling_mean_3y"] = (
        annual["n_pubs"].rolling(window=3, min_periods=1, center=True).mean()
    )
    annual["yoy_pct"] = annual["n_pubs"].pct_change() * 100
    if len(annual) > 1 and annual["n_pubs"].iloc[0] > 0:
        years = annual["pub_year"].iloc[-1] - annual["pub_year"].iloc[0]
        annual["cagr_pct"] = ((annual["n_pubs"].iloc[-1] / annual["n_pubs"].iloc[0]) ** (1 / years) - 1) * 100
    else:
        annual["cagr_pct"] = np.nan
    return annual


def impact_by_year(df: pd.DataFrame) -> pd.DataFrame:
    impact = (
        df.dropna(subset=["pub_year"])
        .groupby("pub_year")["citations_per_year"]
        .median()
        .reset_index()
        .rename(columns={"citations_per_year": "median_citations_per_year"})
        .sort_values("pub_year")
    )
    return impact


def high_impact_share(df: pd.DataFrame, percentile: float = 0.9) -> pd.DataFrame:
    threshold = df["citations_per_year"].quantile(percentile)
    df = df.copy()
    df["high_impact"] = df["citations_per_year"] >= threshold
    share = (
        df.dropna(subset=["pub_year"])
        .groupby("pub_year")["high_impact"]
        .mean()
        .reset_index()
        .rename(columns={"high_impact": "share_high_impact"})
    )
    return share


def top_sources(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    journal_col = resolve_column(
        df, ["prism:publicationName", "publicationName", "journal", "sourceTitle"]
    )
    if not journal_col:
        return pd.DataFrame(columns=["journal", "n_pubs", "median_citations_per_year"])
    summary = (
        df.groupby(journal_col)
        .agg(
            n_pubs=("citations", "size"),
            median_citations_per_year=("citations_per_year", "median"),
        )
        .reset_index()
        .rename(columns={journal_col: "journal"})
        .sort_values(["n_pubs", "median_citations_per_year"], ascending=False)
        .head(n)
    )
    return summary


def keyword_trends(df: pd.DataFrame, periods: dict[str, tuple[int, int]]) -> pd.DataFrame:
    keyword_col = resolve_column(df, ["authkeywords", "author_keywords", "keywords"])
    if not keyword_col:
        return pd.DataFrame(columns=["period", "keyword", "count"])

    records = []
    for period_name, (start, end) in periods.items():
        subset = df[(df["pub_year"] >= start) & (df["pub_year"] <= end)]
        keywords = (
            subset[keyword_col]
            .dropna()
            .astype(str)
            .str.split(";")
            .explode()
            .str.strip()
        )
        keywords = keywords[keywords != ""]
        counts = keywords.value_counts().head(25)
        for keyword, count in counts.items():
            records.append({"period": period_name, "keyword": keyword, "count": count})
    return pd.DataFrame.from_records(records)


def keyword_cooccurrence(df: pd.DataFrame, min_occ: int = 10) -> nx.Graph:
    keyword_col = resolve_column(df, ["authkeywords", "author_keywords", "keywords"])
    graph = nx.Graph()
    if not keyword_col:
        return graph

    keyword_lists = (
        df[keyword_col]
        .dropna()
        .astype(str)
        .str.split(";")
        .apply(lambda terms: [term.strip().lower() for term in terms if term.strip()])
    )

    freq = pd.Series([kw for sublist in keyword_lists for kw in sublist]).value_counts()
    keep = set(freq[freq >= min_occ].index)

    for terms in keyword_lists:
        terms = [kw for kw in terms if kw in keep]
        for i, term_a in enumerate(terms):
            for term_b in terms[i + 1 :]:
                if graph.has_edge(term_a, term_b):
                    graph[term_a][term_b]["weight"] += 1
                else:
                    graph.add_edge(term_a, term_b, weight=1)
    return graph


def plot_annual_production(annual: pd.DataFrame) -> None:
    plt.figure(figsize=(10, 5))
    sns.lineplot(data=annual, x="pub_year", y="n_pubs", label="Pubs per year")
    sns.lineplot(data=annual, x="pub_year", y="rolling_mean_3y", label="3y rolling mean")
    plt.title("Annual Scientific Production (Islamic finance)")
    plt.xlabel("Publication year")
    plt.ylabel("Number of publications")
    plt.tight_layout()
    plt.show()


def plot_impact(impact: pd.DataFrame) -> None:
    plt.figure(figsize=(10, 5))
    sns.lineplot(data=impact, x="pub_year", y="median_citations_per_year")
    plt.title("Median citations per year (time-normalized)")
    plt.xlabel("Publication year")
    plt.ylabel("Median citations/year")
    plt.tight_layout()
    plt.show()


def plot_document_types(df: pd.DataFrame) -> None:
    doc_col = resolve_column(df, ["subtypeDescription", "subtype", "document_type", "doctype"])
    if not doc_col:
        return
    summary = (
        df.dropna(subset=["pub_year"])
        .groupby(["pub_year", doc_col])
        .size()
        .reset_index(name="count")
    )
    plt.figure(figsize=(10, 6))
    sns.histplot(
        data=summary,
        x="pub_year",
        weights="count",
        hue=doc_col,
        multiple="stack",
        bins=summary["pub_year"].nunique(),
    )
    plt.title("Document types over time")
    plt.xlabel("Publication year")
    plt.ylabel("Number of publications")
    plt.tight_layout()
    plt.show()


def export_summary(run_id: str, annual: pd.DataFrame, impact: pd.DataFrame) -> None:
    output_dir = BASE_DIR / "outputs" / "analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    annual.to_csv(output_dir / f"islamic_finance_annual_{run_id}.csv", index=False)
    impact.to_csv(output_dir / f"islamic_finance_impact_{run_id}.csv", index=False)

    summary = {
        "run_id": run_id,
        "config": str(CONFIG_PATH),
        "end_year": END_YEAR,
    }
    with (output_dir / f"islamic_finance_summary_{run_id}.json").open("w") as f:
        json.dump(summary, f, indent=2)


def main() -> None:
    run_pipeline(RUN_ID)
    df = load_cleaned(RUN_ID)
    df = prepare_fields(df)

    annual = annual_production(df)
    impact = impact_by_year(df)
    high_impact = high_impact_share(df)
    sources = top_sources(df)

    periods = {
        "2002-2007": (2002, 2007),
        "2008-2015": (2008, 2015),
        "2016-2025": (2016, 2025),
    }
    keyword_table = keyword_trends(df, periods)
    graph = keyword_cooccurrence(df, min_occ=10)

    print("Annual production head:")
    print(annual.head())
    print("Median impact head:")
    print(impact.head())
    print("High-impact share head:")
    print(high_impact.head())
    print("Top sources:")
    print(sources.head(10))
    print("Keyword trends sample:")
    print(keyword_table.head())
    print(f"Keyword co-occurrence nodes: {graph.number_of_nodes()}")
    print(f"Keyword co-occurrence edges: {graph.number_of_edges()}")

    plot_annual_production(annual)
    plot_impact(impact)
    plot_document_types(df)

    export_summary(RUN_ID, annual, impact)


if __name__ == "__main__":
    main()
