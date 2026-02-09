import argparse
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pybibliometric_analysis")
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser("extract", help="Run Scopus extraction")
    extract_parser.add_argument(
        "--run-id",
        dest="run_id",
        default=None,
        help="Run identifier (defaults to smoke-<UTC timestamp>)",
    )
    extract_parser.add_argument(
        "--config",
        dest="config_path",
        default="config/search.yaml",
        help="Path to search YAML config",
    )
    extract_parser.add_argument(
        "--pybliometrics-config-dir",
        dest="pybliometrics_config_dir",
        default="config/pybliometrics/",
        help="Directory containing pybliometrics.cfg",
    )
    extract_parser.add_argument(
        "--scopus-api-key-file",
        dest="scopus_api_key_file",
        default="config/scopus_api_key.txt",
        help="Path to a local file containing the Scopus API key",
    )
    extract_parser.add_argument(
        "--inst-token-file",
        dest="inst_token_file",
        default="config/inst_token.txt",
        help="Path to a local file containing the optional InstToken",
    )
    extract_parser.add_argument(
        "--view",
        dest="view",
        default=None,
        choices=["STANDARD", "COMPLETE"],
        help="ScopusSearch view (STANDARD or COMPLETE)",
    )
    extract_parser.add_argument(
        "--force-slicing",
        dest="force_slicing",
        action="store_true",
        help="Force year slicing strategy",
    )
    extract_parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Validate config/credentials and write a dry-run manifest (no network)",
    )

    clean_parser = subparsers.add_parser("clean", help="Clean and normalize Scopus data")
    clean_parser.add_argument("--run-id", dest="run_id", required=True, help="Run identifier")
    clean_parser.add_argument(
        "--base-dir",
        dest="base_dir",
        default=".",
        help="Base directory for data/outputs/logs",
    )
    clean_parser.add_argument(
        "--input",
        dest="input_path",
        default=None,
        help="Explicit input file path (parquet/csv)",
    )
    clean_parser.add_argument(
        "--force",
        dest="force",
        action="store_true",
        help="Overwrite existing outputs",
    )
    clean_parser.add_argument(
        "--write-format",
        dest="write_format",
        choices=["auto", "parquet", "csv"],
        default="auto",
        help="Preferred output format",
    )

    analyze_parser = subparsers.add_parser("analyze", help="Analyze cleaned bibliometrics data")
    analyze_parser.add_argument("--run-id", dest="run_id", required=True, help="Run identifier")
    analyze_parser.add_argument(
        "--base-dir",
        dest="base_dir",
        default=".",
        help="Base directory for data/outputs/logs",
    )
    analyze_parser.add_argument(
        "--input",
        dest="input_path",
        default=None,
        help="Explicit input file path (parquet/csv)",
    )
    analyze_parser.add_argument(
        "--figures",
        dest="figures",
        action="store_true",
        help="Generate PNG figures (requires matplotlib)",
    )
    analyze_parser.add_argument(
        "--no-figures",
        dest="figures",
        action="store_false",
        help="Skip figure generation",
    )
    analyze_parser.set_defaults(figures=True)
    analyze_parser.add_argument("--min-year", dest="min_year", type=int, default=None)
    analyze_parser.add_argument("--max-year", dest="max_year", type=int, default=None)
    return parser


def main() -> None:
    from pybibliometric_analysis.settings import generate_run_id

    parser = build_parser()
    args = parser.parse_args()

    if args.command == "extract":
        from pybibliometric_analysis.extract_scopus import run_extract

        run_id = args.run_id or generate_run_id()
        if args.run_id is None:
            print(f"RUN_ID={run_id}")
        run_extract(
            run_id=run_id,
            config_path=Path(args.config_path),
            pybliometrics_config_dir=Path(args.pybliometrics_config_dir),
            scopus_api_key_file=Path(args.scopus_api_key_file),
            inst_token_file=Path(args.inst_token_file),
            view=args.view,
            force_slicing=args.force_slicing,
            dry_run=args.dry_run,
        )
    elif args.command == "clean":
        from pybibliometric_analysis.clean_scopus import run_clean

        run_clean(
            run_id=args.run_id,
            base_dir=Path(args.base_dir),
            input_path=Path(args.input_path) if args.input_path else None,
            force=args.force,
            write_format=args.write_format,
        )
    elif args.command == "analyze":
        from pybibliometric_analysis.analyze_bibliometrics import run_analyze

        run_analyze(
            run_id=args.run_id,
            base_dir=Path(args.base_dir),
            input_path=Path(args.input_path) if args.input_path else None,
            figures=args.figures,
            min_year=args.min_year,
            max_year=args.max_year,
        )


if __name__ == "__main__":
    main()
