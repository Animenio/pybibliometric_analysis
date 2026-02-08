import argparse
from pathlib import Path

from pybibliometric_analysis.extract_scopus import run_extract
from pybibliometric_analysis.settings import generate_run_id


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pybibliometric_analysis")
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser("extract", help="Run Scopus extraction")
    extract_parser.add_argument(
        "--run-id",
        dest="run_id",
        default=None,
        help="Run identifier (defaults to UTC timestamp)",
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
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "extract":
        run_id = args.run_id or generate_run_id()
        run_extract(
            run_id=run_id,
            config_path=Path(args.config_path),
            pybliometrics_config_dir=Path(args.pybliometrics_config_dir),
            scopus_api_key_file=Path(args.scopus_api_key_file),
            view=args.view,
            force_slicing=args.force_slicing,
        )


if __name__ == "__main__":
    main()
