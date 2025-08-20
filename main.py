"""pdf2db ETL Pipeline Entrypoint.

Usage:
	python main.py --pdf path/to/input.pdf

Environment:
	- DATABASE_URL: SQLAlchemy database URL (e.g., postgresql+psycopg2://user:pass@host:5432/db)
	- TARGET_TABLE: Name of the existing DB table to append to (default: pdf_data)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import pandas as pd

from extractor import extract_tables
from transformer import transform_dataframe
from loader import get_database_url, get_target_table_name, load_dataframe_to_db


def _configure_logging() -> None:
	logging.basicConfig(
		level=logging.INFO,
		format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
	)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Extract tables from PDF and load into a database")
	parser.add_argument("--pdf", required=True, help="Path to the input PDF file")
	return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
	_configure_logging()
	args = parse_args(argv)

	pdf_path = args.pdf
	if not os.path.isfile(pdf_path):
		logging.error("PDF path does not exist: %s", pdf_path)
		return 2

	try:
		# 1) Extract
		raw_df: pd.DataFrame = extract_tables(pdf_path)
		# Print extracted DataFrame to console
		print("Extracted DataFrame:")
		print(raw_df[:10])

		# 2) Transform
		clean_df: pd.DataFrame = transform_dataframe(raw_df)
		print("Transformed DataFrame:")
		print(clean_df[:10])

		# 3) Load
		database_url = get_database_url()
		table_name = get_target_table_name()
		load_dataframe_to_db(clean_df, database_url, table_name)

		return 0
	except Exception as exc:
		logging.exception("Pipeline failed: %s", exc)
		return 1


if __name__ == "__main__":
	sys.exit(main()) 