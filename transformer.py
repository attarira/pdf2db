"""Data Transformation Module.

This module cleans and standardizes extracted data:
- Standardize column names (lowercase, underscores)
- Convert types for specific columns
"""
from __future__ import annotations

import logging
from typing import Iterable
import re

import pandas as pd


logger = logging.getLogger(__name__)


def _standardize_column_name(name: str) -> str:
	"""Normalize a single column name to snake_case lowercase without spaces."""
	return (
		name.strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_")
	)


def clean_column_headers(df: pd.DataFrame) -> pd.DataFrame:
	"""Standardize column names for a DataFrame.

	If the first row looks like a header (common with some PDF parsers),
	this function attempts to promote the first row to headers if current headers
	are integers or generic values.
	"""
	df = df.copy()

	# If headers look like 0..N or unnamed, consider first row as header candidates
	if all(isinstance(col, (int, float)) or str(col).startswith("Unnamed") for col in df.columns):
		logger.debug("Promoting first row to header based on integer/unnamed columns")
		candidate_headers = [str(v) for v in df.iloc[0].tolist()]
		if any(h and h != "nan" for h in candidate_headers):
			df = df.iloc[1:].reset_index(drop=True)
			df.columns = candidate_headers

	df.columns = [_standardize_column_name(str(c)) for c in df.columns]
	return df


def _coerce_int(series: pd.Series) -> pd.Series:
	"""Coerce a Series to pandas nullable integer (Int64)."""
	return pd.to_numeric(series, errors="coerce").astype("Int64")


def _extract_ymd_digits(value: object) -> str | None:
	"""Extract an 8-digit YYYYMMDD sequence from a value.

	Prefers the last 8-digit sequence to handle tokens like "1 20250630".
	"""
	s = "" if pd.isna(value) else str(value)
	matches = re.findall(r"(\d{8})", s)
	if not matches:
		return None
	# Prefer last match (e.g., after a leading row number)
	return matches[-1]


def _coerce_date(series: pd.Series, fmt: str = "%Y%m%d") -> pd.Series:
	"""Coerce a Series of strings like 20240131 into Python date objects.

	Using date objects helps SQLAlchemy/psycopg2 insert into DATE columns directly.
	"""
	cleaned = series.apply(_extract_ymd_digits)
	dt = pd.to_datetime(cleaned, format=fmt, errors="coerce")
	return dt.dt.date


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
	"""Clean and convert the extracted DataFrame to the desired schema.

	Expected conversions:
	- row_number -> INT
	- as_of_date -> DATE (YYYYMMDD)
	- customer_code -> BIGINT
	- date_of_restructure -> DATE (YYYYMMDD)
	"""
	df = clean_column_headers(df)

	# If row_number is missing but as_of_date contains a row number + date (e.g., "1 20250630"), split them
	if "row_number" not in df.columns and "as_of_date" in df.columns:
		pattern = re.compile(r"^\s*(\d+)\D+(\d{8})\s*$")
		series_str = df["as_of_date"].astype(str)
		mask = series_str.str.match(pattern)
		if mask.any():
			extracted = series_str.str.extract(pattern)
			# extracted[0] -> row_number, extracted[1] -> yyyymmdd
			df.loc[mask, "row_number"] = pd.to_numeric(extracted[0], errors="coerce").astype("Int64")
			df.loc[mask, "as_of_date"] = extracted[1]

	# Apply type conversions if expected columns are present
	expected_columns: Iterable[str] = (
		"row_number",
		"as_of_date",
		"customer_code",
		"date_of_restructure",
	)

	missing = [c for c in expected_columns if c not in df.columns]
	if missing:
		logger.warning("Missing expected columns: %s", ", ".join(missing))

	if "row_number" in df.columns:
		df["row_number"] = _coerce_int(df["row_number"])

	if "customer_code" in df.columns:
		df["customer_code"] = _coerce_int(df["customer_code"]).astype("Int64")

	if "as_of_date" in df.columns:
		df["as_of_date"] = _coerce_date(df["as_of_date"])

	if "date_of_restructure" in df.columns:
		df["date_of_restructure"] = _coerce_date(df["date_of_restructure"])

	logger.info("Transformed DataFrame with %d rows and %d columns", df.shape[0], df.shape[1])
	return df 