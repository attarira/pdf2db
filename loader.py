"""Database Loader Module.

This module appends transformed data into a relational database using SQLAlchemy.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


logger = logging.getLogger(__name__)


def get_database_url() -> str:
	"""Fetch the database URL from environment variable DATABASE_URL.

	Example for PostgreSQL:
	postgresql+psycopg2://user:password@host:5432/dbname
	"""
	database_url = os.getenv("DATABASE_URL", "").strip()
	if not database_url:
		raise RuntimeError(
			"DATABASE_URL environment variable is not set. Example: postgresql+psycopg2://user:password@host:5432/dbname"
		)
	return database_url


def get_target_table_name(default_table: str = "pdf_data") -> str:
	"""Fetch the target table name from environment variable TARGET_TABLE or use default."""
	return os.getenv("TARGET_TABLE", default_table)


def _create_engine(database_url: str) -> Engine:
	return create_engine(database_url, pool_pre_ping=True, future=True)


def load_dataframe_to_db(df: pd.DataFrame, database_url: str, table_name: str, *, chunksize: int = 1000) -> None:
	"""Append a DataFrame to an existing table.

	Parameters
	----------
	df: pd.DataFrame
		Transformed data to load.
	database_url: str
		SQLAlchemy database URL.
	table_name: str
		Name of the target table (must already exist).
	chunksize: int
		Chunk size for batched inserts.
	"""
	if df.empty:
		logger.warning("Received empty DataFrame. Nothing to load.")
		return

	engine: Optional[Engine] = None
	try:
		engine = _create_engine(database_url)
		logger.info("Loading %d rows into table '%s'", len(df), table_name)
		# Rely on the pre-existing table schema; append only
		df.to_sql(
			name=table_name,
			con=engine,
			if_exists="append",
			index=False,
			method="multi",
			chunksize=chunksize,
		)
		logger.info("Load completed successfully")
	except Exception as exc:
		logger.error("Failed to load data into database: %s", exc)
		raise
	finally:
		if engine is not None:
			engine.dispose() 