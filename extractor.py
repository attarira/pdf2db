"""PDF Table Extraction Module.

This module provides functionality to extract tabular data from PDF files
into a single Pandas DataFrame using Camelot (preferred) with a Tabula fallback.
"""
from __future__ import annotations

import logging
from typing import List

import pandas as pd


logger = logging.getLogger(__name__)


def _extract_with_camelot(pdf_path: str, flavor: str) -> List[pd.DataFrame]:
	"""Try extracting tables using Camelot with the given flavor.

	Parameters
	----------
	pdf_path: str
		Path to the PDF file.
	flavor: str
		Either "lattice" or "stream".

	Returns
	-------
	List[pd.DataFrame]
		List of DataFrames, one per detected table.
	"""
	try:
		import camelot  # type: ignore
	except Exception as exc:  # pragma: no cover - optional dependency
		logger.debug("Camelot import failed: %s", exc)
		return []

	try:
		logger.info("Extracting tables with Camelot (%s) from: %s", flavor, pdf_path)
		tables = camelot.read_pdf(pdf_path, pages="all", flavor=flavor)
		frames: List[pd.DataFrame] = [t.df for t in tables] if tables else []
		# Drop empty tables
		frames = [f for f in frames if not f.empty]
		return frames
	except Exception as exc:  # pragma: no cover - external dependency parsing
		logger.warning("Camelot (%s) extraction failed: %s", flavor, exc)
		return []


def _extract_with_tabula(pdf_path: str) -> List[pd.DataFrame]:
	"""Try extracting tables using Tabula.

	Parameters
	----------
	pdf_path: str
		Path to the PDF file.
	"""
	try:
		import tabula  # type: ignore
	except Exception as exc:  # pragma: no cover - optional dependency
		logger.debug("Tabula import failed: %s", exc)
		return []

	try:
		logger.info("Extracting tables with Tabula from: %s", pdf_path)
		frames: List[pd.DataFrame] = tabula.read_pdf(
			pdf_path,
			pages="all",
			multiple_tables=True,
			guess=True,
			headers=None,
		)
		frames = [f for f in frames if isinstance(f, pd.DataFrame) and not f.empty]
		return frames
	except Exception as exc:  # pragma: no cover - external dependency parsing
		logger.error("Tabula extraction failed: %s", exc)
		return []


def extract_tables(pdf_path: str) -> pd.DataFrame:
	"""Extract all tables from a PDF into a single DataFrame.

	This function tries Camelot (lattice, then stream), and falls back to Tabula.
	If multiple tables are found, they are concatenated vertically.

	Parameters
	----------
	pdf_path: str
		Path to the input PDF file.

	Returns
	-------
	pd.DataFrame
		Concatenated DataFrame of all extracted tables. Raises an error if nothing is extracted.
	"""
	all_frames: List[pd.DataFrame] = []

	# Prefer Camelot lattice for well-delineated tables with cell borders
	all_frames.extend(_extract_with_camelot(pdf_path, flavor="lattice"))

	# Try Camelot stream if lattice found nothing
	if not all_frames:
		all_frames.extend(_extract_with_camelot(pdf_path, flavor="stream"))

	# Fall back to Tabula if Camelot yielded nothing
	if not all_frames:
		all_frames.extend(_extract_with_tabula(pdf_path))

	if not all_frames:
		raise RuntimeError(
			"No tables could be extracted from the PDF. Ensure the PDF has structured tables and that Camelot/Tabula system dependencies are installed."
		)

	# Reset indices and ensure consistent columns count by padding shorter frames if needed
	max_cols = max(frame.shape[1] for frame in all_frames)
	normalized_frames: List[pd.DataFrame] = []
	for frame in all_frames:
		if frame.shape[1] < max_cols:
			# Pad missing columns with None to allow concat
			for _ in range(max_cols - frame.shape[1]):
				frame[frame.shape[1]] = None
			frame = frame.reindex(sorted(frame.columns), axis=1)
		normalized_frames.append(frame.reset_index(drop=True))

	combined = pd.concat(normalized_frames, ignore_index=True)
	logger.info("Extracted %d rows across %d tables", len(combined), len(all_frames))
	return combined 