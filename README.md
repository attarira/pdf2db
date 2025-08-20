# pdf2db

Python-based ETL pipeline to extract tabular data from PDFs and load it into a relational database (e.g., PostgreSQL).

## Features

- Extract tables from PDFs using Camelot (preferred) with Tabula fallback.
- Clean and standardize column headers.
- Convert key columns to proper types:
  - `row_number` → INT
  - `as_of_date` → DATE (format: %Y%m%d)
  - `customer_code` → BIGINT
  - `date_of_restructure` → DATE (format: %Y%m%d)
- Load results into an existing database table using SQLAlchemy.
- Simple CLI entry point.

## Project Structure

```
pdf2db/
├── main.py
├── extractor.py
├── transformer.py
├── loader.py
├── requirements.txt
└── README.md
```

## Requirements

- Python 3.10+
- System dependencies for PDF parsing (choose one):
  - Camelot: Ghostscript and TK/Qt (Camelot with OpenCV via `camelot-py[cv]`)
  - Tabula: Java (JRE 8+)
- A PostgreSQL (or other SQLAlchemy-supported) database. The target table must already exist.

## Installation

1. Create and activate a virtual environment (recommended):

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install Python dependencies:

```bash
pip install -r requirements.txt
```

3. Install system dependencies for your chosen extractor:

- Camelot (macOS with Homebrew):

```bash
brew install ghostscript tesseract
```

- Tabula (if preferred/fallback):

```bash
brew install --cask temurin
```

## Configuration

Set environment variables:

- `DATABASE_URL` — SQLAlchemy URL for your database, e.g.:
  - `postgresql+psycopg2://user:password@localhost:5432/yourdb`
- `TARGET_TABLE` — Existing table name to append to (default: `pdf_data`).

Example for bash/zsh:

```bash
export DATABASE_URL="postgresql+psycopg2://user:password@localhost:5432/yourdb"
export TARGET_TABLE="pdf_data"
```

## Usage

Run the pipeline on a PDF:

```bash
python main.py --pdf sample.pdf
```

The script will:

1. Extract tables from all pages of the PDF
2. Clean and transform data
3. Append rows into the target database table

## Notes

- If extraction fails with Camelot, the script will attempt Tabula automatically.
- Ensure the target table has appropriate column names and types to match your data.
- Column names are standardized by lowercasing and replacing spaces/dashes/slashes with underscores.

## Tech Stack

- Python, Pandas
- Camelot / Tabula-py
- SQLAlchemy, PostgreSQL (via psycopg2)

## License

MIT
