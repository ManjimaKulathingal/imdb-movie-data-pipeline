# IMDb Movie Data Ingestion Pipeline

A Python-based data engineering project that ingests public IMDb datasets, transforms movie metadata, combines ratings, validates data quality, and writes the results to Parquet.

The project demonstrates chunk-based processing, incremental ingestion, data validation, automated testing, and safe output replacement.

## Architecture

```mermaid
flowchart TD
    A[IMDb Public Datasets] --> B[Compressed TSV Files]
    B --> C[Read Title Metadata in Chunks]
    C --> D[Filter Movies and Transform Columns]
    E[IMDb Ratings Dataset] --> F[Load and Validate Ratings]
    D --> G[Join on tconst]
    F --> G
    G --> H[Validate Movie Records]
    H --> I[Skip Existing Movie IDs]
    I --> J[Write Temporary Parquet Output]
    J --> K[Validate Output]
    K --> L[Replace Final Parquet File]

## Technologies

- **Python 3.12** — pipeline implementation
- **Pandas** — reading, cleaning, transforming, and joining datasets
- **PyArrow** — Parquet storage and incremental writes
- **python-dotenv** — environment-based configuration
- **pytest** — automated testing
- **Git and GitHub** — version control

## Data Sources

The project uses IMDb's publicly available datasets:

| Dataset | Description |
|---|---|
| `title.basics.tsv.gz` | Title metadata, including title type, release year, runtime, and genres |
| `title.ratings.tsv.gz` | Average ratings and number of votes |

Source: https://datasets.imdbws.com/

IMDb datasets are subject to IMDb's usage and licensing terms.

## Pipeline Workflow

1. Read title metadata in chunks to reduce memory use during title processing.
2. Load and validate ratings data.
3. Filter the title metadata to include movies only.
4. Join movie records with ratings using `tconst`.
5. Standardize numeric columns and handle missing values.
6. Validate identifiers, required columns, rating ranges, and vote counts.
7. Identify movies already present in the output and skip them.
8. Write new records to a temporary Parquet file alongside existing records.
9. Validate the resulting dataset and replace the final output only after successful processing.

The pipeline uses a left join so that movies without available ratings can remain in the output.

**Incremental processing note:** title metadata is processed in chunks, but the current ratings implementation collects its chunks into memory. Therefore, the entire pipeline is not yet fully memory-streaming.

## Configuration

Pipeline settings are managed through a `.env` file in the project root.

Example:

```dotenv
BASICS_FILE=data/raw/title.basics.tsv.gz
RATINGS_FILE=data/raw/title.ratings.tsv.gz
OUTPUT_FILE=data/processed/movies.parquet
CHUNK_SIZE=100000
```

Paths are resolved relative to the project root. Adjust `CHUNK_SIZE` to change the number of title records processed per chunk.

Do not put passwords, access keys, or other secrets in files committed to Git.

## Data Quality and Reliability

The pipeline and automated tests cover:

- Required columns and missing movie identifiers
- Duplicate identifiers in ratings data
- Rating values between 0 and 10
- Non-negative vote counts
- Movie-only output and expected output columns
- Incremental ingestion that preserves existing movies
- Repeat runs without duplicate movie IDs
- Missing or unexpected columns in title metadata
- Safe handling of ingestion failures without damaging the previous output
- Positive integer validation for `CHUNK_SIZE`

The pipeline also logs processing metrics, including title rows read, movies found, new movies added, existing movies skipped, and final output row counts.

## Results

The following figures are from an earlier successful full ingestion run. Results can change when the IMDb source datasets are updated.

| Metric | Recorded result |
|---|---:|
| Title records read | 12,852,122 |
| Movie records written | 759,083 |
| Output format | Parquet |
| Compression | Snappy |
| Current automated tests | 29 passing |

**Output file:** `data/processed/movies.parquet`

Raw datasets and generated Parquet files are excluded from Git to keep the repository lightweight.

## Run Locally

### 1. Create and activate a virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

```powershell
python -m pip install -r requirements.txt
```

### 3. Download the IMDb datasets

Download `title.basics.tsv.gz` and `title.ratings.tsv.gz` from https://datasets.imdbws.com/ and place them in `data/raw/`.

### 4. Configure the pipeline

Create or update `.env` in the project root using the configuration example above.

### 5. Run ingestion

```powershell
python src/ingest_movies.py
```

### 6. Run automated tests

```powershell
python -m pytest -v
```

## Project Structure

```text
imdb-movie-pipeline/
├── data/
│   ├── raw/
│   └── processed/
├── src/
│   ├── ingest_movies.py
│   └── ingest_movies_sample.py
├── tests/
│   └── test_ingest_movies.py
├── .env
├── .gitignore
├── requirements.txt
└── README.md
```

The `.venv/` virtual environment is local and should not be committed.

## Project Status

### Completed

- Local IMDb data ingestion and transformation
- Chunk-based title processing
- Incremental Parquet output
- Data quality validation
- Temporary output and failure-safety handling
- Automated test suite — 29 passing tests
- Configuration through `.env`
- GitHub version control and documentation

### Planned

- AWS S3 integration, with cost checks before creating resources
- AWS Glue Data Catalog
- SQL analysis with Amazon Athena
- Airflow orchestration and cloud workflow integration
- Portfolio documentation and interview preparation

## Data and Repository Notes

This repository contains source code, tests, and documentation. Raw datasets, generated Parquet files, virtual environments, and credentials should not be committed to GitHub.
