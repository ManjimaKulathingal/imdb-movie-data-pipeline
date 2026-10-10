\# IMDb Movie Data Ingestion Pipeline



\## Overview



A Python-based data engineering project that ingests IMDb public datasets, transforms movie metadata, joins ratings, validates data quality, and stores the processed output in Parquet format.



\## Architecture



IMDb TSV.GZ datasets → Python and Pandas → Data cleaning and transformation → Data quality checks → Parquet output



\## Technologies



\- Python 3.12

\- Pandas

\- PyArrow

\- pytest

\- Git



\## Data Sources



The project uses IMDb's publicly available non-commercial datasets:



\- `title.basics.tsv.gz` — movie titles and metadata

\- `title.ratings.tsv.gz` — average ratings and vote counts



Source: https://datasets.imdbws.com/



\## Processing Steps



1\. Read title metadata in chunks.

2\. Load IMDb ratings and join them to movie records using `tconst`.

3\. Filter records to movies.

4\. Standardize numeric columns.

5\. Validate movie IDs, rating ranges, and vote counts.

6\. Write the processed dataset to Parquet.



\## Output



`data/processed/movies.parquet`



The current processed dataset contains 759,083 movie records.



\## Automated Tests



Run the tests with:



```bash

python -m pytest tests/test\_ingest\_movies.py -v

```



The test suite validates output presence, required columns, unique identifiers, movie filtering, rating ranges, and non-negative vote counts.



\## Project Status



The local ingestion pipeline and six automated tests are working. AWS integration, cloud storage, orchestration, and deployment are planned future enhancements.



\## Data and Licensing



IMDb datasets are subject to IMDb's usage and licensing terms. This repository contains pipeline code and documentation, not the raw IMDb datasets or generated Parquet output.



