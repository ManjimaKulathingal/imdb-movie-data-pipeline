
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
import os

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def configured_path(variable_name, default_path):
    value = os.getenv(variable_name, default_path)
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


BASICS_FILE = configured_path(
    "BASICS_FILE", "data/raw/title.basics.tsv.gz"
)
RATINGS_FILE = configured_path(
    "RATINGS_FILE", "data/raw/title.ratings.tsv.gz"
)
OUTPUT_FILE = configured_path(
    "OUTPUT_FILE", "data/processed/movies.parquet"
)

try:
    CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "100000"))
except ValueError as exc:
    raise ValueError("CHUNK_SIZE must be a positive integer") from exc

if CHUNK_SIZE <= 0:
    raise ValueError("CHUNK_SIZE must be a positive integer")
OUTPUT_DIR = OUTPUT_FILE.parent


NUMERIC_COLUMNS = [
    "isAdult",
    "startYear",
    "endYear",
    "runtimeMinutes",
    "averageRating",
    "numVotes",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def main():
    logging.info("Starting full IMDb ingestion")

    # Validate both input files before starting ingestion.
    for input_file in [BASICS_FILE, RATINGS_FILE]:
        if not input_file.is_file():
            raise FileNotFoundError(
                f"Required input file not found: {input_file}"
            )

        logging.info("Found input file: %s", input_file)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Read ratings in chunks, then combine the smaller ratings dataset.
    ratings_chunks = []

    for chunk in pd.read_csv(
        RATINGS_FILE,
        sep="\t",
        compression="gzip",
        chunksize=CHUNK_SIZE,
        na_values=r"\N",
    ):
        ratings_chunks.append(chunk)

    ratings = pd.concat(ratings_chunks, ignore_index=True)

    ratings["averageRating"] = pd.to_numeric(
        ratings["averageRating"], errors="coerce"
    )
    ratings["numVotes"] = pd.to_numeric(
        ratings["numVotes"], errors="coerce"
    )

    invalid_ratings = ratings["averageRating"].isna().sum()
    invalid_votes = ratings["numVotes"].isna().sum()

    if invalid_ratings > 0:
        logging.warning(
            "Found %s missing or invalid average ratings",
            invalid_ratings,
        )

    if invalid_votes > 0:
        logging.warning(
            "Found %s missing or invalid vote counts",
            invalid_votes,
        )


    if ratings["tconst"].duplicated().any():
        raise ValueError("Duplicate IDs found in ratings data")

    logging.info("Loaded %s rating records", len(ratings))

    total_titles = 0
    total_movies = 0
    writer = None

    try:
        for chunk_number, basics in enumerate(
            pd.read_csv(
                BASICS_FILE,
                sep="\t",
                compression="gzip",
                na_values=r"\N",
                chunksize=CHUNK_SIZE,
                low_memory=False,
            ),
            start=1,
        ):
            total_titles += len(basics)

            required_columns = {
                "tconst",
                "titleType",
                "primaryTitle",
                "isAdult",
                "startYear",
                "endYear",
                "runtimeMinutes",
            }

            missing_columns = required_columns - set(basics.columns)

            if missing_columns:
                raise ValueError(
                    f"IMDb basics file is missing required columns: "
                    f"{sorted(missing_columns)}"
                )


            # Normalize numeric fields to keep Parquet schema consistent.
            for column in [
                "isAdult",
                "startYear",
                "endYear",
                "runtimeMinutes",
            ]:
                basics[column] = pd.to_numeric(
                    basics[column], errors="coerce"
                )

            movies = basics.loc[
                basics["titleType"] == "movie"
            ].copy()

            if movies.empty:
                continue

            merged = movies.merge(
                ratings,
                on="tconst",
                how="left",
                validate="one_to_one",
            )

            for column in NUMERIC_COLUMNS:
                merged[column] = pd.to_numeric(
                    merged[column], errors="coerce"
                ).astype("float64")

            # Data-quality checks for this chunk.
            assert merged["tconst"].notna().all()
            assert merged["tconst"].is_unique

            valid_ratings = merged["averageRating"].dropna()
            assert valid_ratings.between(0, 10).all()

            valid_votes = merged["numVotes"].dropna()
            assert (valid_votes >= 0).all()

            table = pa.Table.from_pandas(
                merged,
                preserve_index=False,
            )

            if writer is None:
                writer = pq.ParquetWriter(
                    OUTPUT_FILE,
                    table.schema,
                    compression="snappy",
                )

            writer.write_table(table)
            total_movies += len(merged)

            logging.info(
                "Processed chunk %s | movies written so far: %s",
                chunk_number,
                total_movies,
            )

    finally:
        if writer is not None:
            writer.close()

    if total_movies == 0:
        raise RuntimeError("No movie records were found")

    logging.info("Total title records read: %s", total_titles)
    logging.info("Total movie records written: %s", total_movies)
    logging.info("Final output: %s", OUTPUT_FILE)
    logging.info("Full IMDb ingestion completed")


if __name__ == "__main__":
    main()
