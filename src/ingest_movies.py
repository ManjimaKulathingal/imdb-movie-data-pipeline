
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"

BASICS_FILE = RAW_DIR / "title.basics.tsv.gz"
RATINGS_FILE = RAW_DIR / "title.ratings.tsv.gz"
OUTPUT_FILE = OUTPUT_DIR / "movies.parquet"
CHUNK_SIZE = 100_000

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
