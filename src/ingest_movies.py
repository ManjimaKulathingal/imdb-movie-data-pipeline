
import logging
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"

BASICS_FILE = RAW_DIR / "title.basics.tsv.gz"
RATINGS_FILE = RAW_DIR / "title.ratings.tsv.gz"
OUTPUT_FILE = OUTPUT_DIR / "movies.parquet"
CHUNK_SIZE = 100_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def main():
    logging.info("Starting full IMDb ingestion")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load ratings in chunks and combine them.
    # Ratings are much smaller than the title basics file.
    ratings_chunks = []

    for chunk in pd.read_csv(
        RATINGS_FILE,
        sep="\t",
        compression="gzip",
        chunksize=CHUNK_SIZE,
    ):
        ratings_chunks.append(chunk)

    ratings = pd.concat(ratings_chunks, ignore_index=True)
    logging.info("Loaded %s rating records", len(ratings))
    ratings["averageRating"] = pd.to_numeric(
        ratings["averageRating"], errors="coerce"
    )
    ratings["numVotes"] = pd.to_numeric(
        ratings["numVotes"], errors="coerce"
    )


    # Process title records in chunks.
    output_parts = []
    total_titles = 0
    total_movies = 0

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
        # Standardize numeric columns across all chunks
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

        assert merged["tconst"].notna().all()
        assert merged["tconst"].is_unique

        valid_ratings = merged["averageRating"].dropna()
        assert valid_ratings.between(0, 10).all()

        # Save each chunk separately to avoid holding
        # the entire final dataset in memory.
        part_file = OUTPUT_DIR / f"movies_part_{chunk_number:04d}.parquet"
        merged.to_parquet(part_file, index=False)
        output_parts.append(part_file)

        total_movies += len(merged)

        logging.info(
            "Processed chunk %s | movies: %s",
            chunk_number,
            len(merged),
        )

    # Combine the processed Parquet parts into one file.
    if not output_parts:
        raise RuntimeError("No movie records were found")

    parts = [
        pd.read_parquet(part_file)
        for part_file in output_parts
    ]
    final_df = pd.concat(parts, ignore_index=True)
    final_df.to_parquet(OUTPUT_FILE, index=False)

    # Remove intermediate parts after the final file is written.
    for part_file in output_parts:
        part_file.unlink()

    logging.info("Total title records read: %s", total_titles)
    logging.info("Total movie records written: %s", total_movies)
    logging.info("Final output: %s", OUTPUT_FILE)
    logging.info("Full IMDb ingestion completed")


if __name__ == "__main__":
    main()
