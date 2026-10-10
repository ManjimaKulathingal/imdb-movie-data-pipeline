
import logging
from pathlib import Path

import pandas as pd

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

BASICS_FILE = RAW_DIR / "title.basics.tsv.gz"
RATINGS_FILE = RAW_DIR / "title.ratings.tsv.gz"


def main():
    logging.info("Starting IMDb movie ingestion")

    # Read a small sample for our first test
    basics = pd.read_csv(
        BASICS_FILE,
        sep="\t",
        compression="gzip",
        na_values=r"\N",
        nrows=100_000,
        low_memory=False,
    )

    ratings = pd.read_csv(
        RATINGS_FILE,
        sep="\t",
        compression="gzip",
        nrows=100_000,
    )

    logging.info("Loaded %s title records", len(basics))
    logging.info("Loaded %s ratings records", len(ratings))

    # Keep movies only
    movies = basics.loc[
        basics["titleType"] == "movie"
    ].copy()

    # Join movie details with ratings
    merged = movies.merge(
        ratings,
        on="tconst",
        how="left",
        validate="one_to_one",
    )

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Save as Parquet
    output_file = OUTPUT_DIR / "movies_sample.parquet"
    merged.to_parquet(output_file, index=False)

    logging.info("Movies retained: %s", len(movies))
    logging.info(
        "Movies with ratings: %s",
        merged["averageRating"].notna().sum(),
    )
        # Data quality checks
    assert merged["tconst"].notna().all(), "Missing movie identifiers"
    assert merged["tconst"].is_unique, "Duplicate movie identifiers"

    valid_ratings = merged["averageRating"].dropna()
    assert valid_ratings.between(0, 10).all(), "Invalid ratings found"

    logging.info("Data quality checks passed")
    logging.info("Saved Parquet file: %s", output_file)
    logging.info("IMDb movie ingestion completed")


if __name__ == "__main__":
    main()
