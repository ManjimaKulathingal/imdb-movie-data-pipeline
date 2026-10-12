
import logging
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

BASICS_FILE = Path(
    os.getenv("BASICS_FILE", "data/raw/title.basics.tsv.gz")
)
RATINGS_FILE = Path(
    os.getenv("RATINGS_FILE", "data/raw/title.ratings.tsv.gz")
)
OUTPUT_FILE = Path(
    os.getenv("OUTPUT_FILE", "data/processed/movies.parquet")
)

if not BASICS_FILE.is_absolute():
    BASICS_FILE = PROJECT_ROOT / BASICS_FILE

if not RATINGS_FILE.is_absolute():
    RATINGS_FILE = PROJECT_ROOT / RATINGS_FILE

if not OUTPUT_FILE.is_absolute():
    OUTPUT_FILE = PROJECT_ROOT / OUTPUT_FILE

OUTPUT_DIR = OUTPUT_FILE.parent
CHUNK_SIZE = os.getenv("CHUNK_SIZE", "100000")
TEMP_OUTPUT_FILE = OUTPUT_FILE.with_suffix(".tmp.parquet")


# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# --------------------------------------------------
# Required columns and output schema
# --------------------------------------------------

BASICS_REQUIRED_COLUMNS = {
    "tconst",
    "titleType",
    "primaryTitle",
    "originalTitle",
    "isAdult",
    "startYear",
    "endYear",
    "runtimeMinutes",
    "genres",
}

RATINGS_REQUIRED_COLUMNS = {
    "tconst",
    "averageRating",
    "numVotes",
}

OUTPUT_COLUMNS = [
    "tconst",
    "titleType",
    "primaryTitle",
    "originalTitle",
    "isAdult",
    "startYear",
    "endYear",
    "runtimeMinutes",
    "genres",
    "averageRating",
    "numVotes",
]


# --------------------------------------------------
# Configuration helpers
# --------------------------------------------------

def get_chunk_size(value=None):
    """Return a valid positive integer chunk size."""
    if value is None:
        value = CHUNK_SIZE

    try:
        chunk_size = int(value)
    except (TypeError, ValueError):
        raise ValueError(
            "CHUNK_SIZE must be a positive integer"
        ) from None

    if chunk_size <= 0:
        raise ValueError(
            "CHUNK_SIZE must be a positive integer"
        )

    return chunk_size


# --------------------------------------------------
# Validation helpers
# --------------------------------------------------

def validate_required_columns(
    dataframe,
    required_columns,
    dataset_name,
):
    """Check that a dataset contains every required column."""
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(
            f"{dataset_name} is missing required columns: {missing}"
        )


def validate_ratings(ratings):
    """Validate required columns, IDs, ratings, and vote counts."""
    validate_required_columns(
        ratings,
        RATINGS_REQUIRED_COLUMNS,
        "Ratings data",
    )

    if ratings["tconst"].isna().any():
        raise ValueError(
            "Missing IDs found in ratings data"
        )

    if ratings["tconst"].duplicated().any():
        raise ValueError(
            "Duplicate IDs found in ratings data"
        )

    ratings = ratings.copy()

    ratings["averageRating"] = pd.to_numeric(
        ratings["averageRating"],
        errors="coerce",
    )

    ratings["numVotes"] = pd.to_numeric(
        ratings["numVotes"],
        errors="coerce",
    )

    valid_ratings = ratings["averageRating"].dropna()

    if not valid_ratings.between(0, 10).all():
        raise ValueError(
            "Ratings must be within the 0–10 range"
        )

    valid_votes = ratings["numVotes"].dropna()

    if (valid_votes < 0).any():
        raise ValueError(
            "Ratings data contains negative vote counts"
        )

    return ratings


def validate_movie_output(movies):
    """Validate the final output's columns and movie IDs."""
    missing_columns = set(OUTPUT_COLUMNS) - set(movies.columns)

    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(
            f"Movie output is missing required columns: {missing}"
        )

    if movies["tconst"].isna().any():
        raise ValueError("Movie output contains missing IDs")

    if movies["tconst"].duplicated().any():
        raise ValueError("Duplicate movie IDs found in output")

    return movies


# --------------------------------------------------
# Incremental ingestion helpers
# --------------------------------------------------

def identify_new_movies(movies, existing_ids):
    """Return movies that are not already in the output."""
    return movies.loc[
        ~movies["tconst"].isin(existing_ids)
    ].copy()


def load_existing_movie_ids(output_file):
    """Load movie IDs from an existing Parquet file."""
    output_file = Path(output_file)

    if not output_file.exists():
        return set()

    ids = pd.read_parquet(
        output_file,
        columns=["tconst"],
    )["tconst"]

    return set(ids.dropna().astype(str))


# --------------------------------------------------
# Data conversion helpers
# --------------------------------------------------

def convert_basics_columns(basics):
    """Convert IMDb missing values and numeric columns."""
    basics = basics.copy()

    basics = basics.replace(
        r"^\s*\\N\s*$",
        pd.NA,
        regex=True,
    )

    for column in [
        "isAdult",
        "startYear",
        "endYear",
        "runtimeMinutes",
    ]:
        basics[column] = pd.to_numeric(
            basics[column],
            errors="coerce",
        ).astype("Int64")

    return basics


def prepare_movie_records(movies, ratings):
    """Join movie basics with rating information."""
    movies = movies.copy()
    ratings = ratings.copy()

    movies["tconst"] = movies["tconst"].astype("string")
    ratings["tconst"] = ratings["tconst"].astype("string")

    merged = movies.merge(
        ratings[["tconst", "averageRating", "numVotes"]],
        on="tconst",
        how="left",
        validate="one_to_one",
    )

    return merged[OUTPUT_COLUMNS]


# --------------------------------------------------
# Main ingestion pipeline
# --------------------------------------------------

def main():
    """Run incremental IMDb ingestion without damaging old output."""
    writer = None
    temporary_file_created = False

    try:
        chunk_size = get_chunk_size()

        if not BASICS_FILE.exists():
            raise FileNotFoundError(
                f"IMDb basics file not found: {BASICS_FILE}"
            )

        if not RATINGS_FILE.exists():
            raise FileNotFoundError(
                f"IMDb ratings file not found: {RATINGS_FILE}"
            )

        OUTPUT_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        # Use the current output path, including paths configured in tests.
        temp_file = TEMP_OUTPUT_FILE

        # Remove an old temporary file before starting.
        if temp_file.exists():
            temp_file.unlink()

        logger.info("Starting IMDb movie ingestion")
        logger.info("Basics file: %s", BASICS_FILE)
        logger.info("Ratings file: %s", RATINGS_FILE)
        logger.info("Output file: %s", OUTPUT_FILE)
        logger.info("Chunk size: %s", chunk_size)

        # ------------------------------------------
        # 1. Load and validate ratings
        # ------------------------------------------

        ratings_chunks = []

        for chunk in pd.read_csv(
            RATINGS_FILE,
            sep="\t",
            na_values=["\\N"],
            dtype={"tconst": "string"},
            chunksize=chunk_size,
        ):
            validate_required_columns(
                chunk,
                RATINGS_REQUIRED_COLUMNS,
                "Ratings data",
            )
            ratings_chunks.append(chunk)

        if not ratings_chunks:
            raise ValueError("Ratings dataset is empty")

        ratings = pd.concat(
            ratings_chunks,
            ignore_index=True,
        )
        ratings = validate_ratings(ratings)

        logger.info("Loaded %s rating records", len(ratings))

        # ------------------------------------------
        # 2. Load existing IDs
        # ------------------------------------------

        existing_ids = load_existing_movie_ids(OUTPUT_FILE)

        logger.info(
            "Found %s existing movie IDs",
            len(existing_ids),
        )

        # ------------------------------------------
        # 3. Copy existing output into a temp file
        # ------------------------------------------

        output_schema = None

        if OUTPUT_FILE.exists():
            existing_table = pq.read_table(OUTPUT_FILE)

            validate_movie_output(
                existing_table.to_pandas()
            )

            output_schema = existing_table.schema

            writer = pq.ParquetWriter(
                temp_file,
                output_schema,
                compression="snappy",
            )
            temporary_file_created = True
            writer.write_table(existing_table)

            logger.info("Copied existing output to temporary file")

        # ------------------------------------------
        # 4. Process basics in chunks
        # ------------------------------------------

        total_rows_read = 0
        total_movies_found = 0
        total_new_movies = 0
        total_duplicates_skipped = 0

        for basics_chunk in pd.read_csv(
            BASICS_FILE,
            sep="\t",
            na_values=["\\N"],
            dtype={"tconst": "string"},
            chunksize=chunk_size,
        ):
            validate_required_columns(
                basics_chunk,
                BASICS_REQUIRED_COLUMNS,
                "Basics data",
            )

            total_rows_read += len(basics_chunk)

            basics_chunk = convert_basics_columns(
                basics_chunk
            )

            movies = basics_chunk.loc[
                basics_chunk["titleType"] == "movie"
            ].copy()

            if movies.empty:
                continue

            # Validate IDs before merging or writing anything further.
            if movies["tconst"].isna().any():
                raise ValueError(
                    "Movie records contain missing IDs"
                )

            if movies["tconst"].duplicated().any():
                raise ValueError(
                    "Duplicate movie IDs found in basics data"
                )

            total_movies_found += len(movies)

            new_movies = identify_new_movies(
                movies,
                existing_ids,
            )

            total_duplicates_skipped += (
                len(movies) - len(new_movies)
            )

            if new_movies.empty:
                continue

            # Track IDs across chunks to avoid duplicate output records.
            existing_ids.update(
                new_movies["tconst"].astype(str).tolist()
            )

            merged_movies = prepare_movie_records(
                new_movies,
                ratings,
            )
            validate_movie_output(merged_movies)

            table = pa.Table.from_pandas(
                merged_movies,
                preserve_index=False,
            )

            # The existing output defines the schema for incremental runs.
            if output_schema is None:
                output_schema = table.schema

                writer = pq.ParquetWriter(
                    temp_file,
                    output_schema,
                    compression="snappy",
                )
                temporary_file_created = True
            else:
                table = table.cast(
                    output_schema,
                    safe=False,
                )

            writer.write_table(table)
            total_new_movies += len(merged_movies)

            logger.info(
                "Read %s basics rows; added %s new movies so far",
                total_rows_read,
                total_new_movies,
            )

        # ------------------------------------------
        # 5. Close the writer and validate the result
        # ------------------------------------------

        if writer is not None:
            writer.close()
            writer = None

        elif not OUTPUT_FILE.exists():
            raise RuntimeError(
                "No movie records were found and no existing "
                "output file is available"
            )

        if not temp_file.exists():
            raise RuntimeError(
                "Temporary output file was not created"
            )

        final_check = pd.read_parquet(temp_file)
        validate_movie_output(final_check)

        # Replace the original only after successful validation.
        os.replace(temp_file, OUTPUT_FILE)
        temporary_file_created = False

        logger.info("IMDb ingestion completed successfully")
        logger.info("Basics rows read: %s", total_rows_read)
        logger.info("Movies found: %s", total_movies_found)
        logger.info("New movies added: %s", total_new_movies)
        logger.info(
            "Existing movies skipped: %s",
            total_duplicates_skipped,
        )
        logger.info("Final movie count: %s", len(final_check))
        logger.info("Output saved to: %s", OUTPUT_FILE)

    except Exception:
        logger.exception("IMDb ingestion failed")
        raise

    finally:
        # Close the writer before trying to remove its temporary file.
        if writer is not None:
            try:
                writer.close()
            except Exception:
                logger.exception(
                    "Failed to close temporary Parquet writer"
                )

        # Preserve the original output and remove partial temporary output.
        if temporary_file_created and TEMP_OUTPUT_FILE.exists():
            try:
                TEMP_OUTPUT_FILE.unlink()
                logger.info(
                    "Removed temporary output after failure"
                )
            except OSError:
                logger.exception(
                    "Could not remove temporary output file"
                )


if __name__ == "__main__":
    main()
