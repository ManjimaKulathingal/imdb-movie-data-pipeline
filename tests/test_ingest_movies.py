
from pathlib import Path

import pandas as pd
import pytest

import src.ingest_movies as ingest_movies
from src.ingest_movies import (
    get_chunk_size,
    identify_new_movies,
    load_existing_movie_ids,
    validate_ratings,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_FILE = PROJECT_ROOT / "data" / "processed" / "movies.parquet"


# --------------------------------------------------
# Test helpers
# --------------------------------------------------

def sample_output_movies():
    """Create a valid output-shaped movie DataFrame."""
    return pd.DataFrame({
        "tconst": ["tt0001"],
        "titleType": ["movie"],
        "primaryTitle": ["Existing Movie"],
        "originalTitle": ["Existing Movie"],
        "isAdult": [0.0],
        "startYear": [2020.0],
        "endYear": [float("nan")],
        "runtimeMinutes": [100.0],
        "genres": ["Drama"],
        "averageRating": [8.0],
        "numVotes": [100.0],
    })


def write_input_files(tmp_path, basics_rows, ratings_rows):
    """Write small compressed IMDb-style input files."""
    basics_file = tmp_path / "basics.tsv.gz"
    ratings_file = tmp_path / "ratings.tsv.gz"

    basics_columns = [
        "tconst",
        "titleType",
        "primaryTitle",
        "originalTitle",
        "isAdult",
        "startYear",
        "endYear",
        "runtimeMinutes",
        "genres",
    ]

    basics = pd.DataFrame(
        basics_rows,
        columns=basics_columns,
    )
    ratings = pd.DataFrame(
        ratings_rows,
        columns=["tconst", "averageRating", "numVotes"],
    )

    basics.to_csv(
        basics_file,
        sep="\t",
        index=False,
        compression="gzip",
    )
    ratings.to_csv(
        ratings_file,
        sep="\t",
        index=False,
        compression="gzip",
    )

    return basics_file, ratings_file


def configure_test_pipeline(
    monkeypatch,
    tmp_path,
    basics_file,
    ratings_file,
    output_file,
    chunk_size=1,
):
    """Point the real pipeline at temporary test files."""
    monkeypatch.setattr(ingest_movies, "BASICS_FILE", basics_file)
    monkeypatch.setattr(ingest_movies, "RATINGS_FILE", ratings_file)
    monkeypatch.setattr(ingest_movies, "OUTPUT_FILE", output_file)
    monkeypatch.setattr(
        ingest_movies,
        "OUTPUT_DIR",
        output_file.parent,
    )
    monkeypatch.setattr(
        ingest_movies,
        "TEMP_OUTPUT_FILE",
        output_file.with_suffix(".tmp.parquet"),
    )
    monkeypatch.setattr(ingest_movies, "CHUNK_SIZE", chunk_size)


# --------------------------------------------------
# Existing output tests
# --------------------------------------------------

@pytest.fixture(scope="module")
def movies():
    assert OUTPUT_FILE.exists(), "Run the ingestion pipeline first"
    return pd.read_parquet(OUTPUT_FILE)


def test_output_is_not_empty(movies):
    assert len(movies) > 0


def test_required_columns_exist(movies):
    required = {
        "tconst",
        "primaryTitle",
        "titleType",
        "averageRating",
        "numVotes",
    }
    assert required.issubset(movies.columns)


def test_movie_ids_are_present_and_unique(movies):
    assert movies["tconst"].notna().all()
    assert movies["tconst"].is_unique


def test_output_contains_movies_only(movies):
    assert (movies["titleType"] == "movie").all()


def test_ratings_are_valid(movies):
    ratings = movies["averageRating"].dropna()
    assert ratings.between(0, 10).all()


def test_vote_counts_are_non_negative(movies):
    votes = movies["numVotes"].dropna()
    assert (votes >= 0).all()


# --------------------------------------------------
# Rating validation tests
# --------------------------------------------------

def test_rejects_rating_above_10():
    ratings = pd.DataFrame({
        "tconst": ["tt001"],
        "averageRating": [10.5],
        "numVotes": [100],
    })

    with pytest.raises(ValueError, match="0–10 range"):
        validate_ratings(ratings)


def test_rejects_rating_below_0():
    ratings = pd.DataFrame({
        "tconst": ["tt001"],
        "averageRating": [-0.5],
        "numVotes": [100],
    })

    with pytest.raises(ValueError, match="0–10 range"):
        validate_ratings(ratings)


def test_rejects_negative_vote_counts():
    ratings = pd.DataFrame({
        "tconst": ["tt001"],
        "averageRating": [8.5],
        "numVotes": [-1],
    })

    with pytest.raises(ValueError, match="negative vote counts"):
        validate_ratings(ratings)


def test_accepts_valid_ratings():
    ratings = pd.DataFrame({
        "tconst": ["tt001", "tt002", "tt003"],
        "averageRating": [0, 7.5, 10],
        "numVotes": [0, 100, 500],
    })

    validate_ratings(ratings)


def test_accepts_missing_ratings_and_votes():
    ratings = pd.DataFrame({
        "tconst": ["tt001", "tt002"],
        "averageRating": [None, 8.0],
        "numVotes": [100, None],
    })

    validate_ratings(ratings)


def test_rejects_duplicate_rating_ids():
    ratings = pd.DataFrame({
        "tconst": ["tt001", "tt001"],
        "averageRating": [8.0, 7.5],
        "numVotes": [100, 200],
    })

    with pytest.raises(
        ValueError,
        match="Duplicate IDs found in ratings data",
    ):
        validate_ratings(ratings)


def test_rejects_missing_rating_ids():
    ratings = pd.DataFrame({
        "tconst": ["tt001", None],
        "averageRating": [8.0, 7.5],
        "numVotes": [100, 200],
    })

    with pytest.raises(
        ValueError,
        match="Missing IDs found in ratings data",
    ):
        validate_ratings(ratings)


def test_accepts_unique_rating_ids():
    ratings = pd.DataFrame({
        "tconst": ["tt001", "tt002"],
        "averageRating": [8.0, 7.5],
        "numVotes": [100, 200],
    })

    validate_ratings(ratings)


def test_rejects_missing_required_rating_columns():
    ratings = pd.DataFrame({
        "tconst": ["tt001"],
        "averageRating": [8.0],
    })

    with pytest.raises(ValueError, match="missing required columns"):
        validate_ratings(ratings)


# --------------------------------------------------
# Chunk-size configuration tests
# --------------------------------------------------

def test_rejects_zero_chunk_size():
    with pytest.raises(
        ValueError,
        match="CHUNK_SIZE must be a positive integer",
    ):
        get_chunk_size("0")


def test_rejects_non_numeric_chunk_size():
    with pytest.raises(
        ValueError,
        match="CHUNK_SIZE must be a positive integer",
    ):
        get_chunk_size("abc")


def test_accepts_valid_chunk_size():
    assert get_chunk_size("100000") == 100000
    assert get_chunk_size("1") == 1


def test_rejects_negative_chunk_size():
    with pytest.raises(
        ValueError,
        match="CHUNK_SIZE must be a positive integer",
    ):
        get_chunk_size("-10")


# --------------------------------------------------
# Incremental-processing helper tests
# --------------------------------------------------

def test_identifies_only_new_movies():
    movies = pd.DataFrame({
        "tconst": ["tt001", "tt002", "tt003"],
        "primaryTitle": ["Movie A", "Movie B", "Movie C"],
    })

    result = identify_new_movies(movies, {"tt001", "tt003"})

    assert result["tconst"].tolist() == ["tt002"]
    assert result["primaryTitle"].tolist() == ["Movie B"]


def test_all_movies_are_new():
    movies = pd.DataFrame({
        "tconst": ["tt001", "tt002"],
        "primaryTitle": ["Movie A", "Movie B"],
    })

    result = identify_new_movies(movies, set())

    assert result["tconst"].tolist() == ["tt001", "tt002"]


def test_no_movies_are_new():
    movies = pd.DataFrame({
        "tconst": ["tt001", "tt002"],
        "primaryTitle": ["Movie A", "Movie B"],
    })

    result = identify_new_movies(movies, {"tt001", "tt002"})

    assert result.empty
    assert list(result.columns) == list(movies.columns)


# --------------------------------------------------
# Existing Parquet ID loader tests
# --------------------------------------------------

def test_loads_existing_movie_ids(tmp_path):
    output_file = tmp_path / "movies.parquet"

    pd.DataFrame({
        "tconst": ["tt001", "tt002", "tt003"],
        "primaryTitle": ["Movie A", "Movie B", "Movie C"],
    }).to_parquet(output_file, index=False)

    assert load_existing_movie_ids(output_file) == {
        "tt001", "tt002", "tt003"
    }


def test_returns_empty_set_when_output_does_not_exist(tmp_path):
    assert load_existing_movie_ids(
        tmp_path / "missing.parquet"
    ) == set()


# --------------------------------------------------
# Integration tests: actual incremental ingestion
# --------------------------------------------------

def test_incremental_ingestion_preserves_old_and_adds_new(
    tmp_path,
    monkeypatch,
):
    output_file = tmp_path / "movies.parquet"
    temp_output_file = output_file.with_suffix(".tmp.parquet")

    sample_output_movies().to_parquet(
        output_file,
        index=False,
    )

    basics_file, ratings_file = write_input_files(
        tmp_path,
        basics_rows=[
            [
                "tt0001", "movie", "Existing Movie", "Existing Movie",
                "0", "2020", "\\N", "100", "Drama",
            ],
            [
                "tt0002", "movie", "New Movie", "New Movie",
                "0", "2022", "\\N", "95", "Comedy",
            ],
        ],
        ratings_rows=[
            ["tt0001", 8.0, 100],
            ["tt0002", 7.5, 50],
        ],
    )

    configure_test_pipeline(
        monkeypatch,
        tmp_path,
        basics_file,
        ratings_file,
        output_file,
        chunk_size=1,
    )

    ingest_movies.main()

    result = pd.read_parquet(output_file)

    assert set(result["tconst"]) == {"tt0001", "tt0002"}
    assert result["tconst"].is_unique
    assert len(result) == 2
    assert result.loc[
        result["tconst"] == "tt0001", "primaryTitle"
    ].iloc[0] == "Existing Movie"
    assert result.loc[
        result["tconst"] == "tt0002", "primaryTitle"
    ].iloc[0] == "New Movie"
    assert not temp_output_file.exists()


def test_failed_ingestion_preserves_existing_output(
    tmp_path,
    monkeypatch,
):
    output_file = tmp_path / "movies.parquet"

    # Use a real Parquet file, not arbitrary bytes.
    sample_output_movies().to_parquet(
        output_file,
        index=False,
    )
    original_bytes = output_file.read_bytes()

    basics_file, ratings_file = write_input_files(
        tmp_path,
        basics_rows=[
            [
                "tt0002", "movie", "Valid New Movie", "Valid New Movie",
                "0", "2021", "\\N", "100", "Drama",
            ],
            [
                None, "movie", "Invalid Movie", "Invalid Movie",
                "0", "2022", "\\N", "90", "Comedy",
            ],
        ],
        ratings_rows=[
            ["tt0002", 8.0, 100],
        ],
    )

    configure_test_pipeline(
        monkeypatch,
        tmp_path,
        basics_file,
        ratings_file,
        output_file,
        chunk_size=1,
    )

    temp_output_file = output_file.with_suffix(".tmp.parquet")

    with pytest.raises(ValueError, match="missing IDs"):
        ingest_movies.main()

    assert output_file.read_bytes() == original_bytes
    assert not temp_output_file.exists()
