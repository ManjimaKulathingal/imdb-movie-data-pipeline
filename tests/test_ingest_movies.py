from src.ingest_movies import validate_ratings
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_FILE = PROJECT_ROOT / "data" / "processed" / "movies.parquet"


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

def test_rejects_rating_above_10():
    ratings = pd.DataFrame({
        "averageRating": [10.5],
        "numVotes": [100],
    })

    with pytest.raises(
        ValueError,
        match="outside the 0–10 range",
    ):
        validate_ratings(ratings)


def test_rejects_negative_vote_counts():
    ratings = pd.DataFrame({
        "averageRating": [8.5],
        "numVotes": [-1],
    })

    with pytest.raises(
        ValueError,
        match="negative vote counts",
    ):
        validate_ratings(ratings)

def test_accepts_valid_ratings():
    ratings = pd.DataFrame({
        "averageRating": [0, 7.5, 10],
        "numVotes": [0, 100, 500],
    })

    # Valid values should not raise an exception.
    validate_ratings(ratings)

def test_accepts_missing_ratings_and_votes():
    ratings = pd.DataFrame({
        "averageRating": [None, 8.0],
        "numVotes": [100, None],
    })

    validate_ratings(ratings)

