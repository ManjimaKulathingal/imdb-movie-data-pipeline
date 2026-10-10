
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
