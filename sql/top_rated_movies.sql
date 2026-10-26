SELECT
    primaryTitle,
    startYear,
    averageRating,
    numVotes
FROM imdb_db.movies
WHERE averageRating IS NOT NULL
  AND numVotes >= 10000
ORDER BY averageRating DESC, numVotes DESC
LIMIT 10;