-- Empty comments contribute no textual review activity. No fabricated sentiment scores.
WITH typed AS (
    SELECT try_cast(id AS BIGINT) id, try_cast(listing_id AS BIGINT) listing_id,
           try_cast(reviewer_id AS BIGINT) reviewer_id, try_cast(date AS DATE) date,
           trim(comments) AS "comments",
           row_number() OVER (PARTITION BY id ORDER BY sha256(to_json(input))) ordinal
    FROM input
)
SELECT ?::VARCHAR city, ?::DATE snapshot_date, ?::VARCHAR currency_code, ?::VARCHAR _lineage,
       * EXCLUDE(ordinal), length(comments)::BIGINT comment_length
FROM typed WHERE ordinal = 1 AND coalesce(comments, '') <> '' ORDER BY id
