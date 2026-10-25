-- One listing per source snapshot. Ties resolve by a stable hash of the source row.
WITH typed AS (
    SELECT try_cast(id AS BIGINT) id, try_cast(host_id AS BIGINT) host_id,
        coalesce(nullif(trim(name), ''), '') AS "name",
        coalesce(nullif(trim(neighbourhood_cleansed), ''), 'unknown') neighbourhood,
        CASE WHEN lower(room_type) LIKE '%entire%' THEN 'entire_home'
             WHEN lower(room_type) LIKE '%private%' THEN 'private'
             WHEN lower(room_type) LIKE '%shared%' THEN 'shared'
             WHEN lower(room_type) LIKE '%hotel%' THEN 'hotel'
             ELSE coalesce(nullif(lower(trim(room_type)), ''), 'unknown') END room_type,
        CASE WHEN money(price) >= 0 THEN money(price) END asking_price,
        CASE WHEN try_cast(review_scores_rating AS DECIMAL(8,4)) BETWEEN 0 AND 5
             THEN try_cast(review_scores_rating AS DECIMAL(8,4)) END review_rating,
        try_cast(host_since AS DATE) host_since,
        try_cast(latitude AS DOUBLE) latitude, try_cast(longitude AS DOUBLE) longitude,
        try_cast(accommodates AS INTEGER) accommodates,
        try_cast(bedrooms AS DECIMAL(8,2)) bedrooms, try_cast(beds AS INTEGER) beds,
        try_cast(minimum_nights AS INTEGER) minimum_nights,
        try_cast(maximum_nights AS INTEGER) maximum_nights,
        try_cast(number_of_reviews AS BIGINT) number_of_reviews,
        flag(instant_bookable) instant_bookable,
        row_number() OVER (PARTITION BY id ORDER BY try_cast(last_scraped AS DATE) DESC NULLS LAST, sha256(to_json(input))) AS ordinal
    FROM input
), unique_rows AS (SELECT * EXCLUDE(ordinal) FROM typed WHERE ordinal = 1),
boundaries AS (
    SELECT quantile_cont(asking_price, [0.2,0.4,0.6,0.8]) q FROM unique_rows
)
SELECT ?::VARCHAR city, ?::DATE snapshot_date, ?::VARCHAR currency_code, ?::VARCHAR _lineage,
       unique_rows.*,
       CASE WHEN asking_price IS NULL THEN NULL
            WHEN asking_price <= q[1] THEN 1 WHEN asking_price <= q[2] THEN 2
            WHEN asking_price <= q[3] THEN 3 WHEN asking_price <= q[4] THEN 4
            ELSE 5 END::SMALLINT price_bucket_5
FROM unique_rows CROSS JOIN boundaries ORDER BY id
