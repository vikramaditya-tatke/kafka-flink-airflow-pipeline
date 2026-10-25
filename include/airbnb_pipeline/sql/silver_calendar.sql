WITH typed AS (
    SELECT try_cast(listing_id AS BIGINT) listing_id, try_cast(date AS DATE) date,
           flag(available) available,
           CASE WHEN money(price) >= 0 THEN money(price) END asking_price,
           CASE WHEN money(adjusted_price) >= 0 THEN money(adjusted_price) END adjusted_price,
           try_cast(minimum_nights AS INTEGER) minimum_nights,
           try_cast(maximum_nights AS INTEGER) maximum_nights,
           row_number() OVER (PARTITION BY listing_id, date ORDER BY sha256(to_json(input))) ordinal
    FROM input
)
SELECT ?::VARCHAR city, ?::DATE snapshot_date, ?::VARCHAR currency_code, ?::VARCHAR _lineage,
       * EXCLUDE(ordinal)
FROM typed WHERE ordinal = 1 ORDER BY listing_id, date
