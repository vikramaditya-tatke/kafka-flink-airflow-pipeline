SELECT city, snapshot_date, currency_code, neighbourhood, room_type,
       count(*)::BIGINT listing_count, count(DISTINCT host_id)::BIGINT host_count,
       count(asking_price)::BIGINT priced_listing_count,
       avg(asking_price)::DECIMAL(18,2) average_asking_price,
       median(asking_price)::DECIMAL(18,2) median_asking_price,
       count(review_rating)::BIGINT rated_listing_count,
       avg(review_rating)::DECIMAL(8,4) average_review_rating,
       ?::VARCHAR _lineage
FROM listings GROUP BY ALL ORDER BY city, snapshot_date, neighbourhood, room_type
