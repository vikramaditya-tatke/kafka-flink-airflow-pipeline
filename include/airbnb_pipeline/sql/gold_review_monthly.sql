SELECT r.city, r.snapshot_date, r.currency_code,
       coalesce(l.neighbourhood, 'unknown') neighbourhood,
       coalesce(l.room_type, 'unknown') room_type,
       date_trunc('month', r.date)::DATE AS "month",
       count(*)::BIGINT review_count, count(DISTINCT r.listing_id)::BIGINT reviewed_listing_count,
       count(*) FILTER (WHERE l.id IS NULL)::BIGINT unmatched_listing_reviews,
       avg(r.comment_length)::DECIMAL(18,2) average_comment_length,
       ?::VARCHAR _lineage
FROM reviews r LEFT JOIN listings l ON r.listing_id = l.id
GROUP BY ALL ORDER BY r.city, r.snapshot_date, month, neighbourhood, room_type
