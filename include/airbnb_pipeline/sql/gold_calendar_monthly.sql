-- Advertised unavailability is not proof of a booking; never label this occupancy.
SELECT c.city, c.snapshot_date, c.currency_code,
       coalesce(l.neighbourhood, 'unknown') neighbourhood,
       coalesce(l.room_type, 'unknown') room_type,
       date_trunc('month', c.date)::DATE AS "month",
       count(*)::BIGINT calendar_days,
       count(*) FILTER (WHERE c.available IS TRUE)::BIGINT available_days,
       count(*) FILTER (WHERE c.available IS FALSE)::BIGINT unavailable_days,
       count(*) FILTER (WHERE c.available IS NULL)::BIGINT unknown_availability_days,
       count(*) FILTER (WHERE l.id IS NULL)::BIGINT unmatched_listing_days,
       (count(*) FILTER (WHERE c.available IS TRUE) / nullif(count(c.available), 0))::DECIMAL(10,6) advertised_availability_rate,
       avg(c.asking_price)::DECIMAL(18,2) average_asking_price,
       ?::VARCHAR _lineage
FROM calendar c LEFT JOIN listings l ON c.listing_id = l.id
GROUP BY ALL ORDER BY c.city, c.snapshot_date, month, neighbourhood, room_type
