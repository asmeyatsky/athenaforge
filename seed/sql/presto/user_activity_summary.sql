-- User Activity Summary Query (Presto/Athena SQL)
WITH user_sessions AS (
    SELECT
        user_id,
        session_id,
        DATE_TRUNC('hour', event_timestamp) AS session_hour,
        ELEMENT_AT(SPLIT(page_url, '/'), 1) AS domain,
        CARDINALITY(ARRAY_AGG(event_type)) AS event_count,
        ARBITRARY(device_type) AS primary_device,
        FROM_UNIXTIME(MIN(TO_UNIXTIME(event_timestamp))) AS session_start,
        FROM_UNIXTIME(MAX(TO_UNIXTIME(event_timestamp))) AS session_end
    FROM analytics.user_events
    WHERE event_timestamp >= DATE_ADD('day', -7, CURRENT_TIMESTAMP)
    GROUP BY user_id, session_id, DATE_TRUNC('hour', event_timestamp),
             ELEMENT_AT(SPLIT(page_url, '/'), 1)
)
SELECT
    CAST(session_hour AS VARCHAR) AS hour_str,
    COUNT(DISTINCT user_id) AS active_users,
    COUNT(session_id) AS total_sessions,
    AVG(event_count) AS avg_events_per_session,
    AVG(DATE_DIFF('second', session_start, session_end)) AS avg_session_duration_sec,
    APPROX_PERCENTILE(event_count, 0.95) AS p95_events
FROM user_sessions
GROUP BY session_hour
ORDER BY session_hour DESC
