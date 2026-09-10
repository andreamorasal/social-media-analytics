-- Create table TRENDING_HASHTAGS:

CREATE TABLE IF NOT EXISTS SOCIAL_MEDIA_DB_AMS.GOLD.TRENDING_HASHTAGS (
    WINDOW_MINUTES INTEGER,
    HASHTAG STRING,
    EVENT_COUNT INTEGER,
    RANK INTEGER,
    WINDOW_START TIMESTAMP_TZ,
    WINDOW_END TIMESTAMP_TZ
);


-- Populate TRENDING_HASHTAGS:


INSERT INTO SOCIAL_MEDIA_DB_AMS.GOLD.TRENDING_HASHTAGS
(
    WINDOW_MINUTES,
    WINDOW_START,
    WINDOW_END,
    HASHTAG,
    EVENT_COUNT,
    RANK
)

WITH max_time AS (
    SELECT MAX(EVENT_TIMESTAMP) AS latest_timestamp
    FROM SOCIAL_MEDIA_DB_AMS.SILVER.CURATED_EVENTS
),

windows AS (
    SELECT 1 AS window_minutes
    UNION ALL
    SELECT 5
    UNION ALL
    SELECT 15
),

hashtag_counts AS (
    SELECT
        w.window_minutes,
        DATEADD(
            minute,
            -w.window_minutes,
            m.latest_timestamp
        ) AS window_start,
        m.latest_timestamp AS window_end,
        c.HASHTAG,
        COUNT(*) AS event_count
    FROM windows w
    CROSS JOIN max_time m
    JOIN SOCIAL_MEDIA_DB_AMS.SILVER.CURATED_EVENTS c
        ON c.EVENT_TIMESTAMP >= DATEADD(
            minute,
            -w.window_minutes,
            m.latest_timestamp
        )
        AND c.EVENT_TIMESTAMP <= m.latest_timestamp
    WHERE c.HASHTAG IS NOT NULL
    GROUP BY
        w.window_minutes,
        m.latest_timestamp,
        c.HASHTAG
),

ranked AS (
    SELECT
        window_minutes,
        window_start,
        window_end,
        hashtag,
        event_count,
        ROW_NUMBER() OVER (
            PARTITION BY window_minutes
            ORDER BY event_count DESC
        ) AS rank
    FROM hashtag_counts
)

SELECT
    window_minutes,
    window_start,
    window_end,
    hashtag,
    event_count,
    rank
FROM ranked
WHERE rank <= 10;
