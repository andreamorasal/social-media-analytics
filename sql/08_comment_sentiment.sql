-- Create COMMENT_SENTIMENT:

CREATE TABLE IF NOT EXISTS SOCIAL_MEDIA_DB_AMS.GOLD.COMMENT_SENTIMENT (
    EVENT_ID STRING,
    POST_ID STRING,
    USER_ID STRING,
    COMMENT_TEXT STRING,
    SENTIMENT STRING,
    EVENT_TIMESTAMP TIMESTAMP_TZ,
    CALCULATED_AT TIMESTAMP_TZ
);

-- Populate COMMENT_SENTIMENT: 

INSERT INTO SOCIAL_MEDIA_DB_AMS.GOLD.COMMENT_SENTIMENT
(
    EVENT_ID,
    POST_ID,
    USER_ID,
    COMMENT_TEXT,
    SENTIMENT,
    EVENT_TIMESTAMP,
    CALCULATED_AT
)
SELECT
    EVENT_ID,
    POST_ID,
    USER_ID,
    COMMENT_TEXT,

    CASE
        WHEN LOWER(COMMENT_TEXT) IN (
            'amazing!',
            'love this!',
            'this is fantastic!',
            'great post!',
            'absolutely beautiful!',
            'this made my day!',
            'so inspiring!',
            'i really enjoyed this!',
            'wonderful content!',
            'this is awesome!',
            'great job!',
            'i love it!',
            'very helpful!',
            'fantastic video!',
            'such a great experience!'
        )
        THEN 'Positive'

        WHEN LOWER(COMMENT_TEXT) IN (
            'interesting.',
            'thanks for sharing.',
            'i see.',
            'good to know.',
            'interesting information.',
            'thanks for posting.',
            'i had not seen this before.',
            'this is useful information.',
            'nice post.',
            'good information.',
            'i will keep this in mind.',
            'interesting perspective.',
            'thanks for the update.',
            'i understand.',
            'noted.'
        )
        THEN 'Neutral'

        WHEN LOWER(COMMENT_TEXT) IN (
            'i don''t like this.',
            'this was disappointing.',
            'not very useful.',
            'i disagree with this.',
            'this could be much better.',
            'i did not enjoy this.',
            'this is disappointing.',
            'the quality is poor.',
            'i expected more.',
            'not a good experience.',
            'this did not work for me.',
            'i am not impressed.',
            'unfortunately this was bad.',
            'i don''t recommend this.',
            'this needs improvement.'
        )
        THEN 'Negative'

        ELSE 'Neutral'
    END AS SENTIMENT,

    EVENT_TIMESTAMP,
    CURRENT_TIMESTAMP() AS CALCULATED_AT

FROM SOCIAL_MEDIA_DB_AMS.SILVER.CURATED_EVENTS
WHERE EVENT_TYPE = 'comment'
  AND COMMENT_TEXT IS NOT NULL;
