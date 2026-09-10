-- =========================================================
-- DATA QUALITY CHECKS
-- =========================================================

-- 1. DUPLICATE EVENTS

SELECT
    EVENT_ID,
    COUNT(*) AS DUPLICATE_COUNT
FROM SOCIAL_MEDIA_DB_AMS.SILVER.CURATED_EVENTS
GROUP BY EVENT_ID
HAVING COUNT(*) > 1;


-- 2. MISSING REQUIRED EVENT FIELDS

SELECT
    COUNT_IF(EVENT_ID IS NULL) AS NULL_EVENT_ID,
    COUNT_IF(EVENT_TYPE IS NULL) AS NULL_EVENT_TYPE,
    COUNT_IF(USER_ID IS NULL) AS NULL_USER_ID,
    COUNT_IF(EVENT_TIMESTAMP IS NULL) AS NULL_EVENT_TIMESTAMP
FROM SOCIAL_MEDIA_DB_AMS.SILVER.CURATED_EVENTS;


-- 3. INVALID EVENT TYPES

SELECT DISTINCT
    EVENT_TYPE
FROM SOCIAL_MEDIA_DB_AMS.SILVER.CURATED_EVENTS
WHERE EVENT_TYPE NOT IN (
    'post_created',
    'like',
    'comment',
    'share',
    'follow',
    'video_view',
    'profile_visit'
);


-- 4. ORPHAN USERS

SELECT DISTINCT
    CE.USER_ID
FROM SOCIAL_MEDIA_DB_AMS.SILVER.CURATED_EVENTS CE
LEFT JOIN SOCIAL_MEDIA_DB_AMS.SILVER.USER_DIM U
    ON CE.USER_ID = U.USER_ID
WHERE CE.USER_ID IS NOT NULL
  AND U.USER_ID IS NULL;


-- 5. ORPHAN POSTS

SELECT DISTINCT
    CE.POST_ID
FROM SOCIAL_MEDIA_DB_AMS.SILVER.CURATED_EVENTS CE
LEFT JOIN SOCIAL_MEDIA_DB_AMS.SILVER.POST_DIM P
    ON CE.POST_ID = P.POST_ID
WHERE CE.POST_ID IS NOT NULL
  AND P.POST_ID IS NULL;


-- 6. DUPLICATE USERS

SELECT
    USER_ID,
    COUNT(*) AS DUPLICATE_COUNT
FROM SOCIAL_MEDIA_DB_AMS.SILVER.USER_DIM
GROUP BY USER_ID
HAVING COUNT(*) > 1;


-- 7. DUPLICATE POSTS

SELECT
    POST_ID,
    COUNT(*) AS DUPLICATE_COUNT
FROM SOCIAL_MEDIA_DB_AMS.SILVER.POST_DIM
GROUP BY POST_ID
HAVING COUNT(*) > 1;


-- 8. GOLD TABLE RECORD COUNTS

SELECT
    'TRENDING_HASHTAGS' AS TABLE_NAME,
    COUNT(*) AS RECORD_COUNT
FROM SOCIAL_MEDIA_DB_AMS.GOLD.TRENDING_HASHTAGS

UNION ALL

SELECT
    'VIRAL_POSTS',
    COUNT(*)
FROM SOCIAL_MEDIA_DB_AMS.GOLD.VIRAL_POSTS

UNION ALL

SELECT
    'INFLUENCER_RANKING',
    COUNT(*)
FROM SOCIAL_MEDIA_DB_AMS.GOLD.INFLUENCER_RANKING

UNION ALL

SELECT
    'COMMENT_SENTIMENT',
    COUNT(*)
FROM SOCIAL_MEDIA_DB_AMS.GOLD.COMMENT_SENTIMENT;

-- =========================================================
-- 9. CHECK FIRST 10 CURATED EVENTS
-- =========================================================

SELECT *
FROM SOCIAL_MEDIA_DB_AMS.SILVER.CURATED_EVENTS
ORDER BY EVENT_TIMESTAMP
LIMIT 10;


-- =========================================================
-- 10. CHECK FIRST 10 USERS
-- =========================================================

SELECT *
FROM SOCIAL_MEDIA_DB_AMS.SILVER.USER_DIM
ORDER BY USER_ID
LIMIT 10;


-- =========================================================
-- 11. CHECK FIRST 10 POSTS
-- =========================================================

SELECT *
FROM SOCIAL_MEDIA_DB_AMS.SILVER.POST_DIM
ORDER BY POST_ID
LIMIT 10;


-- =========================================================
-- 12. CHECK FIRST 10 TRENDING HASHTAGS
-- =========================================================

SELECT *
FROM SOCIAL_MEDIA_DB_AMS.GOLD.TRENDING_HASHTAGS
LIMIT 10;


-- =========================================================
-- 13. CHECK FIRST 10 VIRAL POSTS
-- =========================================================

SELECT *
FROM SOCIAL_MEDIA_DB_AMS.GOLD.VIRAL_POSTS
LIMIT 10;


-- =========================================================
-- 14. CHECK FIRST 10 INFLUENCERS
-- =========================================================

SELECT *
FROM SOCIAL_MEDIA_DB_AMS.GOLD.INFLUENCER_RANKING
ORDER BY RANK
LIMIT 10;


-- =========================================================
-- 15. CHECK COMMENT SENTIMENT
-- =========================================================

SELECT *
FROM SOCIAL_MEDIA_DB_AMS.GOLD.COMMENT_SENTIMENT
LIMIT 10;


-- =========================================================
-- 16. CHECK EVENT TYPE DISTRIBUTION
-- =========================================================

SELECT
    EVENT_TYPE,
    COUNT(*) AS EVENT_COUNT
FROM SOCIAL_MEDIA_DB_AMS.SILVER.CURATED_EVENTS
GROUP BY EVENT_TYPE
ORDER BY EVENT_COUNT DESC;


-- =========================================================
-- 17. CHECK NULL VALUES IN DIMENSIONS
-- =========================================================

SELECT
    COUNT_IF(USER_ID IS NULL) AS NULL_USER_ID,
    COUNT_IF(USERNAME IS NULL) AS NULL_USERNAME,
    COUNT_IF(COUNTRY IS NULL) AS NULL_COUNTRY
FROM SOCIAL_MEDIA_DB_AMS.SILVER.USER_DIM;


-- =========================================================
-- 18. CHECK POST DIMENSION
-- =========================================================

SELECT
    COUNT_IF(POST_ID IS NULL) AS NULL_POST_ID,
    COUNT_IF(USER_ID IS NULL) AS NULL_USER_ID,
    COUNT_IF(CREATED_AT IS NULL) AS NULL_CREATED_AT
FROM SOCIAL_MEDIA_DB_AMS.SILVER.POST_DIM;