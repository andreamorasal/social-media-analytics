import json

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "social_media_events"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ============================================================
# INVALID EVENTS
# ============================================================
#
# These are VALID JSON documents.
#
# However, the DATA is intentionally invalid according to
# the PySpark data-quality rules.
# ============================================================

invalid_events = [

    # ========================================================
    # 1. Missing user_id
    # ========================================================
    {
        "event_id": "invalid_001",
        "event_type": "like",

        "user_id": None,
        "username": "user_025",
        "country": "Germany",
        "user_created_at": "2026-01-15T10:00:00+00:00",

        "target_user_id": "user_10",
        "target_username": "user_010",

        "post_id": "post_1",
        "post_creator_user_id": "user_10",
        "post_created_at": "2026-02-10T12:00:00+00:00",
        "hashtag": "#travel",

        "comment_text": None,

        "timestamp": "2026-09-03T17:30:00+00:00"
    },


    # ========================================================
    # 2. Invalid event type
    # ========================================================
    {
        "event_id": "invalid_002",
        "event_type": "unknown_event",

        "user_id": "user_25",
        "username": "user_025",
        "country": "Germany",
        "user_created_at": "2026-01-15T10:00:00+00:00",

        "target_user_id": "user_10",
        "target_username": "user_010",

        "post_id": "post_1",
        "post_creator_user_id": "user_10",
        "post_created_at": "2026-02-10T12:00:00+00:00",
        "hashtag": "#travel",

        "comment_text": None,

        "timestamp": "2026-09-03T17:30:01+00:00"
    },


    # ========================================================
    # 3. Like without a post
    # ========================================================
    {
        "event_id": "invalid_003",
        "event_type": "like",

        "user_id": "user_30",
        "username": "user_030",
        "country": "France",
        "user_created_at": "2026-01-20T10:00:00+00:00",

        "target_user_id": "user_10",
        "target_username": "user_010",

        "post_id": None,
        "post_creator_user_id": None,
        "post_created_at": None,
        "hashtag": None,

        "comment_text": None,

        "timestamp": "2026-09-03T17:30:02+00:00"
    },


    # ========================================================
    # 4. Comment without comment text
    # ========================================================
    {
        "event_id": "invalid_004",
        "event_type": "comment",

        "user_id": "user_40",
        "username": "user_040",
        "country": "Spain",
        "user_created_at": "2026-01-25T10:00:00+00:00",

        "target_user_id": "user_10",
        "target_username": "user_010",

        "post_id": "post_10",
        "post_creator_user_id": "user_10",
        "post_created_at": "2026-02-15T12:00:00+00:00",
        "hashtag": "#food",

        "comment_text": None,

        "timestamp": "2026-09-03T17:30:03+00:00"
    },


    # ========================================================
    # 5. Follow without target user
    # ========================================================
    {
        "event_id": "invalid_005",
        "event_type": "follow",

        "user_id": "user_50",
        "username": "user_050",
        "country": "Netherlands",
        "user_created_at": "2026-02-01T10:00:00+00:00",

        "target_user_id": None,
        "target_username": None,

        "post_id": None,
        "post_creator_user_id": None,
        "post_created_at": None,
        "hashtag": None,

        "comment_text": None,

        "timestamp": "2026-09-03T17:30:04+00:00"
    },


    # ========================================================
    # 6. Missing timestamp
    # ========================================================
    {
        "event_id": "invalid_006",
        "event_type": "profile_visit",

        "user_id": "user_60",
        "username": "user_060",
        "country": "Italy",
        "user_created_at": "2026-02-05T10:00:00+00:00",

        "target_user_id": "user_70",
        "target_username": "user_070",

        "post_id": None,
        "post_creator_user_id": None,
        "post_created_at": None,
        "hashtag": None,

        "comment_text": None,

        "timestamp": None
    }
]


# ============================================================
# SEND INVALID EVENTS
# ============================================================

for event in invalid_events:

    producer.send(
        KAFKA_TOPIC,
        value=event
    )

    print("Sent invalid event:")
    print(json.dumps(event, indent=2))
    print()


producer.flush()
producer.close()

print("All invalid test events sent.")