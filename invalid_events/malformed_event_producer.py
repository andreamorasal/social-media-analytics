from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "social_media_events"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
)


# ============================================================
# DELIBERATELY MALFORMED JSON
# ============================================================
#
# The JSON is intentionally invalid because the final
# closing } is missing.
#
# The fields otherwise follow the new event structure.
# ============================================================

malformed_event = (
    '{"event_id":"malformed_001",'
    '"event_type":"like",'

    '"user_id":"user_25",'
    '"username":"user_025",'
    '"country":"Germany",'
    '"user_created_at":"2026-01-15T10:00:00+00:00",'

    '"target_user_id":"user_10",'
    '"target_username":"user_010",'

    '"post_id":"post_1",'
    '"post_creator_user_id":"user_10",'
    '"post_created_at":"2026-02-10T12:00:00+00:00",'
    '"hashtag":"#travel",'

    '"comment_text":null,'
    '"timestamp":"2026-09-03T17:30:10+00:00"'
)


producer.send(
    KAFKA_TOPIC,
    value=malformed_event.encode("utf-8")
)

producer.flush()
producer.close()

print("Malformed JSON event sent.")