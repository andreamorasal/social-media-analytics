import json
import random
import time
import uuid
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "social_media_events"

NUMBER_OF_USERS = 100
NUMBER_OF_POSTS = 500

# 20 events/second = 1,200 events/minute
EVENTS_PER_SECOND = 20

# Post that will be used for viral-content detection
VIRAL_POST_ID = "post_1"


# ============================================================
# USERS / POSTS DATA
# ============================================================

COUNTRIES = [
    "Netherlands",
    "Germany",
    "Spain",
    "France",
    "United Kingdom",
    "Italy",
    "Belgium",
    "Portugal",
    "United States",
    "Canada"
]

HASHTAGS = [
    "#travel",
    "#food",
    "#fitness",
    "#music",
    "#technology",
    "#fashion",
    "#sports",
    "#gaming",
    "#photography",
    "#lifestyle",
    "#nature",
    "#business",
    "#coding",
    "#movies",
    "#vacation"
]

# Some hashtags are intentionally more popular.
HASHTAG_WEIGHTS = [
    20,
    15,
    14,
    12,
    10,
    8,
    7,
    5,
    4,
    3,
    2,
    2,
    1,
    1,
    1
]


# ============================================================
# EVENT TYPES
# ============================================================

EVENT_TYPES = [
    "post_created",
    "like",
    "comment",
    "share",
    "follow",
    "video_view",
    "profile_visit"
]

EVENT_WEIGHTS = [
    2,    # post_created
    35,   # like
    10,   # comment
    10,   # share
    5,    # follow
    25,   # video_view
    13    # profile_visit
]


# ============================================================
# COMMENT DATA
# Used later for sentiment analysis
# ============================================================

POSITIVE_COMMENTS = [
    "Amazing!",
    "Love this!",
    "This is fantastic!",
    "Great post!",
    "Absolutely beautiful!",
    "This made my day!",
    "So inspiring!",
    "I really enjoyed this!",
    "Wonderful content!",
    "This is awesome!",
    "Great job!",
    "I love it!",
    "Very helpful!",
    "Fantastic video!",
    "Such a great experience!"
]

NEUTRAL_COMMENTS = [
    "Interesting.",
    "Thanks for sharing.",
    "I see.",
    "Good to know.",
    "Interesting information.",
    "Thanks for posting.",
    "I had not seen this before.",
    "This is useful information.",
    "Nice post.",
    "Good information.",
    "I will keep this in mind.",
    "Interesting perspective.",
    "Thanks for the update.",
    "I understand.",
    "Noted."
]

NEGATIVE_COMMENTS = [
    "I don't like this.",
    "This was disappointing.",
    "Not very useful.",
    "I disagree with this.",
    "This could be much better.",
    "I did not enjoy this.",
    "This is disappointing.",
    "The quality is poor.",
    "I expected more.",
    "Not a good experience.",
    "This did not work for me.",
    "I am not impressed.",
    "Unfortunately this was bad.",
    "I don't recommend this.",
    "This needs improvement."
]


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def utc_now():
    """Return current UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def random_past_timestamp(days=365):
    """Generate a realistic timestamp from the previous year."""

    now = datetime.now(timezone.utc)

    random_seconds = random.randint(
        0,
        days * 24 * 60 * 60
    )

    timestamp = now - timedelta(seconds=random_seconds)

    return timestamp.isoformat()


# ============================================================
# GENERATE USERS
# ============================================================

def generate_users():

    users = []

    for i in range(1, NUMBER_OF_USERS + 1):

        user = {
            "user_id": f"user_{i}",
            "username": f"user_{i:03d}",
            "country": random.choice(COUNTRIES),
            "created_at": random_past_timestamp()
        }

        users.append(user)

    return users


# ============================================================
# GENERATE POSTS
# ============================================================

def generate_posts(users):

    posts = []

    for i in range(1, NUMBER_OF_POSTS + 1):

        post_id = f"post_{i}"

        creator = random.choice(users)

        # post_1 is always #travel
        if post_id == VIRAL_POST_ID:
            hashtag = "#travel"
        else:
            hashtag = random.choices(
                HASHTAGS,
                weights=HASHTAG_WEIGHTS,
                k=1
            )[0]

        post = {
            "post_id": post_id,
            "user_id": creator["user_id"],
            "hashtag": hashtag,
            "created_at": random_past_timestamp()
        }

        posts.append(post)

    return posts


# ============================================================
# SELECT EVENT TYPE
# ============================================================

def choose_event_type():

    return random.choices(
        EVENT_TYPES,
        weights=EVENT_WEIGHTS,
        k=1
    )[0]


# ============================================================
# GENERATE COMMENT
# ============================================================

def generate_comment():

    sentiment = random.choices(
        ["positive", "neutral", "negative"],
        weights=[45, 35, 20],
        k=1
    )[0]

    if sentiment == "positive":
        return random.choice(POSITIVE_COMMENTS)

    if sentiment == "neutral":
        return random.choice(NEUTRAL_COMMENTS)

    return random.choice(NEGATIVE_COMMENTS)


# ============================================================
# SELECT RANDOM USER DIFFERENT FROM CURRENT USER
# ============================================================

def choose_target_user(users, current_user):

    target_user = random.choice(users)

    while target_user["user_id"] == current_user["user_id"]:
        target_user = random.choice(users)

    return target_user


# ============================================================
# CREATE EVENT
# ============================================================

def create_event(event_type, users, posts):

    # User performing the event
    user = random.choice(users)

    # Another user, used for follow/profile interactions
    target_user = choose_target_user(users, user)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,

        # ----------------------------------------------------
        # USER INFORMATION
        # ----------------------------------------------------
        "user_id": user["user_id"],
        "username": user["username"],
        "country": user["country"],
        "user_created_at": user["created_at"],

        # ----------------------------------------------------
        # TARGET USER
        # ----------------------------------------------------
        "target_user_id": None,
        "target_username": None,

        # ----------------------------------------------------
        # POST INFORMATION
        # ----------------------------------------------------
        "post_id": None,
        "post_creator_user_id": None,
        "post_created_at": None,
        "hashtag": None,

        # ----------------------------------------------------
        # COMMENT
        # ----------------------------------------------------
        "comment_text": None,

        # ----------------------------------------------------
        # EVENT TIMESTAMP
        # ----------------------------------------------------
        "timestamp": utc_now()
    }


    # ========================================================
    # LIKE
    # ========================================================

    if event_type == "like":

        # 40% of likes go to post_1
        if random.random() < 0.40:
            post = posts[0]
        else:
            post = random.choice(posts)

        creator = next(
            u for u in users
            if u["user_id"] == post["user_id"]
        )

        event["target_user_id"] = creator["user_id"]
        event["target_username"] = creator["username"]

        event["post_id"] = post["post_id"]
        event["post_creator_user_id"] = post["user_id"]
        event["post_created_at"] = post["created_at"]
        event["hashtag"] = post["hashtag"]


    # ========================================================
    # COMMENT
    # ========================================================

    elif event_type == "comment":

        post = random.choice(posts)

        creator = next(
            u for u in users
            if u["user_id"] == post["user_id"]
        )

        event["target_user_id"] = creator["user_id"]
        event["target_username"] = creator["username"]

        event["post_id"] = post["post_id"]
        event["post_creator_user_id"] = post["user_id"]
        event["post_created_at"] = post["created_at"]
        event["hashtag"] = post["hashtag"]

        event["comment_text"] = generate_comment()


    # ========================================================
    # SHARE
    # ========================================================

    elif event_type == "share":

        post = random.choice(posts)

        creator = next(
            u for u in users
            if u["user_id"] == post["user_id"]
        )

        event["target_user_id"] = creator["user_id"]
        event["target_username"] = creator["username"]

        event["post_id"] = post["post_id"]
        event["post_creator_user_id"] = post["user_id"]
        event["post_created_at"] = post["created_at"]
        event["hashtag"] = post["hashtag"]


    # ========================================================
    # VIDEO VIEW
    # ========================================================

    elif event_type == "video_view":

        post = random.choice(posts)

        creator = next(
            u for u in users
            if u["user_id"] == post["user_id"]
        )

        event["target_user_id"] = creator["user_id"]
        event["target_username"] = creator["username"]

        event["post_id"] = post["post_id"]
        event["post_creator_user_id"] = post["user_id"]
        event["post_created_at"] = post["created_at"]
        event["hashtag"] = post["hashtag"]


    # ========================================================
    # FOLLOW
    # ========================================================

    elif event_type == "follow":

        event["target_user_id"] = target_user["user_id"]
        event["target_username"] = target_user["username"]


    # ========================================================
    # PROFILE VISIT
    # ========================================================

    elif event_type == "profile_visit":

        event["target_user_id"] = target_user["user_id"]
        event["target_username"] = target_user["username"]


    # ========================================================
    # POST CREATED
    # ========================================================

    elif event_type == "post_created":

        post = random.choice(posts)

        creator = next(
            u for u in users
            if u["user_id"] == post["user_id"]
        )

        event["target_user_id"] = creator["user_id"]
        event["target_username"] = creator["username"]

        event["post_id"] = post["post_id"]
        event["post_creator_user_id"] = post["user_id"]
        event["post_created_at"] = post["created_at"]
        event["hashtag"] = post["hashtag"]


    return event


# ============================================================
# MAIN
# ============================================================

print()
print("==============================================")
print("SOCIAL MEDIA EVENT SIMULATOR")
print("==============================================")
print()

print("Generating users...")

users = generate_users()

print(f"Generated {len(users)} users.")

print("Generating posts...")

posts = generate_posts(users)

print(f"Generated {len(posts)} posts.")

print()
print("Users and posts are kept in memory for event generation.")
print("No additional JSON files are created.")
print()

print("Sample USER:")
print(json.dumps(users[0], indent=2))

print()
print("Sample POST:")
print(json.dumps(posts[0], indent=2))

print()
print("Starting continuous event generation...")
print()
print(f"Kafka topic: {KAFKA_TOPIC}")
print(f"Events per second: {EVENTS_PER_SECOND}")
print(f"Expected events per minute: {EVENTS_PER_SECOND * 60}")
print(f"Viral post: {VIRAL_POST_ID}")
print()
print("Press Ctrl+C to stop.")
print()


event_count = 0
start_time = time.time()


try:

    while True:

        loop_start = time.time()

        for _ in range(EVENTS_PER_SECOND):

            event_type = choose_event_type()

            event = create_event(
                event_type,
                users,
                posts
            )

            producer.send(
                KAFKA_TOPIC,
                value=event
            )

            event_count += 1

        producer.flush()

        elapsed = time.time() - start_time

        if elapsed > 0:

            events_per_second = event_count / elapsed
            events_per_minute = events_per_second * 60

        else:

            events_per_second = 0
            events_per_minute = 0

        print(
            f"Events: {event_count:,} | "
            f"Rate: {events_per_second:.2f}/sec | "
            f"~{events_per_minute:.0f}/min"
        )

        loop_elapsed = time.time() - loop_start

        sleep_time = max(
            0,
            1 - loop_elapsed
        )

        time.sleep(sleep_time)


except KeyboardInterrupt:

    print()
    print("Stopping simulator...")


finally:

    producer.flush()
    producer.close()

    print()
    print("Simulator stopped.")
    print(f"Total events generated: {event_count:,}")