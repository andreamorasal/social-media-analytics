# Social Media Analytics Pipeline

A Data Engineering portfolio project exploring real time social media event processing using Python, Apache Kafka, PySpark Structured Streaming and Snowflake.

## Project overview

This project simulates social media activity inspired by platforms such as Instagram, TikTok, LinkedIn and X. It focuses on processing events such as posts, likes, comments, shares, follows, video views and profile visits through a streaming data pipeline.

The simulator generates 100 users and 500 posts, continuously produces social media events at a target rate of at least 1,000 events per minute, and supports the simulation of viral events.

The pipeline ingests events from Kafka, parses and validates their JSON payloads using PySpark Structured Streaming, removes duplicate event IDs within the streaming processing flow, and writes valid records to Snowflake for storage and further analysis.

The project connects Data Engineering with business analytics by exploring how social media activity can be transformed into insights into engagement, trending hashtags, viral content, influencer performance and comment sentiment.

## Technology stack

| Technology | Purpose |
|---|---|
| Python | Simulates social media users, posts and events |
| Apache Kafka | Streams events through the `social_media_events` topic |
| PySpark Structured Streaming | Parses, validates, filters and processes incoming events |
| Snowflake | Stores processed events for downstream analysis |
| SQL | Supports data modelling, transformation and analytical queries |
| Tableau | Supports business intelligence and data visualisation |
| Python Snowflake Connector | Writes processed micro batches to Snowflake |
| `python-dotenv` | Loads Snowflake credentials from environment variables |
| Visual Studio Code | Development environment |

## Pipeline architecture

The pipeline streams simulated social media events through Kafka and PySpark Structured Streaming, where events are parsed, validated and deduplicated before being stored in Snowflake. SQL scripts then transform, enrich and aggregate the raw data to produce curated datasets for social media analytics and reporting.

**Pipeline architecture diagram**

SCREENSHOT 

## Event deneration and ingestion

A Python simulator generates social media events and publishes them to the Kafka topic `social_media_events`.

The PySpark streaming application connects to Kafka at `localhost:9092` and consumes events from the topic, starting from the latest available offsets when a new streaming query begins.

The supported event types are:

- `post_created`
- `like`
- `comment`
- `share`
- `follow`
- `video_view`
- `profile_visit`

Depending on the event type, each event can contain information about the user, target user, post, hashtag, comment text and event timestamp.

## Real time processing with PySpark

PySpark Structured Streaming processes incoming Kafka messages through several stages.

### JSON parsing

Kafka message values are converted into strings and parsed using a predefined PySpark schema. The event fields are then extracted into a structured DataFrame.

### Data quality validation

The pipeline checks that events contain the required information and meet event specific validation rules.

These checks include:

- The presence of an event ID, user ID and event timestamp.
- Validation against the seven supported event types.
- Required post information for post related events.
- Comment text for comment events.
- A target user ID for follow and profile visit events.

Events that fail these validation checks are excluded from the valid streaming DataFrame.

### Duplicate handling

The pipeline applies `dropDuplicates(["event_id"])` to remove duplicate event IDs within the streaming processing flow.

### Micro batch processing

The validated DataFrame is processed using `foreachBatch`. Each non empty micro batch is collected and inserted into Snowflake using the Python Snowflake Connector.

The application uses a checkpoint location to support the recovery of streaming progress.

## Snowflake data storage

The streaming application writes valid, deduplicated records to the following Snowflake destination:

| Setting | Value |
|---|---|
| Database | `SOCIAL_MEDIA_DB_AMS` |
| Schema | `BRONZE` |
| Table | `RAW_EVENTS` |
| Warehouse | `LAB_WH_AMS` |

The table receives the event fields, including identifiers, user information, target user information, post information, comment text and the event timestamp.

Snowflake generates the ingestion timestamp through the table's `INGESTED_AT` default, provided the table is configured accordingly.

This Bronze layer stores the raw event data for subsequent transformations and analytics.

## Data model

The event schema includes the following groups of fields:

| Field Group | Examples |
|---|---|
| Event identification | `event_id`, `event_type` |
| User information | `user_id`, `username`, `country`, `user_created_at` |
| Target user | `target_user_id`, `target_username` |
| Post information | `post_id`, `post_creator_user_id`, `post_created_at`, `hashtag` |
| Comment information | `comment_text` |
| Event timing | `timestamp` |

The source event timestamp is stored as `EVENT_TIMESTAMP` in Snowflake.

## Business analytics

The project covers several analytical use cases that can be developed from the stored social media events.

### Trending hashtags

Analyse hashtag popularity and engagement over time to identify emerging topics and changes in user activity.

### Viral content detection

Explore how posts with rapid engagement can be identified, including a scenario in which a post receives more than 500 likes within five minutes.

### Influencer ranking

Compare content creators using engagement indicators such as likes, comments and shares.

### Comment sentiment

Explore the classification of comments into positive, neutral and negative sentiment categories to understand audience reactions.

These use cases are supported by the project's planned analytical SQL scripts. Their actual implementation depends on the corresponding transformations, analytical datasets and sentiment classification logic.

## Dashboard and visualisation

Tableau can be used to present the analytical outputs through dashboards focused on social media engagement, trending hashtags, viral content, influencer performance and comment sentiment.

## Project screenshots

Add screenshots here to illustrate the pipeline architecture, Snowflake data and, if available, the Tableau dashboard.

### Pipeline architecture

Add your pipeline architecture image here, if you are using a separate image rather than a Mermaid diagram.

### Snowflake results

Add a screenshot showing the data stored in the `RAW_EVENTS` table.

### Tableau dashboard

Add a screenshot if you have implemented a Tableau dashboard.

## Key skills demonstrated

- Python event simulation and JSON processing
- Apache Kafka event ingestion
- PySpark Structured Streaming
- Data quality validation and duplicate handling
- Micro batch processing
- Snowflake data ingestion
- SQL data modelling, transformation and analytical preparation
- Streaming checkpoint configuration
- Connecting technical data pipelines with business analytics

## Project Context

This project demonstrates the application of Data Engineering principles to a social media analytics use case. It combines real time event streaming, data quality validation, Snowflake data modelling and SQL transformations to build a pipeline that supports downstream analytics and business reporting.
