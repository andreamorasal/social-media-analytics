
## Pipeline Architecture

```mermaid
flowchart TD
    A[Python Event Simulator] --> B[Apache Kafka]
    B --> C[PySpark Structured Streaming]
    C --> D[Data Quality Checks]
    D --> E[Deduplication]
    E --> F[Snowflake RAW_EVENTS]
```
