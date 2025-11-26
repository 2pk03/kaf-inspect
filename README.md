# Kafka Inspector

A fast, multi-purpose Kafka CLI to inspect topics, find duplicates, peek/search messages, and check consumer lag.

## Features

-   **Cluster Overview**: Get a high-level summary of your cluster, including topics, partitions, brokers, consumer groups, and optional integration with Schema Registry and Kafka Connect.
-   **Deduplication**: Find duplicate messages based on message `key`, `value`, or a specific JSON `field`.
-   **Topic Listing**: List all topics, with an interactive search for environments with many topics.
-   **Consumer Lag**: Check the current consumer group lag for any topic.
-   **Message Search**: Search for messages containing a specific string or matching a regular expression (`grep` for Kafka).
-   **Message Peeking**: Quickly view the first or last N messages on a topic.
-   **Flexible Storage**: Uses an in-memory store by default and can switch to a SQLite backend for large-scale operations.
-   **Structured Output**: Output data in plain text, `JSONL`, or `CSV` for easy integration with other tools.

## Quick start

```bash
pip install confluent-kafka requests

# List topics or get an overview fast
python3 kafkainspect.py --bootstrap-servers localhost:9092 --list-topics
python3 kafkainspect.py --bootstrap-servers localhost:9092 --overview

# Find duplicate messages by value/key or JSON field
python3 kafkainspect.py --bootstrap-servers localhost:9092 --topic my-topic --field user.id
```

## Installation

1.  Clone the repository.
2.  Install the required Python packages:

    ```bash
    pip install confluent-kafka requests
    ```

## Testing

To run the test suite, which uses mock objects and does not require a live Kafka cluster, use the following command:

```bash
python3 -m unittest test_kafkainspect.py
```

## Usage

The script can be run directly from the command line.

### Cluster Overview

Get a high-level summary of your Kafka cluster. This is useful for a quick health check.

```bash
python3 kafkainspect.py \
  --bootstrap-servers <host:port> \
  --overview
```

For a more complete picture, include your Schema Registry and Kafka Connect URLs:

```bash
python3 kafkainspect.py \
  --bootstrap-servers <host:port> \
  --overview \
  --schema-registry-url http://localhost:8081 \
  --connect-url http://localhost:8083
```

### Basic Example

Scan a topic and find duplicates based on the entire message value:

```bash
python kafkainspect.py \
  --bootstrap-servers localhost:9092 \
  --topic my-topic \
  --dedup-by value
```

### Listing Topics

List all available topics. If there are more than 50, an interactive search prompt will start automatically.

```bash
python3 kafkainspect.py --bootstrap-servers <host:port> --list-topics
```

### Checking Consumer Lag

Monitor the lag for a specific consumer group on a topic.

```bash
python3 kafkainspect.py \
  --bootstrap-servers <host:port> \
  --topic my-topic \
  --group-id my-consumer-group \
  --check-lag
```

### Peeking at Messages

Quickly view the last 5 messages on a topic. Use a negative number to see the first 5 (e.g., `--peek -5`).

```bash
python3 kafkainspect.py \
  --bootstrap-servers <host:port> \
  --topic my-topic \
  --peek 5
```

### Searching for Messages

Find all messages containing the string "error". Use `--regex` for regular expression matching.

```bash
python3 kafkainspect.py \
  --bootstrap-servers <host:port> \
  --topic logs.production \
  --search "error"
```

### Finding Duplicates by JSON Field

If your messages are JSON, you can find duplicates based on a nested field's value:

```bash
python3 kafkainspect.py \
  --bootstrap-servers localhost:9092 \
  --topic events.json \
  --field user.id
```

### Handling Large Topics with SQLite

For topics with millions of messages, use the `--sqlite` flag to store message hashes on disk, preventing high memory usage:

```bash
python3 kafkainspect.py \
  --bootstrap-servers localhost:9092 \
  --topic big-data-topic \
  --max-messages 10000000 \
  --sqlite /tmp/kafka_dedup.db
```

### Writing Output to a File (JSONL)

Save the details of duplicate messages to a file in `JSONL` format. Also supports `csv` and `text`.

```bash
python3 kafkainspect.py \
  --bootstrap-servers localhost:9092 \
  --topic logs.production \
  --output duplicates.jsonl:jsonl
```

### Command-Line Arguments

```
usage: kafkainspect.py [-h] --bootstrap-servers BOOTSTRAP_SERVERS [--topic TOPIC] [--overview] [--schema-registry-url SCHEMA_REGISTRY_URL] [--connect-url CONNECT_URL] [--list-topics] [--check-lag]
                       [--search SEARCH] [--regex] [--peek PEEK] [--group-id GROUP_ID] [--start {earliest,latest}] [--dedup-by {value,key}] [--field FIELD] [--max-messages MAX_MESSAGES]
                       [--sqlite SQLITE] [--output OUTPUT] [--silent]

Kafka topic inspector and deduplication tool.

options:
  -h, --help            show this help message and exit
  --bootstrap-servers BOOTSTRAP_SERVERS
                        Comma-separated list of Kafka bootstrap servers
  --topic TOPIC         Kafka topic to scan. Not required if --list-topics or --check-lag is used.
  --overview            Display a high-level overview of the cluster.
  --schema-registry-url SCHEMA_REGISTRY_URL
                        URL for the Schema Registry to include in overview.
  --connect-url CONNECT_URL
                        URL for Kafka Connect to include in overview.
  --list-topics         List topics interactively and exit.
  --check-lag           Check consumer group lag for a topic.
  --search SEARCH       Search for a pattern in message values.
  --regex               Treat search pattern as a regular expression.
  --peek PEEK           Peek at the first N (if negative) or last N (if positive) messages.
  --group-id GROUP_ID   Consumer group id (default: kafkainspect)
  --start {earliest,latest}
                        Start offset
  --dedup-by {value,key}
                        Field to deduplicate by (default: value)
  --field FIELD         JSON field to deduplicate by (e.g., user.id). Overrides --dedup-by.
  --max-messages MAX_MESSAGES
                        Limit messages to avoid OOM
  --sqlite SQLITE       Optional SQLite path for large-scale deduplication
  --output OUTPUT       Optional path to output file (e.g., out.txt:text, out.jsonl:jsonl, out.csv:csv)
  --silent              Suppress stdout output of duplicates
```
