#!/usr/bin/env python3
import argparse
import csv
import hashlib
import json
import os
import re
import sqlite3
import sys
import termios
import time
import tty
from collections import deque

import requests
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition, OFFSET_INVALID
from confluent_kafka.admin import AdminClient

OUTPUT_FORMATS = ('text', 'jsonl', 'csv')
SQLITE_COMMIT_INTERVAL = 1000
IDLE_TIMEOUT_SECONDS = 10.0


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka topic inspector and deduplication tool.")
    parser.add_argument('--bootstrap-servers', required=True, help='Comma-separated list of Kafka bootstrap servers')
    parser.add_argument('--topic', help='Kafka topic to scan. Not required if --list-topics or --overview is used.')
    parser.add_argument('--overview', action='store_true', help='Display a high-level overview of the cluster.')
    parser.add_argument('--schema-registry-url', help='URL for the Schema Registry to include in overview.')
    parser.add_argument('--connect-url', help='URL for Kafka Connect to include in overview.')
    parser.add_argument('--list-topics', action='store_true', help='List topics interactively and exit.')
    parser.add_argument('--check-lag', action='store_true', help='Check consumer group lag for a topic.')
    parser.add_argument('--search', help='Search for a pattern in message values.')
    parser.add_argument('--regex', action='store_true', help='Treat search pattern as a regular expression.')
    parser.add_argument('--peek', type=int, help='Peek at the first N (if negative) or last N (if positive) messages.')
    parser.add_argument('--group-id', default='kafkainspect', help='Consumer group id (default: kafkainspect)')
    parser.add_argument('--start', choices=['earliest', 'latest'], default='earliest', help='Start offset')
    parser.add_argument('--dedup-by', choices=['value', 'key'], default='value', help='Field to deduplicate by (default: value)')
    parser.add_argument('--field', help='JSON field to deduplicate by (e.g., user.id). Overrides --dedup-by.')
    parser.add_argument('--max-messages', type=int, default=1000000, help='Limit messages to avoid OOM')
    parser.add_argument('--sqlite', help='Optional SQLite path for large-scale deduplication')
    parser.add_argument('--sqlite-resume', action='store_true',
                        help='Keep hashes recorded by earlier runs instead of clearing them for this topic.')
    parser.add_argument('--output', help='Optional path to output file (e.g., out.txt:text, out.jsonl:jsonl, out.csv:csv)')
    parser.add_argument('--silent', action='store_true', help='Suppress stdout output of duplicates')
    parser.add_argument('-X', dest='config', action='append', default=[], metavar='KEY=VALUE',
                        help='librdkafka configuration property, repeatable '
                             '(e.g. -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512)')
    return parser.parse_args()


def parse_extra_config(entries):
    config = {}
    for entry in entries:
        key, sep, value = entry.partition('=')
        if not sep or not key.strip():
            raise ValueError(f"Invalid -X value '{entry}', expected KEY=VALUE")
        config[key.strip()] = value
    return config


def http_auth_from_env(var_name):
    """Reads 'user:password' from an environment variable to keep credentials off the command line."""
    raw = os.environ.get(var_name)
    if not raw:
        return None
    user, sep, password = raw.partition(':')
    if not sep:
        return None
    return (user, password)


def hash_payload(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def decode_value(msg) -> str:
    value = msg.value()
    return '' if value is None else value.decode(errors='ignore')


def decode_key(msg):
    key = msg.key()
    return None if key is None else key.decode(errors='ignore')


def get_field_from_json(payload, field_path):
    """Extracts a nested value from a JSON payload using dot notation."""
    if payload is None:
        return None
    try:
        data = json.loads(payload)
        for key in field_path.split('.'):
            if not isinstance(data, dict):
                return None
            data = data.get(key)
            if data is None:
                return None
        return json.dumps(data, sort_keys=True).encode('utf-8')
    except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
        return None


def build_consumer(args, extra_config):
    conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': args.group_id,
        'auto.offset.reset': args.start,
        'enable.auto.commit': False,
        'enable.partition.eof': True,
    }
    conf.update(extra_config)
    return Consumer(conf)


def topic_partitions(consumer, topic):
    metadata = consumer.list_topics(topic, timeout=10)
    topic_meta = metadata.topics.get(topic)
    if topic_meta is None:
        raise KafkaException(f"Topic '{topic}' not found.")
    if getattr(topic_meta, 'error', None) is not None:
        raise KafkaException(f"Topic '{topic}' unavailable: {topic_meta.error}")
    return sorted(p.id for p in topic_meta.partitions.values())


def assign_partitions(consumer, topic, start='earliest', tail=None):
    """Assigns every partition directly so the tool never joins or disturbs a consumer group."""
    assignments = []
    for partition_id in topic_partitions(consumer, topic):
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition_id), timeout=10)
        if tail is not None:
            offset = max(low, high - tail)
        elif start == 'latest':
            offset = high
        else:
            offset = low
        assignments.append(TopicPartition(topic, partition_id, offset))
    consumer.assign(assignments)
    return assignments


def iter_messages(consumer, max_messages=None, idle_timeout=IDLE_TIMEOUT_SECONDS):
    """Yields messages until every assigned partition reports EOF, the cap is hit, or the broker goes idle."""
    pending = {tp.partition for tp in consumer.assignment()}
    yielded = 0
    last_activity = time.monotonic()

    while pending and (max_messages is None or yielded < max_messages):
        msg = consumer.poll(1.0)
        if msg is None:
            if time.monotonic() - last_activity > idle_timeout:
                break
            continue

        last_activity = time.monotonic()
        error = msg.error()
        if error:
            if error.code() == KafkaError._PARTITION_EOF:
                pending.discard(msg.partition())
                continue
            raise KafkaException(error)

        yield msg
        yielded += 1


def list_and_select_topic(consumer):
    """Lists topics, with interactive search if there are many. Returns the selected topic, if any."""
    try:
        metadata = consumer.list_topics(timeout=10)
        topics = sorted(set(metadata.topics.keys()))
    except KafkaException as e:
        print(f"Error listing topics: {e}", file=sys.stderr)
        return None

    if len(topics) <= 50 or not sys.stdin.isatty():
        print("Available topics:")
        for topic in topics:
            print(f"- {topic}")
        return None

    search_term = ""
    selected = None
    original_settings = termios.tcgetattr(sys.stdin)
    try:
        tty.setraw(sys.stdin.fileno())
        while True:
            filtered = [t for t in topics if search_term in t]

            sys.stdout.write('\x1b[2J\x1b[H')
            sys.stdout.write("--- Interactive Topic Search (Enter to select, Ctrl+C to exit) ---\r\n")
            sys.stdout.write(f"Search: {search_term}\r\n")
            sys.stdout.write("-" * 30 + "\r\n")
            for topic in filtered[:20]:
                sys.stdout.write(f"{topic}\r\n")
            if len(filtered) > 20:
                sys.stdout.write(f"...and {len(filtered) - 20} more.\r\n")
            sys.stdout.flush()

            char = sys.stdin.read(1)
            if char == '\x03':
                break
            if char in ('\r', '\n'):
                if filtered:
                    selected = filtered[0]
                break
            if char == '\x7f':
                search_term = search_term[:-1]
            elif char.isprintable():
                search_term += char
    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, original_settings)

    if selected:
        print(f"Selected topic: {selected}")
    return selected


def get_cluster_overview(admin_client, schema_registry_url, connect_url):
    """Fetches and displays a high-level overview of the Kafka cluster."""
    print("Fetching cluster overview...")

    try:
        metadata = admin_client.list_topics(timeout=10)
        topics = metadata.topics
        topic_count = len(topics)
        partition_count = sum(len(t.partitions) for t in topics.values())
        broker_count = len(metadata.brokers)

        groups_result = admin_client.list_consumer_groups().result()
        group_count = len(groups_result.valid)

        subject_count = "N/A"
        if schema_registry_url:
            try:
                response = requests.get(f"{schema_registry_url.rstrip('/')}/subjects", timeout=5,
                                        auth=http_auth_from_env('KAFKAINSPECT_SCHEMA_REGISTRY_AUTH'))
                response.raise_for_status()
                subject_count = len(response.json())
            except (requests.RequestException, json.JSONDecodeError) as e:
                subject_count = f"Error: {e}"

        connector_count = "N/A"
        if connect_url:
            try:
                response = requests.get(f"{connect_url.rstrip('/')}/connectors", timeout=5,
                                        auth=http_auth_from_env('KAFKAINSPECT_CONNECT_AUTH'))
                response.raise_for_status()
                connector_count = len(response.json())
            except (requests.RequestException, json.JSONDecodeError) as e:
                connector_count = f"Error: {e}"

        print("\n--- Kafka Cluster Overview ---")
        print(f"{'Metric':<20} {'Value':<10}")
        print("-" * 30)
        print(f"{'Topics':<20} {topic_count:<10}")
        print(f"{'Partitions':<20} {partition_count:<10}")
        print(f"{'Brokers':<20} {broker_count:<10}")
        print(f"{'Consumer Groups':<20} {group_count:<10}")
        print(f"{'Subjects':<20} {subject_count:<10}")
        print(f"{'Connectors':<20} {connector_count:<10}")
        print("-" * 30)

    except KafkaException as e:
        print(f"Error fetching cluster overview: {e}", file=sys.stderr)


def check_consumer_lag(consumer, topic, group_id):
    """Checks and prints the consumer lag for a given topic and group."""
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        if metadata.topics.get(topic) is None:
            print(f"Error: Topic '{topic}' not found.", file=sys.stderr)
            return

        partitions = []
        watermarks = {}
        for p in metadata.topics[topic].partitions.values():
            tp = TopicPartition(topic, p.id)
            partitions.append(tp)
            watermarks[p.id] = consumer.get_watermark_offsets(tp, timeout=5)

        committed = consumer.committed(partitions, timeout=5) or []
        committed_map = {tp.partition: tp.offset for tp in committed if tp is not None}

        print(f"Consumer Lag for Group '{group_id}' on Topic '{topic}':")
        print("-" * 60)
        print(f"{'Partition':<12} {'High Watermark':<18} {'Committed Offset':<20} {'Lag':<10}")
        print("-" * 60)

        total_lag = 0
        for p_id, (low, high) in sorted(watermarks.items()):
            committed_offset = committed_map.get(p_id, OFFSET_INVALID)
            has_committed = committed_offset is not None and committed_offset != OFFSET_INVALID
            lag = high - committed_offset if has_committed else high - low
            total_lag += lag
            committed_display = committed_offset if has_committed else 'N/A'
            print(f"{p_id:<12} {high:<18} {committed_display:<20} {lag:<10}")

        print("-" * 60)
        print(f"Total Lag: {total_lag}")

    except KafkaException as e:
        print(f"Error checking lag: {e}", file=sys.stderr)


def peek_messages(consumer, topic, num_messages, max_messages):
    """Peeks at the first N (num_messages negative) or last N (positive) messages of a topic."""
    count = abs(num_messages)
    tail = count if num_messages > 0 else None
    print(f"Peeking at {'last' if num_messages > 0 else 'first'} {count} messages in topic '{topic}'...")

    try:
        assign_partitions(consumer, topic, tail=tail)
    except KafkaException as e:
        print(f"Error: {e}", file=sys.stderr)
        consumer.close()
        return

    buffer = deque(maxlen=count)
    try:
        limit = max_messages if tail is not None else count
        for msg in iter_messages(consumer, max_messages=limit):
            buffer.append(msg)
    finally:
        consumer.close()

    messages = sorted(buffer, key=lambda m: m.timestamp()[1])[-count:] if tail else list(buffer)

    for msg in messages:
        print(f"--- Offset: {msg.offset()}, Partition: {msg.partition()} ---")
        print(f"Key: {decode_key(msg) or 'None'}")
        print(f"Value: {decode_value(msg)}")
        print("-" * (20 + len(str(msg.offset()))))

    print(f"\nDisplayed {len(messages)} messages.")


def search_messages(consumer, topic, pattern, use_regex, max_messages, start):
    """Searches for messages containing a pattern."""
    try:
        assign_partitions(consumer, topic, start=start)
    except KafkaException as e:
        print(f"Error: {e}", file=sys.stderr)
        consumer.close()
        return

    matcher = None
    if use_regex:
        try:
            matcher = re.compile(pattern)
        except re.error as e:
            print(f"Error: invalid regular expression: {e}", file=sys.stderr)
            consumer.close()
            return

    print(f"Searching for pattern '{pattern}' in topic '{topic}'...")
    found_count = 0
    scanned_count = 0

    try:
        for msg in iter_messages(consumer, max_messages=max_messages):
            scanned_count += 1
            value_str = decode_value(msg)

            if matcher.search(value_str) if matcher else pattern in value_str:
                found_count += 1
                print(f"--- Match Found (Offset: {msg.offset()}) ---")
                print(value_str)
                print("-" * (20 + len(str(msg.offset()))))
    finally:
        consumer.close()
        print(f"\nScanned {scanned_count} messages and found {found_count} matches.")


def open_output(spec):
    path, sep, fmt = spec.rpartition(':')
    if not sep or os.sep in fmt:
        path, fmt = spec, 'text'
    fmt = fmt.lower()
    if fmt not in OUTPUT_FORMATS:
        raise ValueError(f"Unknown output format '{fmt}', expected one of: {', '.join(OUTPUT_FORMATS)}")

    handle = open(path, 'w', newline='')
    writer = None
    if fmt == 'csv':
        writer = csv.writer(handle)
        writer.writerow(['timestamp', 'partition', 'offset', 'key', 'value'])
    return handle, fmt, writer


def write_record(handle, fmt, writer, msg):
    _, ts_val = msg.timestamp()
    if fmt == 'jsonl':
        handle.write(json.dumps({
            'timestamp': ts_val,
            'partition': msg.partition(),
            'offset': msg.offset(),
            'key': decode_key(msg),
            'value': decode_value(msg),
        }) + '\n')
    elif fmt == 'csv':
        writer.writerow([ts_val, msg.partition(), msg.offset(), decode_key(msg) or '', decode_value(msg)])
    else:
        handle.write(
            f"Timestamp: {ts_val}, Partition: {msg.partition()}, Offset: {msg.offset()}\n"
            f"Value: {decode_value(msg)}\n---\n"
        )


def deduplicate(consumer, args):
    try:
        assign_partitions(consumer, args.topic, start=args.start)
    except KafkaException as e:
        print(f"Error: {e}", file=sys.stderr)
        consumer.close()
        return

    seen = set()
    db = None
    cursor = None
    if args.sqlite:
        db = sqlite3.connect(args.sqlite)
        cursor = db.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS seen (topic TEXT, hash TEXT, PRIMARY KEY (topic, hash))")
        if not args.sqlite_resume:
            cursor.execute("DELETE FROM seen WHERE topic = ?", (args.topic,))
        db.commit()

    output_file = None
    output_format = 'text'
    csv_writer = None
    if args.output:
        output_file, output_format, csv_writer = open_output(args.output)

    count = 0
    duplicates = 0
    uncommitted = 0

    try:
        for msg in iter_messages(consumer, max_messages=args.max_messages):
            count += 1

            if args.field:
                payload = get_field_from_json(msg.value(), args.field)
            elif args.dedup_by == 'value':
                payload = msg.value()
            else:
                payload = msg.key()

            if payload is None:
                continue

            h = hash_payload(payload)

            if cursor:
                cursor.execute("INSERT OR IGNORE INTO seen (topic, hash) VALUES (?, ?)", (args.topic, h))
                is_duplicate = cursor.rowcount == 0
                uncommitted += 1
                if uncommitted >= SQLITE_COMMIT_INTERVAL:
                    db.commit()
                    uncommitted = 0
            else:
                is_duplicate = h in seen
                seen.add(h)

            if not is_duplicate:
                continue

            duplicates += 1
            if not args.silent:
                print(
                    f"[Duplicate] Offset: {msg.offset()} Partition: {msg.partition()} Timestamp: {msg.timestamp()[1]}\n"
                    f"Value: {decode_value(msg)[:100]}...\n"
                )
            if output_file:
                write_record(output_file, output_format, csv_writer, msg)
    finally:
        consumer.close()
        if db:
            db.commit()
            db.close()
        if output_file:
            output_file.close()

    print(f"Scanned {count} messages, found {duplicates} duplicates.")


def main():
    args = parse_args()

    try:
        extra_config = parse_extra_config(args.config)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if args.overview:
        admin_conf = {'bootstrap.servers': args.bootstrap_servers, **extra_config}
        get_cluster_overview(AdminClient(admin_conf), args.schema_registry_url, args.connect_url)
        return

    consumer = build_consumer(args, extra_config)

    if args.list_topics:
        list_and_select_topic(consumer)
        consumer.close()
        return

    if args.check_lag:
        if not args.topic:
            print("Error: --topic is required when using --check-lag.", file=sys.stderr)
            sys.exit(1)
        check_consumer_lag(consumer, args.topic, args.group_id)
        consumer.close()
        return

    if args.search is not None:
        if not args.topic:
            print("Error: --topic is required when using --search.", file=sys.stderr)
            sys.exit(1)
        search_messages(consumer, args.topic, args.search, args.regex, args.max_messages, args.start)
        return

    if args.peek is not None:
        if not args.topic:
            print("Error: --topic is required when using --peek.", file=sys.stderr)
            sys.exit(1)
        if args.peek == 0:
            print("Error: --peek requires a non-zero number of messages.", file=sys.stderr)
            sys.exit(1)
        peek_messages(consumer, args.topic, args.peek, args.max_messages)
        return

    if not args.topic:
        print("Error: --topic is required for deduplication.", file=sys.stderr)
        sys.exit(1)

    try:
        deduplicate(consumer, args)
    except ValueError as e:
        consumer.close()
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
