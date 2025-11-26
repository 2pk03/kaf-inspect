#!/usr/bin/env python3
import argparse
from confluent_kafka import Consumer, KafkaException, TopicPartition, OFFSET_INVALID
from confluent_kafka.admin import AdminClient
import hashlib
import requests
import json
import sqlite3
import sys
import os
import tty
import termios
import re

def parse_args():
    parser = argparse.ArgumentParser(description="Kafka topic inspector and deduplication tool.")
    parser.add_argument('--bootstrap-servers', required=True, help='Comma-separated list of Kafka bootstrap servers')
    parser.add_argument('--topic', help='Kafka topic to scan. Not required if --list-topics or --check-lag is used.')
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
    parser.add_argument('--output', help='Optional path to output file (e.g., out.txt:text, out.jsonl:jsonl, out.csv:csv)')
    parser.add_argument('--silent', action='store_true', help='Suppress stdout output of duplicates')
    return parser.parse_args()

def hash_payload(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()

def get_field_from_json(payload: bytes, field_path: str):
    """Extracts a nested value from a JSON payload using dot notation."""
    try:
        data = json.loads(payload)
        keys = field_path.split('.')
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key)
            else:
                return None # Path is longer than the object depth
            if data is None:
                return None # Key not found
        # Re-serialize to get a consistent byte representation
        return json.dumps(data, sort_keys=True).encode('utf-8')
    except (json.JSONDecodeError, AttributeError):
        return None

def list_and_select_topic(consumer):
    """Lists topics, with interactive search if there are many."""
    try:
        metadata = consumer.list_topics(timeout=10)
        topics = sorted(list(set(metadata.topics.keys())))

        if len(topics) <= 50:
            print("Available topics:")
            for topic in topics:
                print(f"- {topic}")
            return

        # --- Interactive Mode ---
        search_term = ""
        original_settings = termios.tcgetattr(sys.stdin)
        try:
            tty.setraw(sys.stdin.fileno())
            while True:
                # Redraw screen
                os.system('cls' if os.name == 'nt' else 'clear')
                print("--- Interactive Topic Search (Ctrl+C to exit) ---")
                
                filtered_topics = [t for t in topics if search_term in t]
                
                print(f"Search: {search_term}", end='\r\n')
                print("-" * 30)

                for topic in filtered_topics[:20]:
                    print(topic)
                if len(filtered_topics) > 20:
                    print(f"...and {len(filtered_topics) - 20} more.")

                # Get next character
                char = sys.stdin.read(1)
                if char == '\x03': # Ctrl+C
                    break
                elif char == '\x7f': # Backspace
                    search_term = search_term[:-1]
                elif char.isprintable():
                    search_term += char
        finally:
            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, original_settings)

    except KafkaException as e:
        print(f"Error listing topics: {e}")
    except Exception as e:
        # Restore terminal settings on any other error
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, original_settings)
        print(f"An unexpected error occurred: {e}")

def get_cluster_overview(admin_client, schema_registry_url, connect_url):
    """Fetches and displays a high-level overview of the Kafka cluster."""
    print("Fetching cluster overview...")
    
    try:
        # Basic cluster metadata
        metadata = admin_client.list_topics(timeout=10)
        topics = metadata.topics
        topic_count = len(topics)
        partition_count = sum(len(t.partitions) for t in topics.values())
        broker_count = len(metadata.brokers)

        # Consumer groups
        groups_future = admin_client.list_consumer_groups()
        groups_result = groups_future.result()
        group_count = len(groups_result.valid)

        # Schema Registry Subjects
        subject_count = "N/A"
        if schema_registry_url:
            try:
                response = requests.get(f"{schema_registry_url}/subjects", timeout=5)
                response.raise_for_status()
                subject_count = len(response.json())
            except (requests.RequestException, json.JSONDecodeError) as e:
                subject_count = f"Error: {e}"

        # Kafka Connect Connectors
        connector_count = "N/A"
        if connect_url:
            try:
                response = requests.get(f"{connect_url}/connectors", timeout=5)
                response.raise_for_status()
                connector_count = len(response.json())
            except (requests.RequestException, json.JSONDecodeError) as e:
                connector_count = f"Error: {e}"

        # Display results
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
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)


def check_consumer_lag(consumer, topic, group_id):
    """Checks and prints the consumer lag for a given topic and group."""
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        if metadata.topics.get(topic) is None:
            print(f"Error: Topic '{topic}' not found.", file=sys.stderr)
            return

        # Collect watermark offsets per partition
        partitions = []
        watermarks = {}
        for p in metadata.topics[topic].partitions.values():
            tp = TopicPartition(topic, p.id)
            partitions.append(tp)
            low, high = consumer.get_watermark_offsets(tp, timeout=5)
            watermarks[tp.partition] = (low, high)

        committed = consumer.committed(partitions, timeout=5) or []
        committed_map = {tp.partition: tp.offset for tp in committed if tp is not None}

        print(f"Consumer Lag for Group '{group_id}' on Topic '{topic}':")
        print("-" * 60)
        print(f"{'Partition':<12} {'High Watermark':<18} {'Committed Offset':<20} {'Lag':<10}")
        print("-" * 60)

        total_lag = 0
        for p_id, (low, high) in watermarks.items():
            committed_offset = committed_map.get(p_id, OFFSET_INVALID)
            has_committed = committed_offset is not None and committed_offset != OFFSET_INVALID
            lag = high - committed_offset if has_committed else high
            total_lag += lag
            committed_display = committed_offset if has_committed else 'N/A'
            print(f"{p_id:<12} {high:<18} {committed_display:<20} {lag:<10}")
        
        print("-" * 60)
        print(f"Total Lag: {total_lag}")

    except KafkaException as e:
        print(f"Error checking lag: {e}", file=sys.stderr)

def peek_messages(consumer, topic, num_messages):
    """Peeks at the first or last N messages of a topic."""
    print(f"Peeking at {'last' if num_messages > 0 else 'first'} {abs(num_messages)} messages in topic '{topic}'...")
    consumer.subscribe([topic])
    
    messages = []
    # For simplicity, we'll just consume and store. A more advanced implementation
    # would seek to the correct offsets before consuming for "last N".
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: break
            if msg.error(): raise KafkaException(msg.error())
            messages.append(msg)
            if len(messages) >= abs(num_messages) and num_messages < 0:
                break # Stop after N for "first N"
    finally:
        consumer.close()

    if num_messages > 0: # Last N
        messages_to_show = messages[-num_messages:]
    else: # First N
        messages_to_show = messages

    for msg in messages_to_show:
        print(f"--- Offset: {msg.offset()}, Partition: {msg.partition()} ---")
        print(f"Key: {msg.key().decode(errors='ignore') if msg.key() else 'None'}")
        print(f"Value: {msg.value().decode(errors='ignore')}")
        print("-" * (20 + len(str(msg.offset()))))
    
    print(f"\nDisplayed {len(messages_to_show)} messages.")


def search_messages(consumer, topic, pattern, use_regex, max_messages):
    """Searches for messages containing a pattern."""
    consumer.subscribe([topic])
    
    print(f"Searching for pattern '{pattern}' in topic '{topic}'...")
    found_count = 0
    scanned_count = 0

    try:
        while scanned_count < max_messages:
            msg = consumer.poll(1.0)
            if msg is None:
                break # End of topic
            if msg.error(): raise KafkaException(msg.error())

            scanned_count += 1
            value_str = msg.value().decode(errors='ignore')
            
            match = False
            if use_regex:
                if re.search(pattern, value_str):
                    match = True
            else:
                if pattern in value_str:
                    match = True
            
            if match:
                found_count += 1
                print(f"--- Match Found (Offset: {msg.offset()}) ---")
                print(value_str)
                print("-" * (20 + len(str(msg.offset()))))

    finally:
        consumer.close()
        print(f"\nScanned {scanned_count} messages and found {found_count} matches.")


def main():
    args = parse_args()
    
    conf = {'bootstrap.servers': args.bootstrap_servers}
    
    if args.overview:
        admin_client = AdminClient(conf)
        get_cluster_overview(admin_client, args.schema_registry_url, args.connect_url)
        return

    # For other operations, initialize the consumer
    consumer_conf = {**conf, 'group.id': args.group_id, 'auto.offset.reset': args.start}
    consumer = Consumer(consumer_conf)

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

    if args.search:
        if not args.topic:
            print("Error: --topic is required when using --search.", file=sys.stderr)
            sys.exit(1)
        search_messages(consumer, args.topic, args.search, args.regex, args.max_messages)
        return

    if args.peek:
        if not args.topic:
            print("Error: --topic is required when using --peek.", file=sys.stderr)
            sys.exit(1)
        peek_messages(consumer, args.topic, args.peek)
        return

    if not args.topic:
        print("Error: --topic is required for deduplication.", file=sys.stderr)
        sys.exit(1)

    consumer.subscribe([args.topic])

    seen = set()
    db = None
    cursor = None
    if args.sqlite:
        db = sqlite3.connect(args.sqlite)
        cursor = db.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS seen (hash TEXT PRIMARY KEY)")

    output_file = None
    output_format = 'text'
    csv_writer = None
    if args.output:
        parts = args.output.split(':')
        path = parts[0]
        if len(parts) > 1:
            output_format = parts[1].lower()
        
        output_file = open(path, 'w', newline='')
        if output_format == 'csv':
            import csv
            csv_writer = csv.writer(output_file)
            csv_writer.writerow(['timestamp', 'partition', 'offset', 'key', 'value'])

    
    count = 0
    duplicates = 0

    try:
        while count < args.max_messages:
            msg = consumer.poll(1.0)
            if msg is None:
                break # End of topic
            if msg.error(): raise KafkaException(msg.error())

            payload = None
            if args.field:
                payload = get_field_from_json(msg.value(), args.field)
            elif args.dedup_by == 'value':
                payload = msg.value()
            else: # key
                payload = msg.key()

            if payload is None:
                continue
            
            h = hash_payload(payload)
            is_duplicate = False
            
            if args.sqlite:
                cursor.execute("SELECT 1 FROM seen WHERE hash = ?", (h,))
                if cursor.fetchone():
                    is_duplicate = True
                else:
                    cursor.execute("INSERT INTO seen (hash) VALUES (?)", (h,))
                    db.commit()
            else:
                if h in seen:
                    is_duplicate = True
                else:
                    seen.add(h)

            if is_duplicate:
                duplicates += 1
                if not args.silent:
                    print(
                        f"[Duplicate] Offset: {msg.offset()} Partition: {msg.partition()} Timestamp: {msg.timestamp()[1]}\n"
                        f"Value: {msg.value().decode(errors='ignore')[:100]}...\n"
                    )
                
                if output_file:
                    ts_type, ts_val = msg.timestamp()
                    if output_format == 'jsonl':
                        record = {
                            'timestamp': ts_val,
                            'partition': msg.partition(),
                            'offset': msg.offset(),
                            'key': msg.key().decode(errors='ignore') if msg.key() else None,
                            'value': msg.value().decode(errors='ignore')
                        }
                        output_file.write(json.dumps(record) + '\n')
                    elif output_format == 'csv' and csv_writer:
                        csv_writer.writerow([
                            ts_val,
                            msg.partition(),
                            msg.offset(),
                            msg.key().decode(errors='ignore') if msg.key() else '',
                            msg.value().decode(errors='ignore')
                        ])
                    else: # Plain text
                        output_file.write(
                            f"Timestamp: {ts_val}, Partition: {msg.partition()}, Offset: {msg.offset()}\n"
                            f"Value: {msg.value().decode(errors='ignore')}\n---\n"
                        )

            count += 1
    finally:
        consumer.close()
        if db: db.close()
        if output_file: output_file.close()

    print(f"Scanned {count} messages, found {duplicates} duplicates.")

if __name__ == "__main__":
    main()
