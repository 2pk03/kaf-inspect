import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import MagicMock, patch, call

import requests
from confluent_kafka import KafkaError, TopicPartition

from kafkainspect import (
    OFFSET_INVALID,
    decode_key,
    decode_value,
    get_field_from_json,
    hash_payload,
    http_auth_from_env,
    iter_messages,
    list_and_select_topic,
    main,
    open_output,
    parse_extra_config,
)


class MockError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

    def __str__(self):
        return f"error({self._code})"


class MockMessage:
    def __init__(self, key, value, offset, partition=0, ts=1660000000, error=None):
        self._key = key
        self._value = value
        self._offset = offset
        self._partition = partition
        self._ts = ts
        self._error = error

    def key(self):
        return self._key

    def value(self):
        return self._value

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition

    def timestamp(self):
        return (1, self._ts)

    def error(self):
        return self._error


def eof_message(partition):
    return MockMessage(None, None, -1, partition, error=MockError(KafkaError._PARTITION_EOF))


class FakeConsumer:
    """Models assign/poll/EOF semantics so the consume paths are exercised for real."""

    def __init__(self, messages_by_partition, watermarks, topic='test', committed_offsets=None):
        self.messages_by_partition = messages_by_partition
        self.watermarks = watermarks
        self.topic = topic
        self.committed_offsets = committed_offsets or {}
        self.assigned = None
        self.subscribed = None
        self.closed = False
        self.idle_polls = 0
        self._queue = []

    def list_topics(self, topic=None, timeout=None):
        partitions = {p: MagicMock(id=p) for p in self.watermarks}
        topic_meta = MagicMock(partitions=partitions)
        topic_meta.error = None
        return MagicMock(topics={self.topic: topic_meta})

    def get_watermark_offsets(self, tp, timeout=None):
        return self.watermarks[tp.partition]

    def subscribe(self, topics):
        self.subscribed = topics

    def assign(self, partitions):
        self.assigned = partitions
        self._queue = []
        for tp in partitions:
            for msg in self.messages_by_partition.get(tp.partition, []):
                if msg.offset() >= tp.offset:
                    self._queue.append(msg)
            self._queue.append(eof_message(tp.partition))

    def assignment(self):
        return self.assigned or []

    def poll(self, timeout):
        if self.idle_polls:
            self.idle_polls -= 1
            return None
        return self._queue.pop(0) if self._queue else None

    def committed(self, partitions, timeout=None):
        return [TopicPartition(self.topic, tp.partition, self.committed_offsets.get(tp.partition, OFFSET_INVALID))
                for tp in partitions]

    def close(self):
        self.closed = True


def single_partition(messages, high=None, low=0):
    high = len(messages) if high is None else high
    return FakeConsumer({0: messages}, {0: (low, high)})


def run_main(argv, consumer):
    with patch('kafkainspect.Consumer', return_value=consumer) as MockConsumer, patch('sys.argv', argv):
        main()
    return MockConsumer


class TestHelpers(unittest.TestCase):

    def test_hash_payload(self):
        self.assertEqual(hash_payload(b'hello world'),
                         'b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9')

    def test_get_field_from_json_simple(self):
        self.assertEqual(get_field_from_json(b'{"user": "test", "id": 123}', 'id'), b'123')

    def test_get_field_from_json_nested(self):
        payload = b'{"user": {"name": "test", "details": {"id": 456}}}'
        self.assertEqual(get_field_from_json(payload, 'user.details.id'), b'456')

    def test_get_field_from_json_nonexistent(self):
        self.assertIsNone(get_field_from_json(b'{"user": {"name": "test"}}', 'user.id'))

    def test_get_field_from_json_invalid_json(self):
        self.assertIsNone(get_field_from_json(b'this is not json', 'user.id'))

    def test_get_field_from_json_path_too_deep(self):
        self.assertIsNone(get_field_from_json(b'{"user": "test"}', 'user.id.more'))

    def test_get_field_from_json_object_as_value(self):
        payload = b'{"data": {"id": 1, "value": "test"}}'
        expected = json.dumps({"id": 1, "value": "test"}, sort_keys=True).encode('utf-8')
        self.assertEqual(get_field_from_json(payload, 'data'), expected)

    def test_get_field_from_json_tombstone(self):
        """A null message value must not raise TypeError."""
        self.assertIsNone(get_field_from_json(None, 'user.id'))

    def test_decode_handles_null_payloads(self):
        msg = MockMessage(None, None, 1)
        self.assertEqual(decode_value(msg), '')
        self.assertIsNone(decode_key(msg))

    def test_parse_extra_config(self):
        config = parse_extra_config(['security.protocol=SASL_SSL', 'sasl.password=a=b'])
        self.assertEqual(config, {'security.protocol': 'SASL_SSL', 'sasl.password': 'a=b'})

    def test_parse_extra_config_rejects_malformed(self):
        with self.assertRaises(ValueError):
            parse_extra_config(['security.protocol'])

    def test_http_auth_from_env(self):
        with patch.dict(os.environ, {'KI_AUTH': 'user:secret'}):
            self.assertEqual(http_auth_from_env('KI_AUTH'), ('user', 'secret'))
        with patch.dict(os.environ, {'KI_AUTH': 'nocolon'}):
            self.assertIsNone(http_auth_from_env('KI_AUTH'))
        self.assertIsNone(http_auth_from_env('KI_AUTH_MISSING'))


class TestIterMessages(unittest.TestCase):

    def test_stops_at_partition_eof(self):
        consumer = single_partition([MockMessage(b'k', b'v', i) for i in range(3)])
        consumer.assign([TopicPartition('test', 0, 0)])
        self.assertEqual(len(list(iter_messages(consumer))), 3)

    def test_survives_poll_timeout_mid_stream(self):
        """A None from poll() is a timeout, not end-of-topic."""
        consumer = single_partition([MockMessage(b'k', b'v', i) for i in range(3)])
        consumer.assign([TopicPartition('test', 0, 0)])
        consumer.idle_polls = 2
        self.assertEqual(len(list(iter_messages(consumer))), 3)

    def test_gives_up_when_broker_stays_idle(self):
        consumer = single_partition([])
        consumer.assign([TopicPartition('test', 0, 0)])
        consumer._queue = []
        consumer.idle_polls = 10
        with patch('kafkainspect.time.monotonic', side_effect=[0, 100, 200]):
            self.assertEqual(list(iter_messages(consumer, idle_timeout=1)), [])

    def test_honours_max_messages(self):
        consumer = single_partition([MockMessage(b'k', b'v', i) for i in range(10)])
        consumer.assign([TopicPartition('test', 0, 0)])
        self.assertEqual(len(list(iter_messages(consumer, max_messages=4))), 4)


class TestConsumerSafety(unittest.TestCase):

    @patch('sys.stdout')
    def test_never_joins_the_consumer_group(self, mock_stdout):
        """Read paths must assign partitions directly and never auto-commit."""
        consumer = single_partition([MockMessage(b'k', b'v', 0)])
        argv = ['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test',
                '--group-id', 'production-group']
        MockConsumer = run_main(argv, consumer)

        conf = MockConsumer.call_args[0][0]
        self.assertFalse(conf['enable.auto.commit'])
        self.assertTrue(conf['enable.partition.eof'])
        self.assertIsNone(consumer.subscribed)
        self.assertIsNotNone(consumer.assigned)
        self.assertTrue(consumer.closed)

    @patch('sys.stdout')
    def test_extra_config_reaches_the_client(self, mock_stdout):
        consumer = single_partition([])
        argv = ['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test',
                '-X', 'security.protocol=SASL_SSL', '-X', 'sasl.mechanism=SCRAM-SHA-512']
        MockConsumer = run_main(argv, consumer)

        conf = MockConsumer.call_args[0][0]
        self.assertEqual(conf['security.protocol'], 'SASL_SSL')
        self.assertEqual(conf['sasl.mechanism'], 'SCRAM-SHA-512')


class TestDeduplication(unittest.TestCase):

    @patch('sys.stdout')
    def test_deduplication(self, mock_stdout):
        messages = [
            MockMessage(b'k1', b'value1', 0),
            MockMessage(b'k2', b'value2', 1),
            MockMessage(b'k1', b'value1', 2),
        ]
        run_main(['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test'],
                 single_partition(messages))
        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("Scanned 3 messages, found 1 duplicates.", output)

    @patch('sys.stdout')
    def test_tombstone_does_not_crash_dedup_by_key(self, mock_stdout):
        """A duplicate key whose value is null must not raise AttributeError."""
        messages = [
            MockMessage(b'k1', b'value1', 0),
            MockMessage(b'k1', None, 1),
        ]
        run_main(['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test',
                  '--dedup-by', 'key'], single_partition(messages))
        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("Scanned 2 messages, found 1 duplicates.", output)

    @patch('sys.stdout')
    def test_skipped_payloads_count_toward_max_messages(self, mock_stdout):
        """Messages with no extractable payload must still consume the budget."""
        messages = [MockMessage(b'k', b'not-json', i) for i in range(50)]
        run_main(['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test',
                  '--field', 'a.b', '--max-messages', '3'], single_partition(messages))
        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("Scanned 3 messages", output)


class TestSqliteBackend(unittest.TestCase):

    def setUp(self):
        self.db_path = os.path.join(tempfile.mkdtemp(), 'dedup.db')

    def _run(self, extra=()):
        messages = [MockMessage(b'k1', b'value1', 0), MockMessage(b'k2', b'value2', 1)]
        with patch('sys.stdout') as mock_stdout:
            run_main(['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test',
                      '--sqlite', self.db_path, *extra], single_partition(messages))
            return "".join(c[0][0] for c in mock_stdout.write.call_args_list)

    def test_second_run_is_not_all_duplicates(self):
        """Stale hashes from an earlier run must not poison a fresh scan."""
        self.assertIn("found 0 duplicates", self._run())
        self.assertIn("found 0 duplicates", self._run())

    def test_resume_keeps_previous_hashes(self):
        self.assertIn("found 0 duplicates", self._run())
        self.assertIn("found 2 duplicates", self._run(extra=['--sqlite-resume']))

    def test_hashes_are_scoped_by_topic(self):
        self._run()
        with sqlite3.connect(self.db_path) as db:
            topics = {row[0] for row in db.execute("SELECT topic FROM seen")}
        self.assertEqual(topics, {'test'})


class TestSearch(unittest.TestCase):

    @patch('sys.stdout')
    def test_search_messages(self, mock_stdout):
        messages = [
            MockMessage(b'k1', b'hello world', 0),
            MockMessage(b'k2', b'another message', 1),
            MockMessage(b'k3', b'hello again', 2),
        ]
        run_main(['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test',
                  '--search', 'hello'], single_partition(messages))
        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("Scanned 3 messages and found 2 matches.", output)

    @patch('sys.stdout')
    def test_search_skips_null_values(self, mock_stdout):
        messages = [MockMessage(b'k', None, 0), MockMessage(b'k', b'hello', 1)]
        run_main(['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test',
                  '--search', 'hello'], single_partition(messages))
        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("Scanned 2 messages and found 1 matches.", output)

    @patch('sys.stderr')
    def test_invalid_regex_reports_cleanly(self, mock_stderr):
        run_main(['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test',
                  '--search', '[unclosed', '--regex'], single_partition([]))
        output = "".join(c[0][0] for c in mock_stderr.write.call_args_list)
        self.assertIn("invalid regular expression", output)


class TestPeek(unittest.TestCase):

    @patch('sys.stdout')
    def test_peek_last_n_seeks_instead_of_draining(self, mock_stdout):
        """Last-N must start near the high watermark, not consume the whole topic."""
        messages = [MockMessage(b'k', b'v%d' % i, i, ts=1000 + i) for i in range(10)]
        consumer = single_partition(messages, high=10)
        run_main(['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test',
                  '--peek', '3'], consumer)

        self.assertEqual(consumer.assigned[0].offset, 7)
        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("Displayed 3 messages.", output)
        self.assertIn("Offset: 9", output)
        self.assertNotIn("Offset: 6", output)

    @patch('sys.stdout')
    def test_peek_first_n(self, mock_stdout):
        messages = [MockMessage(b'k', b'v%d' % i, i, ts=1000 + i) for i in range(10)]
        consumer = single_partition(messages, high=10)
        run_main(['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test',
                  '--peek', '-3'], consumer)

        self.assertEqual(consumer.assigned[0].offset, 0)
        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("Displayed 3 messages.", output)
        self.assertIn("Offset: 0", output)
        self.assertNotIn("Offset: 3", output)

    @patch('sys.stdout')
    def test_peek_tolerates_short_partition(self, mock_stdout):
        messages = [MockMessage(b'k', b'v', 0, ts=1000)]
        consumer = single_partition(messages, high=1)
        run_main(['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test',
                  '--peek', '5'], consumer)
        self.assertEqual(consumer.assigned[0].offset, 0)

    @patch('sys.stderr')
    def test_peek_zero_is_rejected(self, mock_stderr):
        """--peek 0 must not silently fall through to deduplication."""
        with self.assertRaises(SystemExit):
            run_main(['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test',
                      '--peek', '0'], single_partition([]))
        output = "".join(c[0][0] for c in mock_stderr.write.call_args_list)
        self.assertIn("non-zero", output)


class TestOutput(unittest.TestCase):

    def test_open_output_rejects_unknown_format(self):
        path = os.path.join(tempfile.mkdtemp(), 'out.txt')
        with self.assertRaises(ValueError):
            open_output(f'{path}:xml')

    def test_open_output_defaults_to_text_without_suffix(self):
        path = os.path.join(tempfile.mkdtemp(), 'out.txt')
        handle, fmt, writer = open_output(path)
        handle.close()
        self.assertEqual(fmt, 'text')
        self.assertIsNone(writer)

    def test_open_output_parses_format(self):
        path = os.path.join(tempfile.mkdtemp(), 'out.csv')
        handle, fmt, writer = open_output(f'{path}:csv')
        handle.close()
        self.assertEqual(fmt, 'csv')
        self.assertIsNotNone(writer)

    @patch('sys.stdout')
    def test_jsonl_output_handles_null_key_and_value(self, mock_stdout):
        path = os.path.join(tempfile.mkdtemp(), 'out.jsonl')
        messages = [MockMessage(None, None, 0), MockMessage(None, None, 1)]
        run_main(['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test',
                  '--dedup-by', 'key', '--output', f'{path}:jsonl'], single_partition(messages))
        self.assertEqual(os.path.getsize(path), 0)


class TestConsumerLag(unittest.TestCase):

    @patch('sys.stdout')
    def test_lag_per_partition(self, mock_stdout):
        consumer = FakeConsumer({}, {0: (0, 10), 1: (0, 5)}, committed_offsets={0: 4})
        run_main(['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test', '--check-lag'],
                 consumer)

        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("N/A", output)
        self.assertIn("Total Lag: 11", output)
        self.assertTrue(consumer.closed)

    @patch('sys.stdout')
    def test_uncommitted_lag_excludes_deleted_messages(self, mock_stdout):
        """With no commit, lag is high - low so retention-deleted records are not counted."""
        consumer = FakeConsumer({}, {0: (100, 150)})
        run_main(['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test', '--check-lag'],
                 consumer)
        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("Total Lag: 50", output)


class TestListTopics(unittest.TestCase):

    @patch('sys.stdout')
    def test_topics_are_deduplicated_and_sorted(self, mock_stdout):
        consumer = MagicMock()
        consumer.list_topics.return_value = MagicMock(topics={'topic-b': MagicMock(), 'topic-a': MagicMock()})
        with patch('sys.stdin') as mock_stdin:
            mock_stdin.isatty.return_value = False
            list_and_select_topic(consumer)
        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertLess(output.index('topic-a'), output.index('topic-b'))

    @patch('sys.stdout')
    def test_non_tty_falls_back_to_plain_listing(self, mock_stdout):
        """Piped stdin with many topics must not enter raw mode or raise."""
        consumer = MagicMock()
        consumer.list_topics.return_value = MagicMock(topics={f'topic-{i:03d}': MagicMock() for i in range(60)})
        with patch('sys.stdin') as mock_stdin, patch('kafkainspect.termios.tcgetattr') as mock_tcgetattr:
            mock_stdin.isatty.return_value = False
            self.assertIsNone(list_and_select_topic(consumer))
            mock_tcgetattr.assert_not_called()
        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn('topic-059', output)


class TestOverview(unittest.TestCase):

    def _admin(self, MockAdminClient, topics, brokers, groups):
        instance = MockAdminClient.return_value
        metadata = MagicMock()
        metadata.topics = topics
        metadata.brokers = brokers
        instance.list_topics.return_value = metadata
        future = MagicMock()
        future.result.return_value = MagicMock(valid=groups)
        instance.list_consumer_groups.return_value = future
        return instance

    @patch('kafkainspect.requests.get')
    @patch('kafkainspect.AdminClient')
    @patch('sys.stdout')
    def test_overview_all_features(self, mock_stdout, MockAdminClient, mock_requests_get):
        topics = {f'topic-{i}': MagicMock() for i in range(5)}
        for t in topics.values():
            t.partitions = [1, 2]
        instance = self._admin(MockAdminClient, topics, {i: MagicMock() for i in range(3)}, [1, 2, 3, 4])

        mock_requests_get.side_effect = [
            MagicMock(json=MagicMock(return_value=['subject1', 'subject2'])),
            MagicMock(json=MagicMock(return_value=['connector1'])),
        ]

        argv = ['kafkainspect.py', '--bootstrap-servers', 'mock', '--overview',
                '--schema-registry-url', 'http://mock-schema',
                '--connect-url', 'http://mock-connect']
        with patch('sys.argv', argv):
            main()

        instance.list_topics.assert_called_once()
        instance.list_consumer_groups.assert_called_once()
        mock_requests_get.assert_has_calls([
            call('http://mock-schema/subjects', timeout=5, auth=None),
            call('http://mock-connect/connectors', timeout=5, auth=None),
        ])

        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("Topics               5", output)
        self.assertIn("Partitions           10", output)
        self.assertIn("Brokers              3", output)
        self.assertIn("Consumer Groups      4", output)
        self.assertIn("Subjects             2", output)
        self.assertIn("Connectors           1", output)

    @patch('kafkainspect.requests.get')
    @patch('kafkainspect.AdminClient')
    @patch('sys.stdout')
    def test_overview_trailing_slash_and_auth(self, mock_stdout, MockAdminClient, mock_requests_get):
        self._admin(MockAdminClient, {}, {}, [])
        mock_requests_get.return_value = MagicMock(json=MagicMock(return_value=[]))

        argv = ['kafkainspect.py', '--bootstrap-servers', 'mock', '--overview',
                '--schema-registry-url', 'http://mock-schema/']
        with patch.dict(os.environ, {'KAFKAINSPECT_SCHEMA_REGISTRY_AUTH': 'user:secret'}), \
                patch('sys.argv', argv):
            main()

        mock_requests_get.assert_called_once_with(
            'http://mock-schema/subjects', timeout=5, auth=('user', 'secret'))

    @patch('kafkainspect.AdminClient')
    @patch('sys.stdout')
    def test_overview_kafka_only(self, mock_stdout, MockAdminClient):
        topics = {'topic-a': MagicMock()}
        topics['topic-a'].partitions = [1, 2, 3]
        self._admin(MockAdminClient, topics, {1: MagicMock()}, [1, 2])

        with patch('sys.argv', ['kafkainspect.py', '--bootstrap-servers', 'mock', '--overview']):
            main()

        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("Topics               1", output)
        self.assertIn("Partitions           3", output)
        self.assertIn("Subjects             N/A", output)
        self.assertIn("Connectors           N/A", output)

    @patch('kafkainspect.requests.get')
    @patch('kafkainspect.AdminClient')
    @patch('sys.stdout')
    def test_overview_schema_registry_error(self, mock_stdout, MockAdminClient, mock_requests_get):
        self._admin(MockAdminClient, {}, {}, [])
        mock_requests_get.side_effect = requests.exceptions.RequestException("Connection failed")

        argv = ['kafkainspect.py', '--bootstrap-servers', 'mock', '--overview',
                '--schema-registry-url', 'http://bad-url']
        with patch('sys.argv', argv):
            main()

        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("Subjects             Error: Connection failed", output)


if __name__ == '__main__':
    unittest.main()
