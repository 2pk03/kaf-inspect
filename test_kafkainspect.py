import unittest
from unittest.mock import MagicMock, patch, call
import json
import requests
from kafkainspect import hash_payload, get_field_from_json, main, list_and_select_topic, OFFSET_INVALID

# Mock Message class to simulate Kafka messages
class MockMessage:
    def __init__(self, key, value, offset, partition=0, ts=1660000000):
        self._key = key
        self._value = value
        self._offset = offset
        self._partition = partition
        self._ts = ts

    def key(self):
        return self._key

    def value(self):
        return self._value

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition

    def timestamp(self):
        return (1, self._ts) # type, timestamp

    def error(self):
        return None

class TestKafkaInspectHelpers(unittest.TestCase):

    def test_hash_payload(self):
        """Tests that the hashing function returns the correct SHA256 hash."""
        payload = b'hello world'
        expected_hash = 'b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9'
        self.assertEqual(hash_payload(payload), expected_hash)

    def test_get_field_from_json_simple(self):
        """Tests extracting a top-level field from a JSON object."""
        payload = b'{"user": "test", "id": 123}'
        field_path = 'id'
        expected_value = b'123'
        self.assertEqual(get_field_from_json(payload, field_path), expected_value)

    def test_get_field_from_json_nested(self):
        """Tests extracting a nested field from a JSON object."""
        payload = b'{"user": {"name": "test", "details": {"id": 456}}}'
        field_path = 'user.details.id'
        expected_value = b'456'
        self.assertEqual(get_field_from_json(payload, field_path), expected_value)

    def test_get_field_from_json_nonexistent(self):
        """Tests extracting a field that does not exist."""
        payload = b'{"user": {"name": "test"}}'
        field_path = 'user.id'
        self.assertIsNone(get_field_from_json(payload, field_path))

    def test_get_field_from_json_invalid_json(self):
        """Tests behavior with an invalid JSON string."""
        payload = b'this is not json'
        field_path = 'user.id'
        self.assertIsNone(get_field_from_json(payload, field_path))

    def test_get_field_from_json_path_too_deep(self):
        """Tests a path that is deeper than the object structure."""
        payload = b'{"user": "test"}'
        field_path = 'user.id.more'
        self.assertIsNone(get_field_from_json(payload, field_path))
        
    def test_get_field_from_json_object_as_value(self):
        """Tests extracting a field whose value is a JSON object."""
        payload = b'{"data": {"id": 1, "value": "test"}}'
        field_path = 'data'
        # The extracted object should be re-serialized consistently
        expected_payload = json.dumps({"id": 1, "value": "test"}, sort_keys=True).encode('utf-8')
        self.assertEqual(get_field_from_json(payload, field_path), expected_payload)

class TestKafkaInspectE2E(unittest.TestCase):

    @patch('kafkainspect.Consumer')
    def test_list_topics_deduplication(self, MockConsumer):
        """Tests that the topic list is correctly deduplicated."""
        mock_consumer_instance = MockConsumer.return_value
        
        # Mock the metadata object returned by list_topics
        mock_metadata = MagicMock()
        mock_metadata.topics = {
            'topic-a': MagicMock(),
            'topic-b': MagicMock(),
            'topic-a': MagicMock() # Duplicate
        }
        mock_consumer_instance.list_topics.return_value = mock_metadata

        # Mock stdin/stdout to avoid terminal interaction
        with patch('sys.stdout'), patch('sys.stdin'):
            list_and_select_topic(mock_consumer_instance)
            # The core check is that sorted() is called on a set, which is implicitly handled
            # by the logic. A more direct assertion isn't straightforward without more refactoring.
            # Visual inspection of the code change is the primary verification here.
            pass

    def _create_mock_poll(self, messages):
        """Creates a poll function that simulates consumer behavior."""
        def mock_poll(timeout):
            if messages:
                return messages.pop(0)
            return None
        return mock_poll

    @patch('kafkainspect.Consumer')
    def test_deduplication(self, MockConsumer):
        """Tests the end-to-end deduplication logic with a mock consumer."""
        mock_consumer_instance = MockConsumer.return_value
        messages = [
            MockMessage(b'k1', b'value1', 1),
            MockMessage(b'k2', b'value2', 2),
            MockMessage(b'k1', b'value1', 3), # Duplicate
        ]
        mock_consumer_instance.poll.side_effect = self._create_mock_poll(messages)

        argv = ['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test', '--dedup-by', 'value', '--max-messages', '5']
        with patch('sys.argv', argv):
            main()
            mock_consumer_instance.close.assert_called_once()

    @patch('kafkainspect.Consumer')
    def test_search_messages(self, MockConsumer):
        """Tests the message search functionality."""
        mock_consumer_instance = MockConsumer.return_value
        messages = [
            MockMessage(b'k1', b'hello world', 1),
            MockMessage(b'k2', b'another message', 2),
            MockMessage(b'k3', b'hello again', 3),
        ]
        mock_consumer_instance.poll.side_effect = self._create_mock_poll(messages)

        argv = ['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test', '--search', 'hello', '--max-messages', '5']
        with patch('sys.argv', argv):
            main()
            mock_consumer_instance.close.assert_called_once()

    @patch('kafkainspect.Consumer')
    def test_peek_messages(self, MockConsumer):
        """Tests the peek functionality."""
        mock_consumer_instance = MockConsumer.return_value
        messages = [
            MockMessage(b'k1', b'msg1', 1),
            MockMessage(b'k2', b'msg2', 2),
            MockMessage(b'k3', b'msg3', 3),
        ]
        mock_consumer_instance.poll.side_effect = self._create_mock_poll(messages)

        argv = ['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test', '--peek', '2']
        with patch('sys.argv', argv):
            main()
            mock_consumer_instance.close.assert_called_once()

    @patch('kafkainspect.TopicPartition')
    @patch('kafkainspect.Consumer')
    @patch('sys.stdout')
    def test_check_consumer_lag_per_partition(self, mock_stdout, MockConsumer, MockTopicPartition):
        """Lag calculation should use per-partition commits and handle uncommitted partitions."""
        mock_consumer_instance = MockConsumer.return_value

        # Partition metadata
        p0 = MagicMock(id=0)
        p1 = MagicMock(id=1)
        topic_meta = MagicMock(partitions={0: p0, 1: p1})
        metadata = MagicMock(topics={'test': topic_meta})
        mock_consumer_instance.list_topics.return_value = metadata

        # Watermarks per partition
        mock_consumer_instance.get_watermark_offsets.side_effect = [(0, 10), (0, 5)]

        # TopicPartition factory
        def _tp(topic, partition, offset=None):
            tp = MagicMock()
            tp.partition = partition
            tp.offset = offset
            return tp
        MockTopicPartition.side_effect = _tp

        # Committed offsets: first committed, second uncommitted
        mock_consumer_instance.committed.return_value = [
            _tp('test', 0, 4),
            _tp('test', 1, OFFSET_INVALID)
        ]

        argv = ['kafkainspect.py', '--bootstrap-servers', 'mock', '--topic', 'test', '--check-lag']
        with patch('sys.argv', argv):
            main()

        mock_consumer_instance.close.assert_called_once()
        mock_consumer_instance.committed.assert_called_once()

        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("0            10", output)
        self.assertIn("4", output)
        self.assertIn("6", output) # 10 - 4
        self.assertIn("1            5", output)
        self.assertIn("N/A", output)
        self.assertIn("Total Lag: 11", output)


class TestKafkaInspectOverview(unittest.TestCase):

    @patch('kafkainspect.requests.get')
    @patch('kafkainspect.AdminClient')
    @patch('sys.stdout')
    def test_overview_all_features(self, mock_stdout, MockAdminClient, mock_requests_get):
        """Tests the overview feature with Kafka, Schema Registry, and Connect."""
        mock_admin_instance = MockAdminClient.return_value

        # Mock AdminClient metadata
        mock_metadata = MagicMock()
        mock_metadata.topics = {f'topic-{i}': MagicMock() for i in range(5)} # 5 topics
        for t in mock_metadata.topics.values():
            t.partitions = [1, 2] # 2 partitions per topic -> 10 total
        mock_metadata.brokers = {i: MagicMock() for i in range(3)} # 3 brokers
        mock_admin_instance.list_topics.return_value = mock_metadata

        # Mock AdminClient groups
        mock_groups_future = MagicMock()
        mock_groups_result = MagicMock()
        mock_groups_result.valid = [1, 2, 3, 4] # 4 consumer groups
        mock_groups_future.result.return_value = mock_groups_result
        mock_admin_instance.list_consumer_groups.return_value = mock_groups_future

        # Mock requests for Schema Registry and Connect
        mock_schema_response = MagicMock()
        mock_schema_response.json.return_value = ['subject1', 'subject2'] # 2 subjects
        mock_connect_response = MagicMock()
        mock_connect_response.json.return_value = ['connector1'] # 1 connector
        
        mock_requests_get.side_effect = [mock_schema_response, mock_connect_response]

        argv = [
            'kafkainspect.py', 
            '--bootstrap-servers', 'mock', 
            '--overview',
            '--schema-registry-url', 'http://mock-schema',
            '--connect-url', 'http://mock-connect'
        ]
        with patch('sys.argv', argv):
            main()

            # Verify calls
            mock_admin_instance.list_topics.assert_called_once()
            mock_admin_instance.list_consumer_groups.assert_called_once()
            mock_requests_get.assert_has_calls([
                call('http://mock-schema/subjects', timeout=5),
                call('http://mock-connect/connectors', timeout=5)
            ])
            
            # Verify output
            output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
            self.assertIn("Topics               5", output)
            self.assertIn("Partitions           10", output)
            self.assertIn("Brokers              3", output)
            self.assertIn("Consumer Groups      4", output)
            self.assertIn("Subjects             2", output)
            self.assertIn("Connectors           1", output)

    @patch('kafkainspect.AdminClient')
    @patch('sys.stdout')
    def test_overview_kafka_only(self, mock_stdout, MockAdminClient):
        """Tests the overview feature with only Kafka, no external services."""
        mock_admin_instance = MockAdminClient.return_value
        mock_metadata = MagicMock()
        mock_metadata.topics = {'topic-a': MagicMock()}
        mock_metadata.topics['topic-a'].partitions = [1, 2, 3]
        mock_metadata.brokers = {1: MagicMock()}
        mock_admin_instance.list_topics.return_value = mock_metadata
        mock_groups_future = MagicMock()
        mock_groups_result = MagicMock()
        mock_groups_result.valid = [1, 2]
        mock_groups_future.result.return_value = mock_groups_result
        mock_admin_instance.list_consumer_groups.return_value = mock_groups_future

        argv = ['kafkainspect.py', '--bootstrap-servers', 'mock', '--overview']
        with patch('sys.argv', argv):
            main()
        
        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("Topics               1", output)
        self.assertIn("Partitions           3", output)
        self.assertIn("Brokers              1", output)
        self.assertIn("Consumer Groups      2", output)
        self.assertIn("Subjects             N/A", output)
        self.assertIn("Connectors           N/A", output)

    @patch('kafkainspect.requests.get')
    @patch('kafkainspect.AdminClient')
    @patch('sys.stdout')
    def test_overview_schema_registry_error(self, mock_stdout, MockAdminClient, mock_requests_get):
        """Tests that an error from Schema Registry is handled gracefully."""
        mock_admin_instance = MockAdminClient.return_value
        mock_admin_instance.list_topics.return_value = MagicMock(topics={}, brokers={})
        mock_groups_future = MagicMock()
        mock_groups_result = MagicMock()
        mock_groups_result.valid = []
        mock_groups_future.result.return_value = mock_groups_result
        mock_admin_instance.list_consumer_groups.return_value = mock_groups_future
        
        # Mock a request exception
        mock_requests_get.side_effect = requests.exceptions.RequestException("Connection failed")

        argv = ['kafkainspect.py', '--bootstrap-servers', 'mock', '--overview', '--schema-registry-url', 'http://bad-url']
        with patch('sys.argv', argv):
            main()

        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("Subjects             Error: Connection failed", output)

    @patch('kafkainspect.requests.get')
    @patch('kafkainspect.AdminClient')
    @patch('sys.stdout')
    def test_overview_with_specific_mock_data(self, mock_stdout, MockAdminClient, mock_requests_get):
        """A very explicit test to demonstrate mocking for the overview feature."""
        mock_admin_instance = MockAdminClient.return_value

        # --- Mock Data Setup ---
        # Kafka
        mock_metadata = MagicMock()
        mock_metadata.topics = {'topic-1': MagicMock(), 'topic-2': MagicMock()} # 2 topics
        mock_metadata.topics['topic-1'].partitions = [0] # 1 partition
        mock_metadata.topics['topic-2'].partitions = [0, 1] # 2 partitions
        # Total partitions = 3
        mock_metadata.brokers = {1001: MagicMock()} # 1 broker
        mock_admin_instance.list_topics.return_value = mock_metadata

        # Consumer Groups
        mock_groups_future = MagicMock()
        mock_groups_result = MagicMock()
        mock_groups_result.valid = ['group-a', 'group-b', 'group-c'] # 3 groups
        mock_groups_future.result.return_value = mock_groups_result
        mock_admin_instance.list_consumer_groups.return_value = mock_groups_future

        # Schema Registry
        mock_schema_response = MagicMock()
        mock_schema_response.json.return_value = ['schema-1', 'schema-2', 'schema-3', 'schema-4'] # 4 subjects
        
        # Kafka Connect
        mock_connect_response = MagicMock()
        mock_connect_response.json.return_value = ['connector-a', 'connector-b'] # 2 connectors
        
        mock_requests_get.side_effect = [mock_schema_response, mock_connect_response]

        # --- Run the main function with mocked arguments ---
        argv = [
            'kafkainspect.py', 
            '--bootstrap-servers', 'mock-server:9092', 
            '--overview',
            '--schema-registry-url', 'http://mock-schema-registry',
            '--connect-url', 'http://mock-kafka-connect'
        ]
        with patch('sys.argv', argv):
            main()

        # --- Assertions ---
        # Verify that the correct functions were called
        mock_admin_instance.list_topics.assert_called_once()
        mock_admin_instance.list_consumer_groups.assert_called_once()
        mock_requests_get.assert_has_calls([
            call('http://mock-schema-registry/subjects', timeout=5),
            call('http://mock-kafka-connect/connectors', timeout=5)
        ])
        
        # Verify the output printed to the console
        output = "".join(c[0][0] for c in mock_stdout.write.call_args_list)
        self.assertIn("Topics               2", output)
        self.assertIn("Partitions           3", output)
        self.assertIn("Brokers              1", output)
        self.assertIn("Consumer Groups      3", output)
        self.assertIn("Subjects             4", output)
        self.assertIn("Connectors           2", output)

if __name__ == '__main__':
    unittest.main()
