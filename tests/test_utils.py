"""Tests for utility functions in langgraph_checkpoint_s3.utils."""

from unittest.mock import MagicMock

from langgraph_checkpoint_s3.utils import (
    denormalize_checkpoint_ns,
    deserialize_checkpoint_data,
    deserialize_write_data,
    get_checkpoint_key,
    get_next_version,
    get_write_key,
    get_writes_prefix,
    normalize_checkpoint_ns,
    serialize_checkpoint_data,
    serialize_write_data,
)


class TestUtilityFunctions:
    """Test cases for utility functions."""

    def test_normalize_checkpoint_ns(self):
        """Test checkpoint namespace normalization."""
        assert normalize_checkpoint_ns("") == "__default__"
        assert normalize_checkpoint_ns("custom") == "custom"

    def test_denormalize_checkpoint_ns(self):
        """Test checkpoint namespace denormalization."""
        assert denormalize_checkpoint_ns("__default__") == ""
        assert denormalize_checkpoint_ns("custom") == "custom"

    def test_get_checkpoint_key(self):
        """Test checkpoint key generation."""
        key = get_checkpoint_key("test-checkpoints/", "thread1", "", "checkpoint1")
        assert key == "test-checkpoints/checkpoints/thread1/__default__/checkpoint1.json"

        key = get_checkpoint_key("test-checkpoints/", "thread1", "custom", "checkpoint1")
        assert key == "test-checkpoints/checkpoints/thread1/custom/checkpoint1.json"

    def test_get_writes_prefix(self):
        """Test writes prefix generation."""
        prefix = get_writes_prefix("test-checkpoints/", "thread1", "", "checkpoint1")
        assert prefix == "test-checkpoints/writes/thread1/__default__/checkpoint1/"

    def test_get_write_key(self):
        """Test write key generation."""
        key = get_write_key("test-checkpoints/", "thread1", "", "checkpoint1", "task1", 0)
        assert key == "test-checkpoints/writes/thread1/__default__/checkpoint1/task1_0.json"

    def test_serialize_deserialize_checkpoint_data(self):
        """Test checkpoint serialization and deserialization."""
        # Mock serde object
        mock_serde = MagicMock()

        # Mock the dumps_typed/loads_typed methods (they return tuples)
        mock_serde.dumps_typed.return_value = ("checkpoint_type", b'{"serialized": "checkpoint"}')
        mock_serde.dumps.return_value = b'{"serialized": "metadata"}'
        mock_serde.loads_typed.return_value = {"deserialized": "checkpoint"}
        mock_serde.loads.return_value = {"deserialized": "metadata"}

        checkpoint = {
            "v": 1,
            "id": "test-id",
            "ts": "2023-01-01T00:00:00Z",
            "channel_values": {"key": "value"},
            "channel_versions": {"key": "1"},
            "versions_seen": {},
        }
        metadata = {"source": "input", "step": 0}

        serialized = serialize_checkpoint_data(checkpoint, metadata, mock_serde)
        deserialized_checkpoint, deserialized_metadata = deserialize_checkpoint_data(serialized, mock_serde)

        # Verify the functions were called
        mock_serde.dumps_typed.assert_called_once_with(checkpoint)
        mock_serde.dumps.assert_called_once_with(metadata)

        assert isinstance(serialized, str)
        assert deserialized_checkpoint == {"deserialized": "checkpoint"}
        assert deserialized_metadata == {"deserialized": "metadata"}

    def test_serialize_deserialize_write_data(self):
        """Test write data serialization and deserialization."""
        # Mock serde object
        mock_serde = MagicMock()
        mock_serde.dumps_typed.return_value = ("value_type", b'{"serialized": "data"}')
        mock_serde.loads_typed.return_value = {"deserialized": "data"}

        channel = "test_channel"
        value = {"test": "data"}

        serialized = serialize_write_data(channel, value, mock_serde)
        deserialized_channel, deserialized_value = deserialize_write_data(serialized, mock_serde)

        # Verify the function was called
        mock_serde.dumps_typed.assert_called_once_with(value)

        assert isinstance(serialized, str)
        assert deserialized_channel == channel
        assert deserialized_value == {"deserialized": "data"}

    def test_get_next_version(self):
        """Test version generation."""
        # Test with None
        version = get_next_version(None, None)
        assert version.startswith("1.")

        # Test with string version
        version = get_next_version("5.123456", None)
        assert version.startswith("6.")

        # Test with int version
        version = get_next_version(10, None)
        assert version.startswith("11.")

        # Test with float version
        version = get_next_version(3.5, None)
        assert version.startswith("4.")
