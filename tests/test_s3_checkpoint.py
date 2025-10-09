"""Tests for S3CheckpointSaver and AsyncS3CheckpointSaver."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from botocore.exceptions import ClientError

from langgraph_checkpoint_s3 import S3CheckpointSaver

try:
    from langgraph_checkpoint_s3 import AsyncS3CheckpointSaver

    ASYNC_AVAILABLE = True
except ImportError:
    ASYNC_AVAILABLE = False


class TestS3CheckpointSaver:
    """Test cases for S3CheckpointSaver."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_s3_client = MagicMock()
        self.bucket_name = "test-bucket"
        self.prefix = "test-checkpoints/"

        self.saver = S3CheckpointSaver(
            bucket_name=self.bucket_name,
            prefix=self.prefix,
            s3_client=self.mock_s3_client,
        )

    def test_init_success(self):
        """Test successful initialization."""
        assert self.saver.bucket_name == self.bucket_name
        assert self.saver.prefix == "test-checkpoints/"
        assert self.saver.s3_client == self.mock_s3_client

    def test_get_tuple_not_found(self):
        """Test getting a checkpoint that doesn't exist."""
        config = {
            "configurable": {
                "thread_id": "thread1",
                "checkpoint_ns": "",
                "checkpoint_id": "nonexistent",
            }
        }

        # Mock S3 NoSuchKey error
        self.mock_s3_client.get_object.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

        result = self.saver.get_tuple(config)
        assert result is None

    def test_get_tuple_error(self):
        """Test getting a checkpoint with S3 error."""
        config = {
            "configurable": {
                "thread_id": "thread1",
                "checkpoint_ns": "",
                "checkpoint_id": "checkpoint1",
            }
        }

        # Mock S3 error
        self.mock_s3_client.get_object.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")

        with pytest.raises(RuntimeError, match="Failed to get checkpoint"):
            self.saver.get_tuple(config)

    def test_put_success(self):
        """Test successful checkpoint save."""
        config = {
            "configurable": {
                "thread_id": "thread1",
                "checkpoint_ns": "",
            }
        }
        checkpoint = {
            "v": 1,
            "id": "checkpoint1",
            "ts": "2023-01-01T00:00:00Z",
            "channel_values": {"key": "value"},
            "channel_versions": {"key": "1"},
            "versions_seen": {},
        }
        metadata = {"source": "input", "step": 0}

        result = self.saver.put(config, checkpoint, metadata, {})

        assert result == {
            "configurable": {
                "thread_id": "thread1",
                "checkpoint_ns": "",
                "checkpoint_id": "checkpoint1",
            }
        }

        self.mock_s3_client.put_object.assert_called_once()
        call_args = self.mock_s3_client.put_object.call_args
        assert call_args[1]["Bucket"] == self.bucket_name
        assert call_args[1]["Key"] == "test-checkpoints/checkpoints/thread1/__default__/checkpoint1.json"
        assert call_args[1]["ContentType"] == "application/json"

    def test_put_writes_success(self):
        """Test successful writes storage."""
        config = {
            "configurable": {
                "thread_id": "thread1",
                "checkpoint_ns": "",
                "checkpoint_id": "checkpoint1",
            }
        }
        writes = [("channel1", {"data": "value1"}), ("channel2", {"data": "value2"})]
        task_id = "task1"

        self.saver.put_writes(config, writes, task_id)

        # Should have called put_object twice (once for each write)
        assert self.mock_s3_client.put_object.call_count == 2

    def test_delete_thread_success(self):
        """Test successful thread deletion."""
        thread_id = "thread1"

        # Mock paginator
        mock_paginator = MagicMock()
        mock_page_iterator = [
            {
                "Contents": [
                    {"Key": "test-checkpoints/checkpoints/thread1/__default__/checkpoint1.json"},
                    {"Key": "test-checkpoints/writes/thread1/__default__/checkpoint1/task1_0.json"},
                ]
            }
        ]
        mock_paginator.paginate.return_value = mock_page_iterator
        self.mock_s3_client.get_paginator.return_value = mock_paginator

        self.saver.delete_thread(thread_id)

        # Should have called delete_objects for both prefixes
        assert self.mock_s3_client.delete_objects.call_count == 2

    def test_list_checkpoints_empty(self):
        """Test listing checkpoints when none exist."""
        config = {
            "configurable": {
                "thread_id": "thread1",
                "checkpoint_ns": "",
            }
        }

        # Mock empty paginator
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = []
        self.mock_s3_client.get_paginator.return_value = mock_paginator

        checkpoints = list(self.saver.list(config))
        assert checkpoints == []

    @pytest.mark.asyncio
    async def test_get_next_version(self):
        """Test version generation."""
        # Test with None
        version = self.saver.get_next_version(None, None)
        assert version.startswith("1.")

        # Test with string version
        version = self.saver.get_next_version("5.123456", None)
        assert version.startswith("6.")

        # Test with int version
        version = self.saver.get_next_version(10, None)
        assert version.startswith("11.")

    def test_async_methods_not_implemented(self):
        """Test that async methods raise NotImplementedError."""
        config = {"configurable": {"thread_id": "thread1"}}

        with pytest.raises(NotImplementedError):
            asyncio.run(self.saver.aget_tuple(config))

        with pytest.raises(NotImplementedError):
            asyncio.run(self.saver.aput(config, {}, {}, {}))

        with pytest.raises(NotImplementedError):
            asyncio.run(self.saver.aput_writes(config, [], "task1"))

        with pytest.raises(NotImplementedError):
            asyncio.run(self.saver.adelete_thread("thread1"))


@pytest.mark.skipif(not ASYNC_AVAILABLE, reason="aioboto3 not available")
class TestAsyncS3CheckpointSaver:
    """Test cases for AsyncS3CheckpointSaver."""

    @pytest_asyncio.fixture(autouse=True)
    async def setup_method(self):
        """Set up test fixtures."""
        self.mock_s3_client = AsyncMock()
        self.bucket_name = "test-bucket"
        self.prefix = "test-checkpoints/"

        self.saver = AsyncS3CheckpointSaver(
            bucket_name=self.bucket_name,
            prefix=self.prefix,
            s3_client=self.mock_s3_client,
        )

    @pytest.mark.asyncio
    async def test_init_success(self):
        """Test successful initialization."""
        assert self.saver.bucket_name == self.bucket_name
        assert self.saver.prefix == "test-checkpoints/"
        assert self.saver.s3_client == self.mock_s3_client

    @pytest.mark.asyncio
    async def test_aget_tuple_not_found(self):
        """Test getting a checkpoint that doesn't exist."""
        config = {
            "configurable": {
                "thread_id": "thread1",
                "checkpoint_ns": "",
                "checkpoint_id": "nonexistent",
            }
        }

        # Mock S3 NoSuchKey error
        self.mock_s3_client.get_object.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

        result = await self.saver.aget_tuple(config)
        assert result is None

    @pytest.mark.asyncio
    async def test_aget_tuple_error(self):
        """Test getting a checkpoint with S3 error."""
        config = {
            "configurable": {
                "thread_id": "thread1",
                "checkpoint_ns": "",
                "checkpoint_id": "checkpoint1",
            }
        }

        # Mock S3 error
        self.mock_s3_client.get_object.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")

        with pytest.raises(RuntimeError, match="Failed to get checkpoint"):
            await self.saver.aget_tuple(config)

    @pytest.mark.asyncio
    async def test_aput_success(self):
        """Test successful checkpoint save."""
        config = {
            "configurable": {
                "thread_id": "thread1",
                "checkpoint_ns": "",
            }
        }
        checkpoint = {
            "v": 1,
            "id": "checkpoint1",
            "ts": "2023-01-01T00:00:00Z",
            "channel_values": {"key": "value"},
            "channel_versions": {"key": "1"},
            "versions_seen": {},
        }
        metadata = {"source": "input", "step": 0}

        result = await self.saver.aput(config, checkpoint, metadata, {})

        assert result == {
            "configurable": {
                "thread_id": "thread1",
                "checkpoint_ns": "",
                "checkpoint_id": "checkpoint1",
            }
        }

        self.mock_s3_client.put_object.assert_called_once()

    @pytest.mark.asyncio
    async def test_aput_writes_success(self):
        """Test successful writes storage."""
        config = {
            "configurable": {
                "thread_id": "thread1",
                "checkpoint_ns": "",
                "checkpoint_id": "checkpoint1",
            }
        }
        writes = [("channel1", {"data": "value1"}), ("channel2", {"data": "value2"})]
        task_id = "task1"

        await self.saver.aput_writes(config, writes, task_id)

        # Should have called put_object twice (once for each write)
        assert self.mock_s3_client.put_object.call_count == 2

    @pytest.mark.asyncio
    async def test_adelete_thread_success(self):
        """Test successful thread deletion."""
        thread_id = "thread1"

        # Mock paginator
        mock_paginator = MagicMock()

        async def mock_paginate(*args, **kwargs):
            yield {
                "Contents": [
                    {"Key": "test-checkpoints/checkpoints/thread1/__default__/checkpoint1.json"},
                    {"Key": "test-checkpoints/writes/thread1/__default__/checkpoint1/task1_0.json"},
                ]
            }

        mock_paginator.paginate = mock_paginate
        # Make get_paginator return a regular MagicMock, not an AsyncMock
        self.mock_s3_client.get_paginator = MagicMock(return_value=mock_paginator)

        await self.saver.adelete_thread(thread_id)

        # Should have called delete_objects for both prefixes
        assert self.mock_s3_client.delete_objects.call_count == 2

    @pytest.mark.asyncio
    async def test_alist_empty(self):
        """Test listing checkpoints when none exist."""
        config = {
            "configurable": {
                "thread_id": "thread1",
                "checkpoint_ns": "",
            }
        }

        # Mock empty paginator
        mock_paginator = MagicMock()

        async def mock_paginate(*args, **kwargs):
            return
            yield  # Make it an async generator

        mock_paginator.paginate = mock_paginate
        # Make get_paginator return a regular MagicMock, not an AsyncMock
        self.mock_s3_client.get_paginator = MagicMock(return_value=mock_paginator)

        checkpoints = []
        async for checkpoint in self.saver.alist(config):
            checkpoints.append(checkpoint)

        assert checkpoints == []

    @pytest.mark.asyncio
    async def test_sync_methods_in_async_context(self):
        """Test that sync methods raise RuntimeError in async context."""
        config = {"configurable": {"thread_id": "thread1"}}

        # Test that sync methods raise InvalidStateError when called in async context
        with pytest.raises(asyncio.InvalidStateError):
            self.saver.get_tuple(config)

        with pytest.raises(asyncio.InvalidStateError):
            self.saver.put(config, {}, {}, {})

        with pytest.raises(asyncio.InvalidStateError):
            self.saver.put_writes(config, [], "task1")

        with pytest.raises(asyncio.InvalidStateError):
            self.saver.delete_thread("thread1")

    @pytest.mark.asyncio
    async def test_get_next_version(self):
        """Test version generation."""
        # Test with None
        version = self.saver.get_next_version(None, None)
        assert version.startswith("1.")

        # Test with string version
        version = self.saver.get_next_version("5.123456", None)
        assert version.startswith("6.")

        # Test with int version
        version = self.saver.get_next_version(10, None)
        assert version.startswith("11.")
