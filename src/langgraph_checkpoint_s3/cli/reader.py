"""Core reading functionality for S3 checkpoints."""

import asyncio
import logging
from typing import Any

import aioboto3
from botocore.exceptions import ClientError
from langgraph.checkpoint.base import Checkpoint

from ..aio import AsyncS3CheckpointSaver
from ..utils import denormalize_checkpoint_ns

logger = logging.getLogger(__name__)


class S3CheckpointReader:
    """Reader for S3-stored checkpoints that outputs JSON to stdout.

    This class uses AsyncS3CheckpointSaver internally for data access,
    providing only the presentation layer for CLI operations.
    """

    def __init__(self, bucket_name: str, prefix: str, session: aioboto3.Session | None = None) -> None:
        """Initialize the S3 checkpoint reader.

        Args:
            bucket_name: The name of the S3 bucket containing checkpoints
            prefix: The prefix for checkpoint keys (should end with '/')
            session: Optional aioboto3 Session instance. If not provided, will create one.
        """
        self.bucket_name = bucket_name
        self.prefix = prefix.rstrip("/") + "/" if prefix else ""
        self.session = session
        self._checkpointer = None  # Lazy initialization

    @staticmethod
    def _format_checkpoint_value(checkpoint: Checkpoint) -> Any:
        """Format checkpoint values for better readability."""
        if isinstance(checkpoint, dict):
            formatted = {}
            for key, value in checkpoint.items():
                if key == "channel_versions":
                    formatted[key] = dict(value) if value else {}
                elif key == "versions_seen":
                    formatted[key] = dict(value) if value else {}
                elif key == "channel_values":
                    # Format channel values for readability
                    if value:
                        formatted_values = {}
                        for channel, channel_value in value.items():
                            formatted_values[channel] = S3CheckpointReader._format_channel_value(channel_value)
                        formatted[key] = formatted_values
                    else:
                        formatted[key] = {}
                else:
                    formatted[key] = S3CheckpointReader._format_checkpoint_value(value)
            return formatted
        elif isinstance(checkpoint, list):
            return [S3CheckpointReader._format_checkpoint_value(item) for item in checkpoint]
        elif hasattr(checkpoint, "__dict__"):
            # For objects, try to convert to dict
            try:
                return vars(checkpoint)
            except Exception:
                return str(checkpoint)
        else:
            return checkpoint

    @staticmethod
    def _format_channel_value(channel_value: Any) -> Any:
        """Format individual channel values for better readability."""
        if isinstance(channel_value, dict):
            return {k: S3CheckpointReader._format_channel_value(v) for k, v in channel_value.items()}
        elif isinstance(channel_value, list):
            return [S3CheckpointReader._format_channel_value(item) for item in channel_value]
        elif hasattr(channel_value, "__dict__"):
            # For objects, try to convert to dict
            try:
                return vars(channel_value)
            except Exception:
                return str(channel_value)
        else:
            return channel_value

    @property
    def checkpointer(self) -> AsyncS3CheckpointSaver:
        """Get the checkpointer, creating it if needed."""
        if self._checkpointer is None:
            self._checkpointer = AsyncS3CheckpointSaver(
                bucket_name=self.bucket_name, prefix=self.prefix, session=self.session
            )
        return self._checkpointer

    async def _discover_namespaces_for_thread(self, thread_id: str) -> list[str]:
        """Discover all namespaces that exist for a given thread.

        Args:
            thread_id: The thread ID to discover namespaces for

        Returns:
            List of namespace strings (including empty string for default namespace)
        """
        thread_id = str(thread_id)
        prefix = f"{self.prefix}checkpoints/{thread_id}/"
        namespaces = set()

        async with self.checkpointer._get_s3_client() as s3_client:
            try:
                # Use list_objects_v2 with delimiter to get "directories" (namespaces)
                paginator = s3_client.get_paginator("list_objects_v2")
                page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix, Delimiter="/")

                async for page in page_iterator:
                    # Check CommonPrefixes for namespace directories
                    if "CommonPrefixes" in page:
                        for common_prefix in page["CommonPrefixes"]:
                            namespace_prefix = common_prefix["Prefix"]
                            # Extract namespace from the prefix
                            # Format: {self.prefix}checkpoints/{thread_id}/{namespace_safe}/
                            namespace_safe = namespace_prefix[len(prefix) :].rstrip("/")
                            if namespace_safe:  # Skip empty namespace_safe (shouldn't happen with delimiter)
                                namespace = denormalize_checkpoint_ns(namespace_safe)
                                namespaces.add(namespace)

                    # Also check for direct files (in case there are checkpoints in subdirectories)
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            key = obj["Key"]
                            if key.endswith(".json"):
                                # Parse the key to extract namespace
                                # Format: {self.prefix}checkpoints/{thread_id}/{namespace_safe}/{checkpoint_id}.json
                                relative_key = key[len(prefix) :]
                                parts = relative_key.split("/")
                                if len(parts) >= 2:  # namespace_safe/checkpoint_id.json
                                    namespace_safe = parts[0]
                                    namespace = denormalize_checkpoint_ns(namespace_safe)
                                    namespaces.add(namespace)

                return sorted(namespaces)

            except ClientError as e:
                logger.warning(f"Failed to discover namespaces for thread {thread_id}: {e}")
                # Fallback to just the default namespace
                return [""]

    def list_checkpoints(self, thread_id: str) -> list[dict[str, str]]:
        """List all (checkpoint_ns, checkpoint_id) pairs for a thread.

        Args:
            thread_id: The thread ID to list checkpoints for

        Returns:
            List of dictionaries with checkpoint_ns and checkpoint_id keys
        """
        return asyncio.run(self._async_list_checkpoints(thread_id))

    async def _async_list_checkpoints(self, thread_id: str) -> list[dict[str, str]]:
        """Async implementation of list_checkpoints."""
        thread_id = str(thread_id)

        checkpoints = []
        try:
            # First, discover all namespaces for this thread by listing the directory structure
            namespaces = await self._discover_namespaces_for_thread(thread_id)

            # For each namespace, list the checkpoints
            for namespace in namespaces:
                config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": namespace}}
                async for checkpoint_tuple in self.checkpointer.alist(config):
                    checkpoint_ns = checkpoint_tuple.config["configurable"].get("checkpoint_ns", "")
                    checkpoint_id = checkpoint_tuple.config["configurable"]["checkpoint_id"]
                    checkpoints.append({"checkpoint_ns": checkpoint_ns, "checkpoint_id": checkpoint_id})

            # Sort by checkpoint_id for consistent output
            checkpoints.sort(key=lambda x: x["checkpoint_id"])
            return checkpoints

        except Exception as e:
            raise RuntimeError(f"Failed to list checkpoints for thread {thread_id}: {e}") from e

    def dump_checkpoint(self, thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> dict[str, Any]:
        """Dump a specific checkpoint object.

        Args:
            thread_id: The thread ID
            checkpoint_ns: The checkpoint namespace
            checkpoint_id: The checkpoint ID

        Returns:
            dictionary containing the checkpoint data, metadata, and pending writes
        """
        return asyncio.run(self._async_dump_checkpoint(thread_id, checkpoint_ns, checkpoint_id))

    async def _async_dump_checkpoint(self, thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> dict[str, Any]:
        """Async implementation of dump_checkpoint."""
        thread_id = str(thread_id)
        checkpoint_ns = checkpoint_ns or ""
        checkpoint_id = str(checkpoint_id)

        config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }

        try:
            checkpoint_tuple = await self.checkpointer.aget_tuple(config)
            if checkpoint_tuple is None:
                raise RuntimeError(f"Checkpoint not found: {checkpoint_id} in thread {thread_id}")

            return {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                "checkpoint": S3CheckpointReader._format_checkpoint_value(checkpoint_tuple.checkpoint),
                "metadata": checkpoint_tuple.metadata,
                "pending_writes": checkpoint_tuple.pending_writes,
            }

        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"Failed to get checkpoint: {e}") from e

    def read_all_checkpoints(self, thread_id: str) -> list[dict[str, Any]]:
        """Read all checkpoints with their objects for a thread.

        Args:
            thread_id: The thread ID to read all checkpoints for

        Returns:
            List of dictionaries containing checkpoint data, metadata, and pending writes
        """
        return asyncio.run(self._async_read_all_checkpoints(thread_id))

    async def _async_read_all_checkpoints(self, thread_id: str) -> list[dict[str, Any]]:
        """Async implementation of read_all_checkpoints with concurrent processing."""
        thread_id = str(thread_id)

        all_checkpoints = []
        try:
            # First, discover all namespaces for this thread
            namespaces = await self._discover_namespaces_for_thread(thread_id)

            # For each namespace, read the checkpoints
            for namespace in namespaces:
                config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": namespace}}
                async for checkpoint_tuple in self.checkpointer.alist(config):
                    checkpoint_ns = checkpoint_tuple.config["configurable"].get("checkpoint_ns", "")
                    checkpoint_id = checkpoint_tuple.config["configurable"]["checkpoint_id"]

                    checkpoint_data = {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint_id,
                        "checkpoint": S3CheckpointReader._format_checkpoint_value(checkpoint_tuple.checkpoint),
                        "metadata": checkpoint_tuple.metadata,
                        "pending_writes": checkpoint_tuple.pending_writes,
                    }
                    all_checkpoints.append(checkpoint_data)

            # Sort by checkpoint_id for consistent output
            all_checkpoints.sort(key=lambda x: x["checkpoint_id"])
            return all_checkpoints

        except Exception as e:
            raise RuntimeError(f"Failed to read checkpoints for thread {thread_id}: {e}") from e


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse an S3 URI into bucket name and prefix.

    Args:
        s3_uri: S3 URI in format s3://bucket/prefix/ or s3://bucket/prefix

    Returns:
        Tuple of (bucket_name, prefix)

    Raises:
        ValueError: If the S3 URI format is invalid
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI format: {s3_uri}. Must start with 's3://'")

    # Remove s3:// prefix
    path = s3_uri[5:]

    # Split into bucket and prefix
    parts = path.split("/", 1)
    if len(parts) == 1:
        # No prefix, just bucket
        bucket_name = parts[0]
        prefix = ""
    else:
        bucket_name = parts[0]
        prefix = parts[1]

    if not bucket_name:
        raise ValueError(f"Invalid S3 URI format: {s3_uri}. Bucket name cannot be empty")

    return bucket_name, prefix
