"""Core reading functionality for S3 checkpoints."""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

import aioboto3

from ..aio import AsyncS3CheckpointSaver

logger = logging.getLogger(__name__)


class S3CheckpointReader:
    """Reader for S3-stored checkpoints that outputs JSON to stdout.
    
    This class uses AsyncS3CheckpointSaver internally for data access,
    providing only the presentation layer for CLI operations.
    """

    def __init__(self, bucket_name: str, prefix: str, session: Optional[aioboto3.Session] = None) -> None:
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
    
    @property
    def checkpointer(self) -> AsyncS3CheckpointSaver:
        """Get the checkpointer, creating it if needed."""
        if self._checkpointer is None:
            self._checkpointer = AsyncS3CheckpointSaver(
                bucket_name=self.bucket_name,
                prefix=self.prefix,
                session=self.session
            )
        return self._checkpointer

    def list_checkpoints(self, thread_id: str) -> List[Dict[str, str]]:
        """List all (checkpoint_ns, checkpoint_id) pairs for a thread.

        Args:
            thread_id: The thread ID to list checkpoints for

        Returns:
            List of dictionaries with checkpoint_ns and checkpoint_id keys
        """
        return asyncio.run(self._async_list_checkpoints(thread_id))

    async def _async_list_checkpoints(self, thread_id: str) -> List[Dict[str, str]]:
        """Async implementation of list_checkpoints."""
        thread_id = str(thread_id)
        config = {"configurable": {"thread_id": thread_id}}
        
        checkpoints = []
        try:
            async for checkpoint_tuple in self.checkpointer.alist(config):
                checkpoint_ns = checkpoint_tuple.config["configurable"].get("checkpoint_ns", "")
                checkpoint_id = checkpoint_tuple.config["configurable"]["checkpoint_id"]
                checkpoints.append({
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id
                })
            
            # Sort by checkpoint_id for consistent output
            checkpoints.sort(key=lambda x: x["checkpoint_id"])
            return checkpoints

        except Exception as e:
            raise RuntimeError(f"Failed to list checkpoints for thread {thread_id}: {e}") from e

    def dump_checkpoint(self, thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> Dict[str, Any]:
        """Dump a specific checkpoint object.

        Args:
            thread_id: The thread ID
            checkpoint_ns: The checkpoint namespace
            checkpoint_id: The checkpoint ID

        Returns:
            Dictionary containing the checkpoint data, metadata, and pending writes
        """
        return asyncio.run(self._async_dump_checkpoint(thread_id, checkpoint_ns, checkpoint_id))

    async def _async_dump_checkpoint(self, thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> Dict[str, Any]:
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
                "checkpoint": checkpoint_tuple.checkpoint,
                "metadata": checkpoint_tuple.metadata,
                "pending_writes": checkpoint_tuple.pending_writes,
            }

        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"Failed to get checkpoint: {e}") from e

    def read_all_checkpoints(self, thread_id: str) -> List[Dict[str, Any]]:
        """Read all checkpoints with their objects for a thread.

        Args:
            thread_id: The thread ID to read all checkpoints for

        Returns:
            List of dictionaries containing checkpoint data, metadata, and pending writes
        """
        return asyncio.run(self._async_read_all_checkpoints(thread_id))

    async def _async_read_all_checkpoints(self, thread_id: str) -> List[Dict[str, Any]]:
        """Async implementation of read_all_checkpoints with concurrent processing."""
        thread_id = str(thread_id)
        config = {"configurable": {"thread_id": thread_id}}
        
        all_checkpoints = []
        try:
            async for checkpoint_tuple in self.checkpointer.alist(config):
                checkpoint_ns = checkpoint_tuple.config["configurable"].get("checkpoint_ns", "")
                checkpoint_id = checkpoint_tuple.config["configurable"]["checkpoint_id"]
                
                checkpoint_data = {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                    "checkpoint": checkpoint_tuple.checkpoint,
                    "metadata": checkpoint_tuple.metadata,
                    "pending_writes": checkpoint_tuple.pending_writes,
                }
                all_checkpoints.append(checkpoint_data)

            return all_checkpoints

        except Exception as e:
            raise RuntimeError(f"Failed to read checkpoints for thread {thread_id}: {e}") from e


def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
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
