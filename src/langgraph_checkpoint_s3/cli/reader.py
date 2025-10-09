"""Core reading functionality for S3 checkpoints."""

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from ..utils import (
    denormalize_checkpoint_ns,
    deserialize_checkpoint_data,
    deserialize_write_data,
    get_checkpoint_key,
    get_writes_prefix,
    normalize_checkpoint_ns,
)

logger = logging.getLogger(__name__)


class S3CheckpointReader:
    """Reader for S3-stored checkpoints that outputs JSON to stdout."""

    def __init__(self, bucket_name: str, prefix: str, s3_client: Optional[Any] = None) -> None:
        """Initialize the S3 checkpoint reader.

        Args:
            bucket_name: The name of the S3 bucket containing checkpoints
            prefix: The prefix for checkpoint keys (should end with '/')
            s3_client: Optional boto3 S3 client instance. If not provided, will create one.
        """
        self.bucket_name = bucket_name
        self.prefix = prefix.rstrip("/") + "/" if prefix else ""
        self.s3_client = s3_client or boto3.client("s3")
        # Use a simple serde for deserialization - we'll use the default from langgraph
        from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

        self.serde = JsonPlusSerializer()

    def list_checkpoints(self, thread_id: str) -> List[Dict[str, str]]:
        """List all (checkpoint_ns, checkpoint_id) pairs for a thread.

        Args:
            thread_id: The thread ID to list checkpoints for

        Returns:
            List of dictionaries with checkpoint_ns and checkpoint_id keys
        """
        thread_id = str(thread_id)
        prefix = f"{self.prefix}checkpoints/{thread_id}/"

        checkpoints = []
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

            for page in page_iterator:
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    key = obj["Key"]
                    if not key.endswith(".json"):
                        continue

                    # Parse the key to extract checkpoint_ns and checkpoint_id
                    # Key format: {prefix}checkpoints/{thread_id}/{checkpoint_ns_safe}/{checkpoint_id}.json
                    # Remove the prefix to get the relative path
                    relative_key = key[len(self.prefix):]
                    parts = relative_key.split("/")
                    
                    # Validate key structure: should be checkpoints/{thread_id}/{checkpoint_ns_safe}/{checkpoint_id}.json
                    if len(parts) < 4 or parts[0] != "checkpoints":
                        continue

                    key_thread_id = parts[1]
                    if key_thread_id != thread_id:
                        continue

                    key_checkpoint_ns_safe = parts[2]
                    key_checkpoint_id = parts[3].replace(".json", "")
                    key_checkpoint_ns = denormalize_checkpoint_ns(key_checkpoint_ns_safe)

                    checkpoints.append({"checkpoint_ns": key_checkpoint_ns, "checkpoint_id": key_checkpoint_id})

            # Sort by checkpoint_id for consistent output
            checkpoints.sort(key=lambda x: x["checkpoint_id"])
            return checkpoints

        except ClientError as e:
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
        thread_id = str(thread_id)
        checkpoint_ns = checkpoint_ns or ""
        checkpoint_id = str(checkpoint_id)

        # Get the checkpoint
        key = get_checkpoint_key(self.prefix, thread_id, checkpoint_ns, checkpoint_id)
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            checkpoint_data = response["Body"].read().decode("utf-8")
            checkpoint, metadata = deserialize_checkpoint_data(checkpoint_data, self.serde)

            # Get pending writes
            pending_writes = self._get_writes(thread_id, checkpoint_ns, checkpoint_id)

            return {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                "checkpoint": checkpoint,
                "metadata": metadata,
                "pending_writes": pending_writes,
            }

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise RuntimeError(f"Checkpoint not found: {checkpoint_id} in thread {thread_id}") from e
            raise RuntimeError(f"Failed to get checkpoint: {e}") from e

    def read_all_checkpoints(self, thread_id: str) -> List[Dict[str, Any]]:
        """Read all checkpoints with their objects for a thread.

        Args:
            thread_id: The thread ID to read all checkpoints for

        Returns:
            List of dictionaries containing checkpoint data, metadata, and pending writes
        """
        # First, list all checkpoints for the thread
        checkpoint_list = self.list_checkpoints(thread_id)

        # Then dump each checkpoint
        all_checkpoints = []
        for checkpoint_info in checkpoint_list:
            try:
                checkpoint_data = self.dump_checkpoint(
                    thread_id, checkpoint_info["checkpoint_ns"], checkpoint_info["checkpoint_id"]
                )
                all_checkpoints.append(checkpoint_data)
            except RuntimeError as e:
                # Log the error but continue with other checkpoints
                logger.warning(f"Failed to dump checkpoint {checkpoint_info['checkpoint_id']}: {e}")

        return all_checkpoints

    def _get_writes(self, thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> List[Tuple[str, str, Any]]:
        """Get all writes for a specific checkpoint.

        Args:
            thread_id: The thread ID
            checkpoint_ns: The checkpoint namespace
            checkpoint_id: The checkpoint ID

        Returns:
            List of tuples containing (task_id, channel, value)
        """
        writes_prefix = get_writes_prefix(self.prefix, thread_id, checkpoint_ns, checkpoint_id)
        writes = []

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=writes_prefix)

            for page in page_iterator:
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    key = obj["Key"]
                    # Extract task_id and idx from filename
                    filename = key.split("/")[-1].replace(".json", "")
                    if "_" in filename:
                        task_id, idx_str = filename.rsplit("_", 1)
                        try:
                            idx = int(idx_str)
                        except ValueError:
                            continue

                        # Get the write data
                        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                        write_data = response["Body"].read().decode("utf-8")
                        channel, value = deserialize_write_data(write_data, self.serde)

                        writes.append((task_id, channel, value, idx))

            # Sort writes by task_id and idx (matching SQLite implementation)
            writes.sort(key=lambda x: (x[0], x[3]))  # Sort by task_id, then idx
            # Remove idx from the final result to match expected format
            return [(task_id, channel, value) for task_id, channel, value, idx in writes]

        except ClientError as e:
            logger.warning(f"Failed to get writes for checkpoint {checkpoint_id}: {e}")
            return []


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
