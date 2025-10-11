"""S3 checkpoint storage implementation for LangGraph."""

import logging
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any

import boto3
from botocore.exceptions import ClientError
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
    get_checkpoint_metadata,
)

from .utils import (
    denormalize_checkpoint_ns,
    deserialize_checkpoint_data,
    deserialize_write_data,
    get_checkpoint_key,
    get_write_key,
    get_writes_prefix,
    normalize_checkpoint_ns,
    serialize_checkpoint_data,
    serialize_write_data,
)

logger = logging.getLogger(__name__)


class S3CheckpointSaver(BaseCheckpointSaver[str]):
    """A checkpoint saver that stores checkpoints in Amazon S3.

    This class provides functionality to save and load LangGraph checkpoints
    to/from Amazon S3, enabling persistent storage of graph execution states.

    Args:
        bucket_name: The name of the S3 bucket to store checkpoints
        prefix: Optional prefix for checkpoint keys (default: "checkpoints/")
        s3_client: Optional boto3 S3 client instance. If not provided, will create one.

    Example:
        >>> import boto3
        >>> s3_client = boto3.client('s3')
        >>> saver = S3CheckpointSaver("my-bucket", prefix="my-app/", s3_client=s3_client)
        >>> # Use with LangGraph
        >>> graph = builder.compile(checkpointer=saver)
    """

    def __init__(
        self,
        bucket_name: str,
        *,
        prefix: str = "checkpoints/",
        s3_client: Any | None = None,
        **kwargs,
    ) -> None:
        """Initialize the S3 checkpoint saver."""
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix.rstrip("/") + "/" if prefix else ""
        self.s3_client = s3_client or boto3.client("s3")

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Get a checkpoint tuple from S3.

        This method retrieves a checkpoint tuple from S3 based on the
        provided config. If the config contains a "checkpoint_id" key, the checkpoint with
        the matching thread ID and checkpoint ID is retrieved. Otherwise, the latest checkpoint
        for the given thread ID is retrieved.

        Args:
            config: The config to use for retrieving the checkpoint.

        Returns:
            Optional[CheckpointTuple]: The retrieved checkpoint tuple, or None if no matching checkpoint was found.
        """
        thread_id = str(config["configurable"]["thread_id"])
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        if checkpoint_id := get_checkpoint_id(config):
            # Get specific checkpoint
            key = get_checkpoint_key(self.prefix, thread_id, checkpoint_ns, checkpoint_id)
            try:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                checkpoint_data = response["Body"].read().decode("utf-8")
                checkpoint, metadata = deserialize_checkpoint_data(checkpoint_data, self.serde)

                # Update config with checkpoint_id if not present
                if not get_checkpoint_id(config):
                    config = {
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": checkpoint_id,
                        }
                    }

                # Get pending writes
                pending_writes = self._get_writes(thread_id, checkpoint_ns, checkpoint_id)

                # Determine parent config
                parent_config = None
                if "parents" in metadata and checkpoint_ns in metadata["parents"]:
                    parent_checkpoint_id = metadata["parents"][checkpoint_ns]
                    parent_config = {
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": parent_checkpoint_id,
                        }
                    }

                return CheckpointTuple(
                    config=config,
                    checkpoint=checkpoint,
                    metadata=metadata,
                    parent_config=parent_config,
                    pending_writes=pending_writes,
                )

            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    return None
                raise RuntimeError(f"Failed to get checkpoint: {e}") from e
        else:
            # Get latest checkpoint - list checkpoints and get the most recent
            checkpoint_ns_safe = normalize_checkpoint_ns(checkpoint_ns)
            prefix = f"{self.prefix}checkpoints/{thread_id}/{checkpoint_ns_safe}/"

            try:
                # Use paginator to go over all objects under the prefix
                paginator = self.s3_client.get_paginator("list_objects_v2")
                page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

                objects = []
                for page in page_iterator:
                    if "Contents" in page:
                        objects.extend(page["Contents"])

                if not objects:
                    return None

                # Sort by key (checkpoint_id) in descending order to get latest
                objects = sorted(objects, key=lambda x: x["Key"], reverse=True)

                # Extract checkpoint_id from the key
                latest_key = objects[0]["Key"]
                latest_checkpoint_id = latest_key.split("/")[-1].replace(".json", "")

                # Recursively call with specific checkpoint_id
                config_with_id = {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": latest_checkpoint_id,
                    }
                }
                return self.get_tuple(config_with_id)

            except ClientError as e:
                # Use RuntimeError for S3 operation failures as it's more appropriate than ValueError
                raise RuntimeError(f"Failed to list checkpoints: {e}") from e

    def _get_writes(self, thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> list[tuple[str, str, Any]]:
        """Get all writes for a specific checkpoint."""
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

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints from S3.

        This method retrieves a list of checkpoint tuples from S3 based
        on the provided config. The checkpoints are ordered by checkpoint ID in descending order (newest first).

        Args:
            config: Base configuration for filtering checkpoints.
            filter: Additional filtering criteria for metadata.
            before: If provided, only checkpoints before the specified checkpoint ID are returned.
            limit: Maximum number of checkpoints to return.

        Yields:
            Iterator[CheckpointTuple]: An iterator of matching checkpoint tuples.
        """
        if config is None:
            # List all checkpoints across all threads - this could be expensive
            prefix = f"{self.prefix}checkpoints/"
        else:
            thread_id = str(config["configurable"]["thread_id"])
            checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
            checkpoint_ns_safe = normalize_checkpoint_ns(checkpoint_ns)
            prefix = f"{self.prefix}checkpoints/{thread_id}/{checkpoint_ns_safe}/"

        before_checkpoint_id = None
        if before:
            before_checkpoint_id = get_checkpoint_id(before)

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

            checkpoints = []
            for page in page_iterator:
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    key = obj["Key"]
                    if not key.endswith(".json"):
                        continue

                    # Parse the key to extract thread_id, checkpoint_ns, checkpoint_id
                    parts = key[len(self.prefix) :].split("/")
                    if len(parts) < 4 or parts[0] != "checkpoints":
                        continue

                    key_thread_id = parts[1]
                    key_checkpoint_ns_safe = parts[2]
                    key_checkpoint_id = parts[3].replace(".json", "")
                    key_checkpoint_ns = denormalize_checkpoint_ns(key_checkpoint_ns_safe)

                    # Apply before filter
                    if before_checkpoint_id and key_checkpoint_id >= before_checkpoint_id:
                        continue

                    checkpoints.append((key_thread_id, key_checkpoint_ns, key_checkpoint_id, obj["LastModified"]))

            # Sort by checkpoint_id descending (newest first)
            checkpoints.sort(key=lambda x: x[2], reverse=True)

            # Apply limit
            if limit:
                checkpoints = checkpoints[:limit]

            # Yield checkpoint tuples
            for thread_id, checkpoint_ns, checkpoint_id, _ in checkpoints:
                checkpoint_config = {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint_id,
                    }
                }

                checkpoint_tuple = self.get_tuple(checkpoint_config)
                if checkpoint_tuple:
                    # Apply metadata filter if provided
                    if filter:
                        metadata_matches = all(checkpoint_tuple.metadata.get(k) == v for k, v in filter.items())
                        if not metadata_matches:
                            continue

                    yield checkpoint_tuple

        except ClientError as e:
            # Use RuntimeError for S3 operation failures as it's more appropriate than ValueError
            raise RuntimeError(f"Failed to list checkpoints: {e}") from e

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Save a checkpoint to S3.

        This method saves a checkpoint to S3. The checkpoint is associated
        with the provided config and its parent config (if any).

        Args:
            config: The config to associate with the checkpoint.
            checkpoint: The checkpoint to save.
            metadata: Additional metadata to save with the checkpoint.
            new_versions: New channel versions as of this write.

        Returns:
            RunnableConfig: Updated configuration after storing the checkpoint.
        """
        thread_id = str(config["configurable"]["thread_id"])
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = checkpoint["id"]

        # Prepare metadata with parent information
        full_metadata = get_checkpoint_metadata(config, metadata)

        # Serialize and store checkpoint
        checkpoint_data = serialize_checkpoint_data(checkpoint, full_metadata, self.serde)
        key = get_checkpoint_key(self.prefix, thread_id, checkpoint_ns, checkpoint_id)

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=checkpoint_data,
                ContentType="application/json",
            )

            logger.info(f"Checkpoint saved to s3://{self.bucket_name}/{key}")

            return {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            }

        except ClientError as e:
            raise RuntimeError(f"Failed to save checkpoint: {e}") from e

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store intermediate writes linked to a checkpoint.

        This method saves intermediate writes associated with a checkpoint to S3.

        Args:
            config: Configuration of the related checkpoint.
            writes: List of writes to store, each as (channel, value) pair.
            task_id: Identifier for the task creating the writes.
            task_path: Path of the task creating the writes.
        """
        thread_id = str(config["configurable"]["thread_id"])
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = str(config["configurable"]["checkpoint_id"])

        try:
            for idx, (channel, value) in enumerate(writes):
                # Use WRITES_IDX_MAP for special channels, otherwise use sequential index
                write_idx = WRITES_IDX_MAP.get(channel, idx)

                write_data = serialize_write_data(channel, value, self.serde)
                key = get_write_key(self.prefix, thread_id, checkpoint_ns, checkpoint_id, task_id, write_idx)

                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=write_data,
                    ContentType="application/json",
                )

            logger.debug(f"Stored {len(writes)} writes for checkpoint {checkpoint_id}")

        except ClientError as e:
            raise RuntimeError(f"Failed to store writes: {e}") from e

    def delete_thread(self, thread_id: str) -> None:
        """Delete all checkpoints and writes associated with a thread ID.

        Args:
            thread_id: The thread ID to delete.
        """
        thread_id = str(thread_id)

        # Delete all objects with the thread_id prefix
        prefixes = [
            f"{self.prefix}checkpoints/{thread_id}/",
            f"{self.prefix}writes/{thread_id}/",
        ]

        try:
            for prefix in prefixes:
                paginator = self.s3_client.get_paginator("list_objects_v2")
                page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

                objects_to_delete = []
                for page in page_iterator:
                    if "Contents" not in page:
                        continue

                    for obj in page["Contents"]:
                        objects_to_delete.append({"Key": obj["Key"]})

                        # Delete in batches of 1000 (S3 limit)
                        if len(objects_to_delete) >= 1000:
                            self.s3_client.delete_objects(
                                Bucket=self.bucket_name, Delete={"Objects": objects_to_delete}
                            )
                            objects_to_delete = []

                # Delete remaining objects
                if objects_to_delete:
                    self.s3_client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": objects_to_delete})

            logger.info(f"Deleted all data for thread {thread_id}")

        except ClientError as e:
            raise RuntimeError(f"Failed to delete thread data: {e}") from e

    # Async methods - not implemented in sync version
    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Async version not supported in sync implementation."""
        raise NotImplementedError(
            "Async methods are not supported in S3CheckpointSaver. Use AsyncS3CheckpointSaver instead."
        )

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """Async version not supported in sync implementation."""
        raise NotImplementedError(
            "Async methods are not supported in S3CheckpointSaver. Use AsyncS3CheckpointSaver instead."
        )
        yield  # Make this a generator

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Async version not supported in sync implementation."""
        raise NotImplementedError(
            "Async methods are not supported in S3CheckpointSaver. Use AsyncS3CheckpointSaver instead."
        )

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Async version not supported in sync implementation."""
        raise NotImplementedError(
            "Async methods are not supported in S3CheckpointSaver. Use AsyncS3CheckpointSaver instead."
        )

    async def adelete_thread(self, thread_id: str) -> None:
        """Async version not supported in sync implementation."""
        raise NotImplementedError(
            "Async methods are not supported in S3CheckpointSaver. Use AsyncS3CheckpointSaver instead."
        )
