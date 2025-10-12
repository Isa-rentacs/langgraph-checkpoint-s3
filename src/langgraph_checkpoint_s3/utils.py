"""Common utilities for S3 checkpoint storage implementations."""

import base64
import json
from typing import Any, cast

from langgraph.checkpoint.base import (
    Checkpoint,
    CheckpointMetadata,
)
from langgraph.checkpoint.serde.base import SerializerProtocol


def normalize_checkpoint_ns(checkpoint_ns: str) -> str:
    """Convert empty/None checkpoint_ns to a safe directory name."""
    return checkpoint_ns if checkpoint_ns else "__default__"


def denormalize_checkpoint_ns(checkpoint_ns_safe: str) -> str:
    """Convert back from safe directory name to original checkpoint_ns."""
    return "" if checkpoint_ns_safe == "__default__" else checkpoint_ns_safe


def get_checkpoint_key(prefix: str, thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> str:
    """Generate the S3 key for a checkpoint."""
    checkpoint_ns_safe = normalize_checkpoint_ns(checkpoint_ns)
    return f"{prefix}checkpoints/{thread_id}/{checkpoint_ns_safe}/{checkpoint_id}.json"


def get_writes_prefix(prefix: str, thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> str:
    """Generate the S3 prefix for writes associated with a checkpoint."""
    checkpoint_ns_safe = normalize_checkpoint_ns(checkpoint_ns)
    return f"{prefix}writes/{thread_id}/{checkpoint_ns_safe}/{checkpoint_id}/"


def get_write_key(prefix: str, thread_id: str, checkpoint_ns: str, checkpoint_id: str, task_id: str, idx: int) -> str:
    """Generate the S3 key for a specific write."""
    writes_prefix = get_writes_prefix(prefix, thread_id, checkpoint_ns, checkpoint_id)
    return f"{writes_prefix}{task_id}_{idx}.json"


def serialize_checkpoint_data(checkpoint: Checkpoint, metadata: CheckpointMetadata, serde: SerializerProtocol) -> str:
    """Serialize checkpoint and metadata for storage."""
    type_, serialized_checkpoint = serde.dumps_typed(checkpoint)
    # Convert bytes to base64 string for JSON serialization
    checkpoint_b64 = base64.b64encode(serialized_checkpoint).decode("utf-8")
    # Use same serde for metadata
    metadata_bytes = serde.dumps(metadata)
    metadata_b64 = base64.b64encode(metadata_bytes).decode("utf-8")

    data = {
        "checkpoint_type": type_,
        "checkpoint": checkpoint_b64,
        "metadata": metadata_b64,
    }
    return json.dumps(data, indent=2)


def deserialize_checkpoint_data(data: str, serde: SerializerProtocol) -> tuple[Checkpoint, CheckpointMetadata]:
    """Deserialize checkpoint and metadata from storage."""
    parsed = json.loads(data)
    # Convert base64 string back to bytes
    checkpoint_bytes = base64.b64decode(parsed["checkpoint"])
    checkpoint = serde.loads_typed((parsed["checkpoint_type"], checkpoint_bytes))
    # Use same serde for metadata
    metadata_bytes = base64.b64decode(parsed["metadata"])
    metadata = cast(CheckpointMetadata, serde.loads(metadata_bytes))
    return checkpoint, metadata


def serialize_write_data(channel: str, value: Any, serde: SerializerProtocol) -> str:
    """Serialize write data for storage."""
    type_, serialized_value = serde.dumps_typed(value)
    # Convert bytes to base64 string for JSON serialization
    value_b64 = base64.b64encode(serialized_value).decode("utf-8")

    data = {
        "channel": channel,
        "value_type": type_,
        "value": value_b64,
    }
    return json.dumps(data, indent=2)


def deserialize_write_data(data: str, serde: SerializerProtocol) -> tuple[str, Any]:
    """Deserialize write data from storage."""
    parsed = json.loads(data)
    channel = parsed["channel"]
    # Convert base64 string back to bytes
    value_bytes = base64.b64decode(parsed["value"])
    value = serde.loads_typed((parsed["value_type"], value_bytes))
    return channel, value
