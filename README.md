# LangGraph Checkpoint S3

A Python library for storing LangGraph checkpoints in Amazon S3, providing both synchronous and asynchronous APIs.

## Features

- **Full LangGraph Compatibility**: Implements the complete `BaseCheckpointSaver` interface
- **Sync and Async Support**: Both `S3CheckpointSaver` and `AsyncS3CheckpointSaver` implementations
- **Smart Namespace Handling**: Automatically handles empty checkpoint namespaces with `__default__` directory
- **Hierarchical Storage**: Organized S3 structure for efficient checkpoint and writes management
- **Robust Error Handling**: Comprehensive error handling with proper S3 exception management
- **Efficient Operations**: Uses S3 pagination and batch operations for optimal performance

## Installation

```bash
pip install langgraph-checkpoint-s3
```

## Quick Start

### Synchronous Usage

```python
import boto3
from langgraph_checkpoint_s3 import S3CheckpointSaver
from langgraph.graph import StateGraph

# Create S3 client
s3_client = boto3.client('s3')

# Initialize the checkpoint saver
checkpointer = S3CheckpointSaver(
    bucket_name="my-checkpoints-bucket",
    prefix="my-app/checkpoints/",
    s3_client=s3_client
)

# Use with LangGraph
builder = StateGraph(dict)
builder.add_node("step1", lambda x: {"value": x["value"] + 1})
builder.set_entry_point("step1")
builder.set_finish_point("step1")

graph = builder.compile(checkpointer=checkpointer)

# Run with checkpointing
config = {"configurable": {"thread_id": "thread-1"}}
result = graph.invoke({"value": 1}, config)
print(result)  # {"value": 2}

# Continue from checkpoint
result = graph.invoke({"value": 10}, config)
print(result)  # Continues from previous state
```

### Asynchronous Usage

```python
import aioboto3
from langgraph_checkpoint_s3 import AsyncS3CheckpointSaver
from langgraph.graph import StateGraph

async def main():
    # Create aioboto3 session
    session = aioboto3.Session()
    
    # Use as async context manager
    async with AsyncS3CheckpointSaver(
        bucket_name="my-checkpoints-bucket",
        prefix="my-app/checkpoints/",
        session=session
    ) as checkpointer:
        
        # Build graph
        builder = StateGraph(dict)
        builder.add_node("step1", lambda x: {"value": x["value"] + 1})
        builder.set_entry_point("step1")
        builder.set_finish_point("step1")
        
        graph = builder.compile(checkpointer=checkpointer)
        
        # Run with checkpointing
        config = {"configurable": {"thread_id": "thread-1"}}
        result = await graph.ainvoke({"value": 1}, config)
        print(result)  # {"value": 2}

# Run the async function
import asyncio
asyncio.run(main())
```

## S3 Storage Structure

The library organizes data in S3 using the following structure:

```
s3://your-bucket/your-prefix/
├── checkpoints/
│   └── {thread_id}/
│       └── {checkpoint_ns}/     # "__default__" for empty namespace
│           └── {checkpoint_id}.json
└── writes/
    └── {thread_id}/
        └── {checkpoint_ns}/     # "__default__" for empty namespace
            └── {checkpoint_id}/
                └── {task_id}_{idx}.json
```

### Namespace Handling

- Empty or `None` checkpoint namespaces are stored as `__default__`
- This avoids issues with empty directory names in S3
- The `__default__` name is unlikely to conflict with user-defined namespaces

## API Reference

### S3CheckpointSaver

Synchronous checkpoint saver for Amazon S3.

```python
S3CheckpointSaver(
    bucket_name: str,
    *,
    prefix: str = "checkpoints/",
    s3_client: Optional[boto3.client] = None,
    **kwargs
)
```

**Parameters:**
- `bucket_name`: S3 bucket name for storing checkpoints
- `prefix`: Optional prefix for all S3 keys (default: "checkpoints/")
- `s3_client`: Optional boto3 S3 client (creates one if not provided)
- `**kwargs`: Additional arguments passed to `BaseCheckpointSaver`

**Methods:**
- `get_tuple(config)`: Retrieve a checkpoint tuple
- `list(config, *, filter=None, before=None, limit=None)`: List checkpoints
- `put(config, checkpoint, metadata, new_versions)`: Store a checkpoint
- `put_writes(config, writes, task_id, task_path="")`: Store intermediate writes
- `delete_thread(thread_id)`: Delete all data for a thread
- `get_next_version(current, channel)`: Generate next version ID

### AsyncS3CheckpointSaver

Asynchronous checkpoint saver for Amazon S3.

```python
AsyncS3CheckpointSaver(
    bucket_name: str,
    *,
    prefix: str = "checkpoints/",
    session: Optional[aioboto3.Session] = None,
    **kwargs
)
```

**Parameters:**
- `bucket_name`: S3 bucket name for storing checkpoints
- `prefix`: Optional prefix for all S3 keys (default: "checkpoints/")
- `session`: Optional aioboto3 session (creates one if not provided)
- `**kwargs`: Additional arguments passed to `BaseCheckpointSaver`

**Async Methods:**
- `aget_tuple(config)`: Retrieve a checkpoint tuple
- `alist(config, *, filter=None, before=None, limit=None)`: List checkpoints
- `aput(config, checkpoint, metadata, new_versions)`: Store a checkpoint
- `aput_writes(config, writes, task_id, task_path="")`: Store intermediate writes
- `adelete_thread(thread_id)`: Delete all data for a thread

**Context Manager:**
The async version must be used as an async context manager:

```python
async with AsyncS3CheckpointSaver("bucket") as checkpointer:
    # Use checkpointer here
    pass
```

## CLI Tool

The package includes a command-line tool `s3-checkpoint` for reading and inspecting checkpoints stored in S3.

### Installation

The CLI tool is automatically installed when you install the package:

```bash
pip install langgraph-checkpoint-s3
```

### Usage

The CLI tool provides three main commands:

#### List Checkpoints

List all (checkpoint_ns, checkpoint_id) pairs for a thread:

```bash
s3-checkpoint list --s3-prefix s3://my-bucket/checkpoints/ --thread-id thread123
```

Output:
```json
{
  "thread_id": "thread123",
  "checkpoints": [
    {"checkpoint_ns": "", "checkpoint_id": "checkpoint1"},
    {"checkpoint_ns": "namespace1", "checkpoint_id": "checkpoint2"}
  ]
}
```

#### Dump Specific Checkpoint

Dump a specific checkpoint object with full data:

```bash
s3-checkpoint dump --s3-prefix s3://my-bucket/checkpoints/ --thread-id thread123 --checkpoint-ns "" --checkpoint-id checkpoint1
```

Output:
```json
{
  "thread_id": "thread123",
  "checkpoint_ns": "",
  "checkpoint_id": "checkpoint1",
  "checkpoint": { /* full checkpoint object */ },
  "metadata": { /* checkpoint metadata */ },
  "pending_writes": [ /* associated writes */ ]
}
```

#### Read All Checkpoints

Read all checkpoints for a thread with their full data:

```bash
s3-checkpoint read --s3-prefix s3://my-bucket/checkpoints/ --thread-id thread123
```

Output:
```json
{
  "thread_id": "thread123",
  "checkpoints": [
    {
      "checkpoint_ns": "",
      "checkpoint_id": "checkpoint1",
      "checkpoint": { /* checkpoint object */ },
      "metadata": { /* metadata */ },
      "pending_writes": [ /* writes */ ]
    }
  ]
}
```

### CLI Options

- `--s3-prefix`: S3 prefix in format `s3://bucket/prefix/` (required)
- `--profile`: AWS profile to use for authentication (optional)
- `--thread-id`: Thread ID to operate on (required for all commands)
- `--checkpoint-ns`: Checkpoint namespace (required for dump command, use empty string for default)
- `--checkpoint-id`: Checkpoint ID (required for dump command)

### AWS Authentication for CLI

The CLI tool uses the standard AWS credential chain:

1. `--profile` parameter (if specified)
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
3. AWS credentials file (`~/.aws/credentials`)
4. IAM roles (for EC2/ECS/Lambda)

Example with AWS profile:
```bash
s3-checkpoint read --s3-prefix s3://my-bucket/checkpoints/ --thread-id thread123 --profile my-aws-profile
```

### Error Codes

The CLI tool uses standard exit codes:

- `0`: Success
- `1`: Invalid S3 URI format
- `2`: AWS credentials error
- `3`: S3 access error
- `4`: Checkpoint not found or other runtime error
- `5`: Unexpected error

## Configuration

### AWS Credentials

The library uses boto3/aioboto3 for S3 access. Configure AWS credentials using any of the standard methods:

1. **Environment Variables:**
   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_DEFAULT_REGION=us-east-1
   ```

2. **AWS Credentials File:**
   ```ini
   # ~/.aws/credentials
   [default]
   aws_access_key_id = your_access_key
   aws_secret_access_key = your_secret_key
   ```

3. **IAM Roles** (recommended for EC2/ECS/Lambda)

4. **Custom S3 Client:**
   ```python
   import boto3
   
   s3_client = boto3.client(
       's3',
       aws_access_key_id='your_access_key',
       aws_secret_access_key='your_secret_key',
       region_name='us-east-1'
   )
   
   checkpointer = S3CheckpointSaver("bucket", s3_client=s3_client)
   ```

### Required S3 Permissions

Your AWS credentials need the following S3 permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        }
    ]
}
```

## Error Handling

The library provides comprehensive error handling:

- **Bucket Access**: Validates bucket existence and permissions on initialization
- **Not Found**: Returns `None` for missing checkpoints instead of raising exceptions
- **S3 Errors**: Wraps S3 client errors with descriptive messages
- **Serialization**: Handles serialization/deserialization errors gracefully

## Performance Considerations

- **Pagination**: Uses S3 pagination for listing operations to handle large numbers of checkpoints
- **Batch Operations**: Deletes multiple objects in batches for efficient cleanup
- **Async Concurrency**: Async version uploads writes concurrently for better performance
- **Prefix Organization**: Hierarchical structure enables efficient prefix-based operations

## Development

### Running Tests

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=src/langgraph_checkpoint_s3 --cov-report=html
```

### Code Quality

```bash
# Format code
black src tests
isort src tests

# Lint code
flake8 src tests

# Type checking
mypy src
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Changelog

### 0.1.0

- Initial release
- Sync and async S3 checkpoint savers
- Full LangGraph BaseCheckpointSaver compatibility
- Smart namespace handling with `__default__` for empty namespaces
- CLI tool `s3-checkpoint` for reading and inspecting checkpoints
- AWS profile support for CLI authentication
- Comprehensive test coverage
- Complete documentation
