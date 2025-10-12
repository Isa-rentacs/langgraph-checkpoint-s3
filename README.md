# LangGraph Checkpoint S3

A Python library for storing LangGraph checkpoints in Amazon S3, providing both synchronous and asynchronous APIs.

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

## CLI Tool

The package includes a command-line tool `s3-checkpoint` for reading and inspecting checkpoints stored in S3.

### Installation

The CLI tool is automatically installed when you install the package:

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

### Error Codes

The CLI tool uses standard exit codes:

- `0`: Success
- `1`: Invalid S3 URI format
- `2`: AWS credentials error
- `3`: S3 access error
- `4`: Checkpoint not found or other runtime error
- `5`: Unexpected error

## Required S3 Permissions

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

## Building the Package

This project uses modern Python packaging with `hatchling` as the build backend. Here are the steps to build and develop the package:

### Prerequisites

- Python 3.10 or higher
- pip (latest version recommended)

### Development Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Isa-rentacs/langgraph-checkpoint-s3.git
   cd langgraph-checkpoint-s3
   ```

2. **Install in development mode:**
   ```bash
   # Install the package in editable mode with development dependencies
   pip install -e ".[dev]"
   ```

3. **Verify installation:**
   ```bash
   # Test that the CLI tool is available
   s3-checkpoint --help
   
   # Test that the package can be imported
   python -c "from langgraph_checkpoint_s3 import S3CheckpointSaver; print('Import successful')"
   ```

### Building Distribution Packages

```bash
# Install hatch
pip install hatch

# Build the package
hatch build

# Build only wheel
hatch build --target wheel

# Build only source distribution
hatch build --target sdist
```

### Testing with Different Python Versions

Use hatch to test against multiple Python versions:

```bash
# Test against all configured Python versions (3.10-3.14)
hatch run all:test

# Test against specific Python version
hatch run +py=3.11 test
```

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
