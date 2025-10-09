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

#### Using pip (recommended)

```bash
# Install build dependencies
pip install build

# Build source distribution and wheel
python -m build

# This creates:
# - dist/langgraph_checkpoint_s3-0.1.0.tar.gz (source distribution)
# - dist/langgraph_checkpoint_s3-0.1.0-py3-none-any.whl (wheel)
```

#### Using hatch (alternative)

If you prefer using hatch directly:

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

### Development Workflows

#### Running Tests

```bash
# Run all tests
pytest

# Run tests with coverage report
pytest --cov=src/langgraph_checkpoint_s3 --cov-report=html

# Run tests for specific module
pytest tests/test_checkpoint.py

# Run async tests specifically
pytest tests/test_s3_checkpoint.py -k "async"
```

#### Code Quality and Formatting

```bash
# Format code with ruff
ruff format src tests

# Lint and fix issues
ruff check --fix src tests

# Just check without fixing
ruff check src tests

# Type checking
mypy src
```

#### Using Hatch Environments

This project is configured with hatch environments for different tasks:

```bash
# Run tests in hatch environment
hatch run test

# Run tests with coverage
hatch run test-cov

# Open coverage report in browser
hatch run cov-report

# Lint code
hatch run lint:check

# Format code
hatch run lint:format

# Type checking
hatch run type-check:check
```

#### Pre-commit Hooks

Set up pre-commit hooks for automatic code quality checks:

```bash
# Install pre-commit
pip install pre-commit

# Install the git hook scripts
pre-commit install

# Run against all files (optional)
pre-commit run --all-files
```

### Testing with Different Python Versions

Use hatch to test against multiple Python versions:

```bash
# Test against all configured Python versions (3.10-3.14)
hatch run all:test

# Test against specific Python version
hatch run +py=3.11 test
```

### Publishing the Package

#### To Test PyPI (recommended for testing)

```bash
# Install twine
pip install twine

# Build the package
python -m build

# Upload to Test PyPI
twine upload --repository testpypi dist/*

# Test installation from Test PyPI
pip install --index-url https://test.pypi.org/simple/ langgraph-checkpoint-s3
```

#### To Production PyPI

```bash
# Upload to PyPI (requires proper credentials)
twine upload dist/*
```

### Environment Variables for Development

For testing with real S3, set up these environment variables:

```bash
# AWS credentials (if not using AWS CLI profiles)
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1

# Test bucket for integration tests
export TEST_S3_BUCKET=your-test-bucket
export TEST_S3_PREFIX=test-checkpoints/
```

### Troubleshooting Build Issues

#### Common Issues

1. **Missing build dependencies:**
   ```bash
   pip install --upgrade pip setuptools wheel build
   ```

2. **Import errors during development:**
   ```bash
   # Reinstall in development mode
   pip install -e ".[dev]" --force-reinstall
   ```

3. **Type checking errors:**
   ```bash
   # Install type stubs
   pip install types-aioboto3[s3] boto3-stubs[s3]
   ```

4. **Test failures:**
   ```bash
   # Clear pytest cache
   pytest --cache-clear
   
   # Run tests with verbose output
   pytest -v
   ```

## Development

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
