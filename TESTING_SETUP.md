# Multi-Python Version Testing Setup

This document describes the automated testing setup for Python versions 3.10 to 3.14.

## Overview

The project now supports automated testing across Python versions 3.10 to 3.14 using:
- **Hatch** for local development and testing
- **GitHub Actions** for CI/CD automation

## Local Development with Hatch

### Available Commands

```bash
# Run tests with current Python version
hatch run test

# Run tests with coverage
hatch run test-cov

# Run tests across all supported Python versions (3.10-3.14)
hatch run all:test

# Run tests with specific Python version
hatch run all.py3.11:test
hatch run all.py3.12:test
# ... etc for py3.10, py3.13, py3.14

# Code quality checks
hatch run lint:check      # Check formatting and linting
hatch run lint:format     # Auto-format code
hatch run type-check:check # Type checking with mypy

# View coverage report in browser
hatch run cov-report
```

### Hatch Environments

- **default**: Basic testing environment with pytest and coverage
- **all**: Matrix environment for testing across Python 3.10-3.14
- **lint**: Code formatting and linting (black, isort, flake8)
- **type-check**: Type checking with mypy and boto3-stubs
- **docs**: Documentation building with Sphinx

## GitHub Actions CI/CD

The project includes automated CI/CD via GitHub Actions (`.github/workflows/ci.yml`) that:

1. **Tests across Python versions 3.10-3.14**
   - Runs on every push to `main`/`develop` branches
   - Runs on all pull requests
   - Uses matrix strategy for parallel testing

2. **Quality checks**
   - Code formatting (black, isort)
   - Linting (flake8)
   - Type checking (mypy with boto3-stubs)

3. **Package validation**
   - Builds the package with hatch
   - Validates package integrity with twine
   - Tests package installation

4. **Coverage reporting**
   - Uploads coverage reports to Codecov
   - Generates HTML and XML coverage reports

## Configuration Files

### pyproject.toml
- Updated to include Python 3.14 in classifiers
- Configured hatch environments for multi-version testing
- Added boto3-stubs for proper type checking
- Configured tools (black, isort, mypy, pytest, coverage)

### GitHub Actions Workflow
- Matrix testing across Python 3.10-3.14
- Uses hatch for all operations
- Includes package building and validation
- Uploads coverage reports

## Type Safety

The project now includes proper type annotations with:
- `mypy_boto3_s3` for S3 client type hints
- `TYPE_CHECKING` imports to avoid runtime dependencies
- Strict mypy configuration for better code quality

## Testing Results

✅ All 19 tests pass across supported Python versions
✅ 92% code coverage
✅ Type checking passes with no issues
✅ Code formatting and linting passes
✅ Package builds and installs successfully

## Usage Examples

### Local Testing
```bash
# Test current Python version
hatch run test

# Test all Python versions (if available locally)
hatch run all:test

# Check code quality
hatch run lint:check
hatch run type-check:check
```

### CI/CD
The GitHub Actions workflow automatically runs on:
- Push to `main` or `develop` branches
- Pull requests to `main` or `develop` branches

## Benefits

1. **Comprehensive Testing**: Ensures compatibility across Python 3.10-3.14
2. **Early Detection**: Catches issues before they reach production
3. **Type Safety**: Strong type checking with boto3-stubs
4. **Code Quality**: Automated formatting and linting
5. **Easy Local Development**: Simple hatch commands for all operations
6. **CI/CD Integration**: Automated testing on GitHub
