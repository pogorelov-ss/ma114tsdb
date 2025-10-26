# Testing Guide for ma114tsdb

## Quick Start

### Run all tests
```bash
uv run pytest
```

### Run specific test suite
```bash
# Contract tests (adapter interface validation)
uv run pytest test/contract/

# Integration tests
uv run pytest test/integration/

# Unit tests
uv run pytest test/unit/
```

### Run single test file
```bash
uv run pytest test/contract/test_adapter_contract.py -v
```

### Run single test
```bash
uv run pytest test/contract/test_adapter_contract.py::TestIAdapterContract::test_init_success -v
```

## Test Organization

```
test/
├── contract/           # Contract tests for adapter interfaces
│   ├── test_adapter_contract.py      # IAdapter base tests (7 tests)
│   ├── test_producer_contract.py     # IProducer tests (8 tests)
│   ├── test_consumer_contract.py     # IConsumer tests (9 tests)
│   ├── test_processor_contract.py    # IProcessor tests (9 tests)
│   └── test_transport_contract.py    # ITransport tests (9 tests)
├── integration/        # End-to-end pipeline tests
└── unit/              # Component unit tests
```

## Current Test Status

**Contract Tests**: 42 tests (all SKIPPED - awaiting adapter implementations)

Following TDD approach:
1. ✅ Contract tests written FIRST (Phase 3.6)
2. ⏳ Adapter implementations (Phase 3.7+)
3. ⏳ Tests will PASS when implementations complete

## Test Output Modes

### Verbose mode
```bash
uv run pytest -v
```

### Show test output (print statements)
```bash
uv run pytest -s
```

### Stop on first failure
```bash
uv run pytest -x
```

### Show only failing tests
```bash
uv run pytest --tb=short
```

### Show full traceback
```bash
uv run pytest --tb=long
```

## Coverage Reports

```bash
# Run tests with coverage
uv run pytest --cov=components --cov-report=html

# View coverage report
open htmlcov/index.html
```

## Contract Test Usage

Contract tests validate that adapter implementations conform to interface contracts.

### For Adapter Developers

When implementing a new adapter (e.g., MyCustomProducer):

1. Create test file that inherits from contract tests:
```python
# test/unit/test_my_producer.py
from test.contract.test_producer_contract import TestIProducerContract
from my_module import MyCustomProducer

class TestMyCustomProducer(TestIProducerContract):
    @pytest.fixture
    def producer(self):
        return MyCustomProducer()

    @pytest.fixture
    def valid_config(self):
        return {"url": "http://sensor/data"}
```

2. Run tests:
```bash
uv run pytest test/unit/test_my_producer.py -v
```

3. All contract tests must PASS for valid implementation

## CI/CD Integration

### GitHub Actions (recommended)
```yaml
- name: Run tests
  run: uv run pytest --cov --cov-report=xml
```

### Pre-commit Hook
```bash
# .git/hooks/pre-commit
#!/bin/bash
uv run pytest test/contract/ -x
```

## Test Dependencies

- pytest>=8.0.0
- pytest-asyncio>=0.23.0 (for async adapter tests)
- hypothesis>=6.0.0 (for property-based testing)

Installed automatically via `uv sync`
