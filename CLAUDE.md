# ma114tsdb Development Guidelines

Auto-generated from all feature plans. Last updated: 2025-10-05

## Active Technologies
- Python 3.11+ + Kombu (transport layer), FastStream (web API backend), AWS Greengrass Python client (IPC/MQTT), MessagePack (wire format), Prometheus client (metrics) (001-what-we-re)

## Project Structure

Polylith monorepo with NX build system:

```
components/               # Reusable components (bricks)
├── core/                # Core models and interfaces
│   ├── interfaces/      # IAdapter, IProducer, IConsumer, IProcessor, ITransport
│   ├── models/          # DataStream, Event, TraceContext, PipelineConfig
│   └── versioning/      # Semantic versioning
├── pipeline/            # Pipeline execution engine
│   ├── lifecycle/       # Adapter lifecycle manager (retry, circuit breaker)
│   └── runtime/         # Pipeline runtime engine
├── observability/       # Metrics, logging, tracing
├── serialization/       # MessagePack serialization
└── transports/          # Memory, IPC, MQTT, Kombu transports

bases/                   # Application entry points
├── api/                 # FastStream API (future)
└── runtime/             # Pipeline runtime main

projects/                # Deployable artifacts + infrastructure
├── ma114tsdb-runtime/   # Runtime application
├── ma114tsdb-api/       # API application
├── inf_core_*/          # Core infrastructure (network, storage)
├── inf_shared_*/        # Shared infrastructure (monitoring)
└── dev_*/               # Development utilities

test/
├── contract/            # Interface contract tests (42 tests)
├── integration/         # End-to-end scenarios
└── unit/                # Component tests (23 tests)
```

## Commands

### Testing
```bash
# Run all tests (154 total)
uv run pytest

# Run specific test suites
uv run pytest test/unit/          # Unit tests (23 passing)
uv run pytest test/contract/      # Contract tests (42 skipped - awaiting implementations)
uv run pytest test/integration/   # Integration tests

# Run with verbose output
uv run pytest -v

# See TESTING.md for comprehensive testing guide
```

### Development
```bash
# Install dependencies
uv sync

# Lint code
ruff check .

# Format code
ruff format .
```

## Code Style
Python 3.11+: Follow standard conventions, use type hints, async/await patterns

## Recent Changes
- 2025-10-05: Phase 3.10 complete - Pipeline runtime & lifecycle components (T062-T065)
- 2025-10-04: Phase 3.9 complete - Transport implementations (Memory, IPC, MQTT, Kombu)
- 2025-10-04: Phase 3.7-3.8 complete - Observability and serialization components

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->