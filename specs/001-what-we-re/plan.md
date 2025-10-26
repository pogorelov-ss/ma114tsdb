
# Implementation Plan: ma114tsdb Core System

**Branch**: `001-what-we-re` | **Date**: 2025-10-04 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/home/pss/AIExperiments/ma114tsdb/specs/001-what-we-re/spec.md`

## Execution Flow (/plan command scope)
```
1. Load feature spec from Input path
   → If not found: ERROR "No feature spec at {path}"
2. Fill Technical Context (scan for NEEDS CLARIFICATION)
   → Detect Project Type from file system structure or context (web=frontend+backend, mobile=app+api)
   → Set Structure Decision based on project type
3. Fill the Constitution Check section based on the content of the constitution document.
4. Evaluate Constitution Check section below
   → If violations exist: Document in Complexity Tracking
   → If no justification possible: ERROR "Simplify approach first"
   → Update Progress Tracking: Initial Constitution Check
5. Execute Phase 0 → research.md
   → If NEEDS CLARIFICATION remain: ERROR "Resolve unknowns"
6. Execute Phase 1 → contracts, data-model.md, quickstart.md, agent-specific template file (e.g., `CLAUDE.md` for Claude Code, `.github/copilot-instructions.md` for GitHub Copilot, `GEMINI.md` for Gemini CLI, `QWEN.md` for Qwen Code, or `AGENTS.md` for all other agents).
7. Re-evaluate Constitution Check section
   → If new violations: Refactor design, return to Phase 1
   → Update Progress Tracking: Post-Design Constitution Check
8. Plan Phase 2 → Describe task generation approach (DO NOT create tasks.md)
9. STOP - Ready for /tasks command
```

**IMPORTANT**: The /plan command STOPS at step 7. Phases 2-4 are executed by other commands:
- Phase 2: /tasks command creates tasks.md
- Phase 3-4: Implementation execution (manual or via tools)

## Summary

ma114tsdb is a modular time-series data acquisition system with pluggable adapter architecture. The system handles two distinct data types: high-volume DataStreams (lossy-tolerant) and critical Events (durable), routing them through configurable pipelines with processors and multiple transport options (Memory, IPC, MQTT, Kombu). Built-in observability, versioned contracts, and graceful failure handling ensure production reliability at 100-10,000 streams/second throughput.

## Technical Context
**Language/Version**: Python 3.11+
**Primary Dependencies**: Kombu (transport layer), FastStream (web API backend), AWS Greengrass Python client (IPC/MQTT), MessagePack (wire format), Prometheus client (metrics)
**Storage**: File-based configuration (v1.0), optional Dead Letter Queue storage (configurable backend)
**Testing**: pytest (unit/integration), contract testing framework
**Target Platform**: Linux (primary), containerized deployment (Docker, K8s)
**Project Type**: Polylith monorepo (single codebase, multiple deployable components)
**Build System**: NX (task orchestration, caching, dependency graph)
**Performance Goals**: 100-10,000 DataStreams/second per pipeline, 1-5 minute time window buffering, <100ms adapter lifecycle operations
**Constraints**: Language-agnostic transport layer, N-1 version compatibility, graceful degradation on adapter failure
**Scale/Scope**: Multiple concurrent pipelines, 4 adapter types (Producer, Consumer, Processor, Transport), cross-language adapter support
**Infrastructure**:
- Per-project infrastructure (cdk/, k8s/, k8s.cdk/ subfolders in projects/)
- Core infrastructure projects: `inf_core_network`, `inf_core_storage`
- Shared infrastructure projects: `inf_shared_monitoring`
- Development projects: `dev_local_cluster`, `dev_testing_utils`
- AWS CDK (cloud infrastructure), AWS GDK (Greengrass components), k8s.cdk (Kubernetes)
- Docker Desktop + k3d (local dev environment)

## Constitution Check
*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### I. Time is First-Class
- [x] All data models include timestamp fields (FR-002, FR-003: sub-second precision mandatory)
- [x] Time-series query operations are supported (DataStream batching, time window prioritization)
- [x] Temporal ordering is preserved throughout pipeline (FR-026: FIFO for Events, latest-first for DataStreams)

### II. Everything is an Adapter
- [x] Components implement standard adapter interfaces (FR-007: 4 adapter roles with uniform IAdapter base)
- [x] Adapters are composable and substitutable (FR-014: metadata-driven capabilities)
- [x] No hard-coded dependencies on specific implementations (transport-agnostic design, FR-018)

### III. Observable by Default
- [x] Telemetry collection configured (FR-030: mandatory metrics per adapter)
- [x] Structured logging implemented (FR-031: timestamp, level, adapter-id, context)
- [x] Distributed tracing spans defined (FR-032: TraceContext propagation)

### IV. Programmed with Strict Contracts
- [x] Adapter interfaces defined in code (Python protocols/ABCs for IAdapter, IProducer, IConsumer, IProcessor, ITransport)
- [x] Type safety enforced (Python type hints, runtime validation with pydantic)
- [x] Contract tests written for all adapters (Phase 1 deliverable)

### V. Versioned Evolution
- [x] Interface versions specified (FR-045: semantic versioning for all contracts)
- [x] Data format versions documented (FR-005: major.minor in DataStream/Event)
- [x] Migration paths defined for version changes (FR-046: N-1 compatibility, deprecation warnings)

## Project Structure

### Documentation (this feature)
```
specs/[###-feature]/
├── plan.md              # This file (/plan command output)
├── research.md          # Phase 0 output (/plan command)
├── data-model.md        # Phase 1 output (/plan command)
├── quickstart.md        # Phase 1 output (/plan command)
├── contracts/           # Phase 1 output (/plan command)
└── tasks.md             # Phase 2 output (/tasks command - NOT created by /plan)
```

### Source Code (repository root)

Polylith monorepo structure with uv package manager and NX build system:

```
ma114tsdb/
├── nx.json                  # NX workspace configuration
├── project.json             # Root project configuration for NX
├── workspace.toml           # Polylith workspace configuration
├── pyproject.toml           # uv workspace root
├── components/              # Reusable components (bricks)
│   ├── core/
│   │   ├── models/          # DataStream, Event, TraceContext data models
│   │   ├── interfaces/      # IAdapter, IProducer, IConsumer, IProcessor, ITransport
│   │   └── versioning/      # Semantic version handling, compatibility checks
│   ├── transports/
│   │   ├── memory/          # In-memory transport
│   │   ├── ipc/             # Unix socket IPC (AWS Greengrass)
│   │   ├── mqtt/            # MQTT transport (AWS Greengrass)
│   │   └── kombu/           # Kombu transport (RabbitMQ, Redis, SQS)
│   ├── observability/
│   │   ├── metrics/         # Prometheus metrics collection
│   │   ├── logging/         # Structured logging (JSON/EDN formatters)
│   │   └── tracing/         # Distributed tracing propagation
│   ├── pipeline/
│   │   ├── runtime/         # Pipeline execution engine
│   │   ├── config/          # Configuration file loading
│   │   └── lifecycle/       # Adapter lifecycle management
│   └── serialization/
│       └── msgpack/         # MessagePack encoding/decoding
├── bases/                   # Application entry points
│   ├── api/                 # FastStream web API backend
│   └── runtime/             # Main pipeline runtime
├── projects/                # Deployable artifacts with per-project infrastructure
│   ├── ma114tsdb-runtime/
│   │   ├── project.json     # NX project configuration
│   │   ├── src/             # Runtime application code
│   │   ├── cdk/             # AWS CDK for runtime infrastructure (if needed)
│   │   ├── k8s/             # Kubernetes manifests (if needed)
│   │   └── k8s.cdk/         # k8s.cdk definitions (if needed)
│   ├── ma114tsdb-api/
│   │   ├── project.json     # NX project configuration
│   │   ├── src/             # API application code
│   │   ├── cdk/             # AWS CDK for API infrastructure (if needed)
│   │   └── k8s/             # Kubernetes manifests (if needed)
│   ├── inf_core_network/    # Core infrastructure: VPC, subnets, security groups
│   │   ├── project.json
│   │   └── cdk/             # AWS CDK infrastructure definitions
│   ├── inf_core_storage/    # Core infrastructure: databases, S3, persistent storage
│   │   ├── project.json
│   │   └── cdk/
│   ├── inf_shared_monitoring/  # Shared infrastructure: Prometheus, Grafana, logging
│   │   ├── project.json
│   │   ├── cdk/
│   │   └── k8s/
│   ├── dev_local_cluster/   # Development project: k3d cluster setup
│   │   ├── project.json
│   │   └── scripts/
│   └── dev_testing_utils/   # Development project: testing utilities, mocks
│       ├── project.json
│       └── src/
├── development/
│   ├── adapters-examples/   # Example producer/consumer/processor adapters
│   └── configs/             # Sample pipeline configurations
└── test/
    ├── contract/            # Contract tests for adapter interfaces
    ├── integration/         # End-to-end pipeline tests
    └── unit/                # Component unit tests
```

**Structure Decision**: Polylith monorepo with NX build orchestration chosen for:
- **Polylith benefits**: Maximum code reuse, independent component evolution
- **NX benefits**: Task caching, parallel execution, dependency graph analysis
- **Infrastructure co-location**: Each project contains its own infrastructure (cdk/, k8s/) only when needed
- **Naming conventions**:
  - `inf_core_*`: Core shared infrastructure (network, storage)
  - `inf_shared_*`: Shared services (monitoring, observability)
  - `dev_*`: Development-only projects (local cluster, testing tools)
- **Build system**: NX manages CI/CD for local and cloud environments
  - One `project.json` per deployable project
  - Root `project.json` for workspace-level tasks
  - `nx.json` for workspace configuration and task runners

**Architecture benefits**:
- Cross-language adapter compatibility (components expose stable interfaces)
- Independent versioning of transport implementations
- Shared observability components across all adapters
- Simplified testing (components tested in isolation, integration tests at project level)
- Infrastructure as code co-located with applications
- Efficient CI/CD with NX task caching and affected project detection

## Phase 0: Outline & Research
1. **Extract unknowns from Technical Context** above:
   - For each NEEDS CLARIFICATION → research task
   - For each dependency → best practices task
   - For each integration → patterns task

2. **Generate and dispatch research agents**:
   ```
   For each unknown in Technical Context:
     Task: "Research {unknown} for {feature context}"
   For each technology choice:
     Task: "Find best practices for {tech} in {domain}"
   ```

3. **Consolidate findings** in `research.md` using format:
   - Decision: [what was chosen]
   - Rationale: [why chosen]
   - Alternatives considered: [what else evaluated]

**Output**: research.md with all NEEDS CLARIFICATION resolved

## Phase 1: Design & Contracts
*Prerequisites: research.md complete*

1. **Extract entities from feature spec** → `data-model.md`:
   - Entity name, fields, relationships
   - Validation rules from requirements
   - State transitions if applicable

2. **Generate API contracts** from functional requirements:
   - For each user action → endpoint
   - Use standard REST/GraphQL patterns
   - Output OpenAPI/GraphQL schema to `/contracts/`

3. **Generate contract tests** from contracts:
   - One test file per endpoint
   - Assert request/response schemas
   - Tests must fail (no implementation yet)

4. **Extract test scenarios** from user stories:
   - Each story → integration test scenario
   - Quickstart test = story validation steps

5. **Update agent file incrementally** (O(1) operation):
   - Run `.specify/scripts/bash/update-agent-context.sh claude`
     **IMPORTANT**: Execute it exactly as specified above. Do not add or remove any arguments.
   - If exists: Add only NEW tech from current plan
   - Preserve manual additions between markers
   - Update recent changes (keep last 3)
   - Keep under 150 lines for token efficiency
   - Output to repository root

**Output**: data-model.md, /contracts/*, failing tests, quickstart.md, agent-specific file

## Phase 2: Task Planning Approach
*This section describes what the /tasks command will do - DO NOT execute during /plan*

**Task Generation Strategy**:
- Load `.specify/templates/tasks-template.md` as base
- Generate tasks from Phase 1 design docs (contracts, data model, quickstart)
- Workspace setup (Polylith, uv, NX) → setup tasks [P]
- NX project configurations (nx.json, project.json files) → setup tasks [P]
- Core data types (DataStream, Event, TraceContext, Version) → model tasks [P]
- Adapter interfaces (IAdapter, IProducer, IConsumer, IProcessor, ITransport) → interface definition tasks [P]
- Each interface → contract test task [P]
- Transport implementations (Memory, IPC, MQTT, Kombu) → adapter tasks
- Observability components (metrics, logging, tracing) → integration tasks [P]
- Pipeline runtime → lifecycle management task
- Configuration loader → file parsing task
- Each quickstart scenario → integration test task
- Infrastructure projects (inf_core_*, inf_shared_*, dev_*) → infrastructure tasks [P]

**Ordering Strategy**:
- TDD order: Contract tests before implementations
- Dependency order:
  1. Setup (Polylith workspace, uv, NX workspace configuration)
  2. NX project.json files (root + per-project configurations)
  3. Core infrastructure projects (inf_core_network, inf_core_storage)
  4. Core models (DataStream, Event, TraceContext, Version)
  5. Adapter interfaces (IAdapter hierarchy)
  6. Contract tests for interfaces (must fail initially)
  7. Observability components (can be parallel with transports)
  8. Transport implementations (Memory → IPC → MQTT → Kombu)
  9. Pipeline runtime and lifecycle management
  10. Configuration loader
  11. Shared infrastructure projects (inf_shared_monitoring)
  12. Development projects (dev_local_cluster, dev_testing_utils)
  13. Integration tests (quickstart scenarios)
  14. Per-project infrastructure (cdk/, k8s/ folders in ma114tsdb-runtime, ma114tsdb-api)
- Mark [P] for parallel execution (independent components in Polylith, independent NX projects)

**Polylith + NX task organization**:
- Component tasks can execute in parallel (bricks are independent)
- Base tasks depend on component completion
- Project tasks depend on bases
- Tests can run in parallel per component
- NX manages task graph and caching (only run affected tasks)
- Infrastructure projects can be deployed independently via NX

**Estimated Output**: 50-60 numbered, ordered tasks in tasks.md

Tasks breakdown by category:
- Setup & Structure: 8 tasks (Polylith, uv, NX workspace, nx.json, root project.json)
- NX Project Configurations: 7 tasks (project.json for each deployable project)
- Core Infrastructure: 4 tasks (inf_core_network, inf_core_storage CDK stacks)
- Core Models: 8 tasks (4 models + 4 contract tests)
- Adapter Interfaces: 10 tasks (5 interfaces + 5 contract tests)
- Observability: 6 tasks (metrics, logging, tracing + tests)
- Transports: 12 tasks (4 implementations × 3 tasks each)
- Pipeline Runtime: 5 tasks
- Configuration: 3 tasks
- Shared Infrastructure: 3 tasks (inf_shared_monitoring)
- Development Projects: 4 tasks (dev_local_cluster, dev_testing_utils)
- Integration Tests: 8 tasks (6 quickstart scenarios + 2 performance tests)
- Per-Project Infrastructure: 4 tasks (runtime/api CDK + k8s manifests)

**IMPORTANT**: This phase is executed by the /tasks command, NOT by /plan

## Phase 3+: Future Implementation
*These phases are beyond the scope of the /plan command*

**Phase 3**: Task execution (/tasks command creates tasks.md)  
**Phase 4**: Implementation (execute tasks.md following constitutional principles)  
**Phase 5**: Validation (run tests, execute quickstart.md, performance validation)

## Complexity Tracking
*Fill ONLY if Constitution Check has violations that must be justified*

**No violations detected.** All constitutional principles satisfied:
- ✅ Time is First-Class: DataStream/Event carry sub-second timestamps
- ✅ Everything is an Adapter: Uniform IAdapter interface for all 4 roles
- ✅ Observable by Default: Prometheus metrics, structlog, trace propagation
- ✅ Programmed with Strict Contracts: Python protocols/ABCs, pydantic validation
- ✅ Versioned Evolution: Semantic versioning with N-1 compatibility checks


## Progress Tracking
*This checklist is updated during execution flow*

**Phase Status**:
- [x] Phase 0: Research complete (/plan command)
- [x] Phase 1: Design complete (/plan command)
- [x] Phase 2: Task planning complete (/plan command - describe approach only)
- [ ] Phase 3: Tasks generated (/tasks command)
- [ ] Phase 4: Implementation complete
- [ ] Phase 5: Validation passed

**Gate Status**:
- [x] Initial Constitution Check: PASS (all 5 principles satisfied)
- [x] Post-Design Constitution Check: PASS (no violations)
- [x] All NEEDS CLARIFICATION resolved (5 clarifications documented)
- [x] Complexity deviations documented (none - all principles aligned)

---
*Based on Constitution v1.0.0 - See `.specify/memory/constitution.md`*
