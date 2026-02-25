# Backend Codebase Summary

## Project Structure

### Main Application (backend/app)

#### Executors (backend/app/executors)
- **llm.py**: LLM executor for running language model tasks
- **llm_ollama.py**: Ollama-specific LLM implementation
- **llm_openai_compat.py**: OpenAI-compatible LLM implementation
- **source.py**: Source executor for data ingestion
- **tool.py**: Tool executor for running tools
- **transform.py**: Transform executor for data transformations (currently being fixed)

#### Main Entry Points
- **main.py**: FastAPI application setup
- **runtime.py**: Runtime manager for execution

#### Routes (backend/app/routes)
- **runs.py**: API endpoints for run management

#### Runner (backend/app/runner)
- **artifacts.py**: Artifact storage interface and implementations
- **cache.py**: Execution cache
- **compile.py**: DAG compilation
- **emit.py**: Event emission utilities
- **events.py**: Event bus for run events
- **materialize.py**: Artifact materialization
- **metadata.py**: Metadata models and execution context
- **run.py**: Run execution engine
- **schemas.py**: Data schemas
- **validator.py**: Validation logic

#### Tools (backend/app/tools)
- **providers/mcp.py**: MCP server provider

## Current State

### What Has Been Completed
1. **Core Infrastructure**:
   - FastAPI application setup
   - Artifact storage (memory-based)
   - Event bus system
   - Execution cache
   - DAG compilation

2. **Executors**:
   - LLM executors (Ollama and OpenAI-compatible)
   - Source executor
   - Tool executor
   - Transform executor (in progress)

3. **Runner System**:
   - Run execution engine
   - Metadata management
   - Validation
   - Materialization

### Work Yet to be Done

1. **Transform Executor Fixes** (Current Focus):
   - Fix context validation (avoid awaiting on None)
   - Fix input metadata handling
   - Fix artifact reading from store
   - Fix DataFrame loading from bytes
   - Fix test expectations

2. **Testing**:
   - Update tests to use AsyncMock properly
   - Provide valid test data (parquet files)
   - Verify all test assertions match implementation

3. **Potential Issues**:
   - The transform.py code uses `eval()` which is dangerous
   - Some error handling could be improved
   - Tests need proper async setup

## Key Files for Current Task

1. **backend/app/executors/transform.py** - Main file being fixed
2. **backend/tests/executors/test_transform.py** - Tests that need updating
3. **backend/app/runner/artifacts.py** - ArtifactStore interface
4. **backend/app/runner/events.py** - RunEventBus interface

## Summary

The project is a data pipeline/execution framework with:
- FastAPI backend
- Async execution model
- Artifact storage and caching
- Event-based architecture
- Multiple executor types (LLM, source, tool, transform)

The current task is to fix the transform executor and its tests to handle edge cases properly and use async/await correctly throughout.

## Runtime Event Contract Notes

- `cache_decision` events include `schema_version: 1`.
- `cache_decision.reason` is required in emitted events; the runner resolves a default reason when one is not provided at call sites.
- Keep `schema_version` at `1` for additive/non-breaking event changes, and bump only for breaking payload changes.
