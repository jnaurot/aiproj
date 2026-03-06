# Dev-Only Features Playbook

Purpose: quick reference for identifying dev/debug-only behavior and safely hiding/removing it for production.

## Quick Inventory Commands

Run from repo root:

```powershell
rg "console\.debug|print\(|\[debug|DEV_MODE|maintenance|cache/config|external-schema|resolve-source|RUNTIME_STRICT|ENABLE_MAINTENANCE_ENDPOINTS" src backend -n
```

```powershell
rg "global_cache_enabled|cache_enabled" src backend -n
```

## Current Dev-Only Features

### 1) Global Cache Toggle (UI + API)
- Purpose: debugging cache behavior across entire project.
- Locations:
  - `src/lib/flow/FlowCanvas.svelte` (`Cache` checkbox in top bar)
  - `src/lib/flow/client/runs.ts` (`getGlobalCacheConfig`, `setGlobalCacheConfig`)
  - `backend/app/routes/runs.py` (`GET/PUT /runs/cache/config`)
  - `backend/app/runtime.py` (`global_cache_enabled`)
  - `backend/app/runner/run.py` (runtime gate in `use_cache_for_node`)
- Hide (keep code):
  - Remove/hide checkbox in `FlowCanvas.svelte`.
  - Keep backend endpoint but restrict with auth if needed.
- Remove (full):
  - Delete `/runs/cache/config` endpoints.
  - Remove client helpers in `runs.ts`.
  - Remove `global_cache_enabled` from runtime.
  - Remove `runtime_cache_enabled` condition in `run.py`.

### 2) Maintenance Endpoints
- Purpose: GC, event prune, storage reset for ops/dev cleanup.
- Location: `backend/app/routes/maintenance.py`, mounted in `backend/app/main.py`.
- Gate: `ENABLE_MAINTENANCE_ENDPOINTS`.
- Hide (recommended for prod):
  - Set `ENABLE_MAINTENANCE_ENDPOINTS=false` (or unset).
- Remove (full):
  - Stop including router in `main.py`.
  - Delete `maintenance.py` routes if not needed.

### 3) External Schema Console Emission
- Purpose: print typed schema + sample for external data debugging.
- Prefix: `[external-schema]`
- Location: `backend/app/runner/run.py` (`_emit_external_schema_debug`).
- Hide:
  - Wrap call in env flag, e.g. `ENABLE_EXTERNAL_SCHEMA_DEBUG`.
  - Default flag off for production.
- Remove:
  - Delete emitter function and invocation block.

### 4) Source Resolve Debug Endpoint Logs
- Purpose: debug source cache resolve behavior.
- Prefix: `[resolve-source]`
- Location: `backend/app/routes/runs.py` (`/runs/resolve/source` handler).
- Hide:
  - Leave endpoint, remove print line.
- Remove:
  - Remove endpoint if no longer needed by frontend/source flow.

### 5) Runtime Regression/Debug Prints
- Purpose: execution/binding/cache diagnostics.
- Examples:
  - `[binding-regression]`, `[invalidation-regression]`, `[debug-exec-inputs]`, `[debug-exec-key]`, `[cache-hit]`, `[artifact]`
- Locations:
  - `backend/app/runtime.py`
  - `backend/app/runner/run.py`
  - 일부 executor/validator/schema files with raw `print(...)`.
- Hide:
  - Convert to `logger.debug(...)` and set production log level to INFO/WARN.
  - Gate critical diagnostics with env flags.
- Remove:
  - Delete one-off debug print statements after stabilization.

### 6) Frontend DEV_MODE Console Diagnostics
- Purpose: local graph/editor diagnostics.
- Locations:
  - `src/lib/flow/store/graphStore.ts` (`DEV_MODE` + `console.debug`)
  - `src/lib/flow/components/editors/TransformEditor/TransformEditor.svelte`
  - `src/lib/flow/components/editors/TransformEditor/TransformFilterEditor.svelte`
- Hide:
  - Keep `DEV_MODE` guards; production builds already suppress.
  - Optional: remove noisy debug branches.
- Remove:
  - Delete debug blocks and tags (`[filter-schema-prop]`, `[join-ui]`).

## Recommended Production Hardening Order

1. Disable maintenance endpoints (`ENABLE_MAINTENANCE_ENDPOINTS=false`).
2. Hide global cache toggle in UI (keep backend capability for emergency ops).
3. Gate `[external-schema]` logging behind env flag defaulting off.
4. Replace `print(...)` diagnostics with structured logger calls.
5. Run grep inventory commands again and clear remaining dev-only logs.

## Fast “Silence Dev Logs” Checklist

- Backend:
  - Replace raw `print(...)` with logger usage.
  - Set production log level to INFO or WARN.
  - Keep error/critical regressions only.
- Frontend:
  - Remove or keep only `DEV_MODE`-guarded console output.

## Optional Policy

Adopt naming convention for future debug-only code:
- Env flags: `ENABLE_*_DEBUG`, `ENABLE_*_MAINTENANCE`
- Log prefixes: `[debug-*]`
- Require all debug output to be behind a single flag per subsystem.
