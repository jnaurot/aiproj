# DIAGNOSTIC_AUTHORITY_AND_EDGE_COLOR_RESTORATION_PLAN.md

## Goal
Restore consistent edge coloring and warning behavior by enforcing clear domain ownership:

- Control plane owns runtime orchestration and event transport.
- Schema/contract engine owns semantic diagnostics (`clean|warning|error`).
- UI uses one canonical diagnostic authority for color/warning classes.

This removes conflicting signals such as:
- Run logs reporting `OPAQUE_DEPENDENCY` warning while Schema Contract panel is `clean`.
- Edge color staying warning/yellow when contract diagnostics are clean.

---

## Problem Statement (Precise)
Current behavior mixes two independent warning sources:

1. Schema plane validation state (`getEdgeSchemaValidationState`) can emit `warning` for `OPAQUE_DEPENDENCY`.
2. Contract diagnostics (`computeEdgeSchemaDiagnosticsInternal`) can emit `clean` for the same edge.

Both flow into edge class/warning behavior, causing mismatch in authority and user confusion.

---

## Target Architecture

### Domain Ownership
1. Control plane domain (authoritative):
- Node/edge runtime lifecycle (`active`, `waiting`, `blocked`, `done`).
- Queue, lease, scheduler, and sequencing telemetry.
- Diagnostic event transport lifecycle (`diagnostic_raised`, `diagnostic_cleared`) only.

2. Schema/contract domain (authoritative):
- Edge semantic compatibility (`clean|warning|error`).
- Coercion/drift/type mismatch reasoning.
- Deterministic recomputation from graph state.

3. UI domain:
- Edge schema color classes are driven by contract severity only.
- Control-plane diagnostic events are display metadata, not severity authority.

### Precedence Rules
1. `contractSeverity` is sole authority for `edge-schema-warning` / `edge-schema-error`.
2. `schemaPlaneState=warning` with `contractSeverity=clean` is downgraded to informational (`SCHEMA_INFO`) or suppressed.
3. Runtime edge state class (`edge-state-*`) remains independent and can coexist with schema class.

---

## Implementation Phases

## Phase 1 - Introduce Canonical Diagnostic Snapshot
**Commit message:** `phase1: add canonical edge diagnostic snapshot with explicit authority fields`

### Changes
1. Add canonical edge diagnostic selector/model:
- `contractSeverity: clean|warning|error` (authoritative)
- `schemaPlaneState: neutral|valid|warning|error` (context)
- `runtimeState: inactive|running|waiting|blocked|settled` (context)
- `effectiveSeverity` derived strictly from `contractSeverity`

2. Add helper in store layer:
- `getEdgeDiagnosticSnapshot(edgeId)`
- Ensure recompute is deterministic and derived from existing state, no mutable caches.

3. Do not change visual behavior yet; only add model and test harness.

### Integration Tests
1. Build state with contract clean + schemaPlane warning (opaque).
- Assert snapshot `contractSeverity=clean`, `schemaPlaneState=warning`, `effectiveSeverity=clean`.

2. Build state with contract warning + schemaPlane valid.
- Assert `effectiveSeverity=warning`.

### Regression Tests
1. Existing schema plane tests continue passing.
2. Existing edge schema diagnostics tests continue passing.

---

## Phase 2 - Move UI Edge Schema Class to Canonical Authority
**Commit message:** `phase2: make edge schema classes use contract severity authority only`

### Changes
1. In `FlowCanvas.svelte`, replace direct `schemaValidation.state || diag.severity` logic with canonical selector result:
- `edge-schema-error` iff `effectiveSeverity=error`
- `edge-schema-warning` iff `effectiveSeverity=warning`
- none iff `effectiveSeverity=clean`

2. Keep runtime visual class logic unchanged.

3. Keep tooltip text dual-sourced (contract first, schema-plane context second), but clearly label source:
- `Schema: warning (contract)`
- `Schema-plane note: opaque dependency` (info)

### Integration Tests
1. End-to-end edge class mapping test in store/UI model layer:
- `contract clean + schemaPlane warning` -> no schema warning class.
- `contract warning` -> warning class.
- `contract error` -> error class.

2. Verify runtime state class remains applied independently.

### Regression Tests
1. Reproduce prior false-yellow case:
- Source->Model simple graph, contract clean, schema plane opaque.
- Assert edge class is settled/green (or runtime class), not schema-warning.

---

## Phase 3 - Rework Run Log Warning Emission Semantics
**Commit message:** `phase3: align schema warn logging with contract authority and add info channel`

### Changes
1. Update `SCHEMA_WARN` emission rules:
- Emit only when `contractSeverity in {warning,error}`.

2. Introduce `SCHEMA_INFO` for non-authoritative schema-plane observations:
- Example: `OPAQUE_DEPENDENCY` while contract is clean.

3. Emit paired lifecycle events with stable keys:
- `[SCHEMA_WARN_RAISED] key=...`
- `[SCHEMA_WARN_CLEARED] key=...`
- Same pattern for info channel.

4. Include structured details in message payload:
- `edge`, `contractSeverity`, `schemaPlaneState`, `code`, `from`, `to`.

### Integration Tests
1. Opaque + clean contract:
- Assert no `SCHEMA_WARN` log.
- Assert `SCHEMA_INFO` optional log (if info channel enabled).

2. Contract warning case:
- Assert `SCHEMA_WARN` emitted once (deduped).

3. Resolution case:
- Assert `SCHEMA_WARN_CLEARED` emitted when warning resolves.

### Regression Tests
1. No duplicate warning spam for repeated events.
2. Run-start race does not produce misleading warning before diagnostics settle.

---

## Phase 4 - Control-Plane/Event Contract Hardening
**Commit message:** `phase4: separate diagnostic transport events from semantic evaluation`

### Changes
1. Define event contract docs/types:
- `diagnostic_raised` / `diagnostic_cleared` carry identity + source + metadata.
- They do not define semantic severity authority.

2. Ensure reducer processes transport events idempotently and merges with canonical snapshot, not replacing it.

3. Add explicit `source` field:
- `source=contract_engine` (authoritative)
- `source=schema_plane` (informational)

### Integration Tests
1. Out-of-order diagnostic transport events still converge to canonical severity from contract snapshot.
2. Reload/hydration path recomputes canonical diagnostics correctly without requiring event replay.

### Regression Tests
1. SSE reconnect/fallback poll does not flip clean edge back to warning due to stale transport events.

---

## Phase 5 - Observability and Rollout Guardrails
**Commit message:** `phase5: add observability counters and gated rollout for diagnostic authority switch`

### Changes
1. Add counters:
- `diagnostic_authority_conflict_total`
- `schema_warn_emitted_total`
- `schema_info_emitted_total`

2. Add temporary feature flag:
- `USE_CONTRACT_SEVERITY_AUTHORITY` default `true` in dev/test first.

3. Add debug panel fields in Run Monitor/Inspector for quick triage:
- `contractSeverity`, `schemaPlaneState`, `effectiveSeverity`.

### Integration Tests
1. Flag on/off parity tests for old/new path while flag exists.
2. Metrics counters increment as expected.

### Regression Tests
1. Edge coloring remains stable across run lifecycle transitions.

---

## Exit Criteria
1. Edge schema coloring is fully driven by contract severity authority.
2. No run-log `SCHEMA_WARN` appears when contract is clean.
3. Any schema-plane-only warning appears as info (or is suppressed), never as schema warning class.
4. Runtime edge state colors (running/waiting/blocked/settled) remain correct and independent.
5. All new integration/regression tests pass.
6. Existing related suites pass:
- schema plane tests
- edge schema diagnostics tests
- run scope/race tests affecting reducer ordering

---

## File Touch Map (Planned)
- `src/lib/flow/FlowCanvas.svelte`
- `src/lib/flow/store/graphStore.run.ts`
- `src/lib/flow/store/graphStore.node-schema.ts` (if selector/helper colocated)
- `src/lib/flow/store/graphStore.ts` (public selector export)
- `src/lib/flow/store/graphStore.types.ts` (canonical snapshot types)
- `src/lib/flow/store/*.test.ts` (new integration/regression coverage)

---

## Risks and Mitigations
1. Risk: accidental behavior changes in runtime edge color logic.
- Mitigation: keep runtime class resolver untouched; isolate schema class authority change.

2. Risk: warning suppression hides useful signals.
- Mitigation: emit `SCHEMA_INFO` channel and include context in inspector.

3. Risk: test fragility due to event ordering.
- Mitigation: assert on canonical snapshot and explicit raised/cleared events, not timestamps.

---

## Suggested Execution Order
1. Phase 1 + tests
2. Phase 2 + tests
3. Phase 3 + tests
4. Phase 4 + tests
5. Phase 5 + tests and cleanup flag when stable
