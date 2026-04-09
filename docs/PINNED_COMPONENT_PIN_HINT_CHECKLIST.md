# Pinned Component Pin-Hint Checklist (Mechanical Implementation Guide)

Last updated: 2026-04-09  
Owner: Flow runtime/store

## Goal

When a component node is pinned and included in a run scope, we must deterministically answer:

1. Were valid pin hints produced?
2. Were they sent in `graph.__executionHints`?
3. Were they parsed/accepted by backend?
4. Did planner mark node as cache-only?
5. Did executor reuse pinned artifact or recompute?
6. If recompute happened, what exact reason code triggered it?

---

## Canonical Trace Toggle

- Frontend trace gate:
	- `src/lib/flow/store/graphStore.run.ts`
	- `graphStore.setPauseResumeTraceLoggingEnabled(true|false)` (already present)
	- `__setPauseResumeTraceEnabledForTest(true|false)` (test-only)
- Add/keep a separate pin trace gate if needed:
	- recommended key: `pinTraceEnabled`
	- default `false`

---

## Canonical Reason Codes

Use these exact reason keys across FE+BE logs/tests:

- `PIN_HINT_VALID`
- `PIN_HINT_INVALID_MISSING_ARTIFACT_ID`
- `PIN_HINT_INVALID_MISSING_EXEC_KEY`
- `PIN_HINT_INVALID_MISSING_COMPONENT_OUTPUTS` (only when strict output map required)
- `PIN_HINT_DROPPED_SANITIZATION`
- `PIN_HINT_NOT_IN_SUBGRAPH`
- `PIN_HINT_NOT_MARKED_CACHE_ONLY`
- `PIN_TRUSTED_ARTIFACT_PRESENT`
- `PIN_TRUSTED_ARTIFACT_MISSING_IN_STORE`
- `PIN_FALLBACK_RECOMPUTE`
- `PIN_REUSE_EXECUTED`
- `PIN_COMPONENT_OUTPUT_NOT_RESOLVED`

---

## Frontend Checklist (Store/Request Path)

### 1) Pin collection from store bindings

- File: `src/lib/flow/store/graphStore.run.ts`
- Function: `collectPinnedArtifactsByNode(...)`
- Required checks:
	- node is pinned (`nodeFreezeMode(node) !== null`)
	- `artifactId` exists
	- `execKey` exists
	- component: include `outputs[handle]` when `outputLineage` exists
- Required trace log (when pin trace enabled):
	- key: `pin.collect`
	- fields:
		- `nodeId`
		- `kind`
		- `isPinned`
		- `artifactIdPresent`
		- `execKeyPresent`
		- `outputsCount`
		- `reasonCode`

### 2) Run request build + sanitization

- File: `src/lib/flow/store/runScope.ts`
- Function: `buildRunCreateRequest(...)`
- Required checks:
	- pinned IDs survive sanitization
	- pinned artifacts survive sanitization
	- component outputs survive sanitization (valid handles only)
- Required trace log:
	- key: `pin.request_build`
	- fields:
		- `runFrom`
		- `runMode`
		- `pinnedNodeIdsIn`
		- `pinnedNodeIdsOut`
		- `pinnedArtifactsInCount`
		- `pinnedArtifactsOutCount`
		- `droppedNodeIds`
		- `reasonCode`

### 3) Submission boundary (before `createRun`)

- File: `src/lib/flow/store/graphStore.run.ts`
- Function: `runRemote(...)`
- Required checks:
	- log final `payload.graph.__executionHints`
	- ensure component pinned node in active scope still has hint payload
- Required trace log:
	- key: `pin.submit`
	- fields:
		- `graphId`
		- `runFrom`
		- `runMode`
		- `plannedNodeSetSize`
		- `pinnedNodeIds`
		- `pinnedArtifactsNodeIds`
		- `reasonCode`

---

## Backend Checklist (Parse/Plan/Execute Path)

### 4) Parse execution hints from request

- File: `backend/app/runner/run.py`
- Block: `raw_hints = graph.get("__executionHints") ...`
- Required checks:
	- parsed `pinnedNodeIds`
	- parsed `pinnedArtifacts`
	- parsed component `outputs`
	- explicit invalid/drop accounting
- Required trace log:
	- key: `pin.backend_parse`
	- fields:
		- `runId`
		- `runFrom`
		- `runMode`
		- `pinnedNodeIdsParsed`
		- `pinnedArtifactsParsed`
		- `invalidHints`
		- `reasonCode`

### 5) Planner classification

- File: `backend/app/runner/compile.py`
- Function: `compile_plan(...)`
- Required checks:
	- pinned node in subgraph => in `cache_only_nodes`
	- if not cache-only, record exact reason
- Required trace log:
	- key: `pin.plan`
	- fields:
		- `runFrom`
		- `runMode`
		- `subgraphNodeIds`
		- `requestedPinnedNodeIds`
		- `effectivePinnedNodeIds`
		- `cacheOnlyNodeIds`
		- `nonCacheOnlyPinnedReasons`
		- `reasonCode`

### 6) Executor branch decision per pinned node

- File: `backend/app/runner/run.py`
- Function/block: `_execute_node(..., cache_only=...)`
- Required checks:
	- always emit branch decision for pinned/cache-only nodes
	- indicate reuse vs recompute + reason
- Required trace log:
	- key: `pin.execute_decision`
	- fields:
		- `runId`
		- `nodeId`
		- `nodeKind`
		- `cacheOnly`
		- `trustedPinPresent`
		- `decision` (`reuse` | `recompute` | `fail`)
		- `reasonCode`

### 7) Component-specific reuse checks

- File: `backend/app/runner/run.py`
- Trusted pin component branch under `if isinstance(trusted_pin, dict):`
- Required checks:
	- declared component outputs
	- pinned `outputs` coverage
	- fallback behavior logged explicitly
- Required trace log:
	- key: `pin.component_output_map`
	- fields:
		- `nodeId`
		- `declaredOutputHandles`
		- `pinnedOutputHandles`
		- `fallbackApplied`
		- `reasonCode`

---

## Required Regression Test Suite

## Frontend tests

1. `runScope` sanitization keeps valid component pinned outputs  
	- file: `src/lib/flow/store/runScope.test.ts`
2. `runScope` drops invalid pinned payload and records reason  
	- file: `src/lib/flow/store/runScope.test.ts`
3. `runRemote` emits execution hints for pinned component in from-selected run  
	- file: `src/lib/flow/store/graphStore.runRemotePinnedHintsAfterReset.test.ts`
4. pinned component missing lineage is logged and excluded from hints  
	- file: `src/lib/flow/store/graphStore.resetRunUi.test.ts` (or new dedicated test)
5. trace event smoke test for `pin.collect`/`pin.request_build`/`pin.submit`  
	- file: `src/lib/flow/store/graphStore.runScope.test.ts` or new `graphStore.pinTrace.test.ts`

## Backend tests

6. from-selected downstream with pinned component reuses trusted artifact (no recompute)  
	- file: `backend/tests/integration/test_run_from_selected_incremental.py`
7. pinned component with missing trusted artifact fails with `PIN_TRUSTED_ARTIFACT_MISSING_IN_STORE`  
	- file: `backend/tests/integration/test_run_from_selected_incremental.py`
8. pinned component present but planner excludes it -> emits `PIN_HINT_NOT_IN_SUBGRAPH`  
	- file: `backend/tests/runner/test_compile_plan_pins.py` (new if absent)
9. cache-only pinned node without trusted pin payload emits `PIN_FALLBACK_RECOMPUTE`  
	- file: `backend/tests/integration/test_run_from_selected_incremental.py`
10. component output mapping unresolved during recompute emits `PIN_COMPONENT_OUTPUT_NOT_RESOLVED`  
	- file: `backend/tests/integration/test_run_from_selected_incremental.py`

## End-to-end behavior tests

11. pinned component + run from selected immediate downstream should not fail due to `COMPONENT_OUTPUT_NOT_RESOLVED` when hints are valid  
	- backend integration test + frontend store request assertion
12. every pinned node touched by run has exactly one `pin.execute_decision` event  
	- backend runner test (event stream assertions)

---

## Acceptance Gates (Do not merge unless all pass)

- [ ] Every pinned node in run scope has deterministic trace from collect -> request -> parse -> plan -> execute.
- [ ] Any recompute of pinned node has explicit `reasonCode`.
- [ ] Component pinned path proves artifact reuse in tests.
- [ ] No silent fallback from pin reuse to recompute.
- [ ] `run from selected` downstream of pinned component passes without output-resolution failure when hints are valid.

---

## Quick Triage Recipe

1. Enable pin trace logging.
2. Run failing scenario once.
3. Check in order:
	1) `pin.collect`
	2) `pin.request_build`
	3) `pin.submit`
	4) `pin.backend_parse`
	5) `pin.plan`
	6) `pin.execute_decision`
4. First missing/invalid step is root cause; fix there, not downstream.

