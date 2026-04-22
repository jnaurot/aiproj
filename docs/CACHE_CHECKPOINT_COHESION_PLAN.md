# Cache/Artifact/Checkpoint Cohesion Implementation Plan

Date: 2026-04-22
Owner: Runtime + Flow Store

## Goals

Build a consistent and cohesive authority model for:
- `artifactId`
- `execKey`
- `memoKey` / checkpoint `fingerprintAtCreation`
- cache decision surfaces
- checkpoint save/reuse semantics (including cached artifacts)

## Authority Model (Target)

- `artifactId`: immutable identity of persisted artifact bytes/metadata.
- `execKey`: deterministic execution identity for a node run context.
- `memoKey`: semantic memo fingerprint (kind + params + input artifact ids) used for checkpoint freshness.
- `fingerprintAtCreation`: persisted checkpoint copy of `memoKey` only.

No field should silently substitute for another except in explicit legacy migration paths.

## Phase Protocol (Must Follow for Each Phase)

For each phase:
1. Add/adjust tests first (or with implementation in same change if needed).
2. Implement phase code.
3. Run focused tests for phase.
4. Run checkpoint regressions proving checkpoint still works for cached artifacts.
5. Commit phase with a scoped commit message.
6. Record pass/fail notes in commit body (or PR notes).

### Required checkpoint regression gate after every phase

Backend:
- `python -m pytest tests/integration/test_run_from_selected_incremental.py -k checkpoint_hint_valid_marks_cache_only_and_reuses`

Frontend:
- `npm run test -- src/lib/flow/store/graphStore.checkpointActions.test.ts`

Both must pass before committing the phase.

---

## Phase 1: Event Contract Hardening (`node_output.execKey`)

### Changes
- Backend emits `execKey` on all successful `node_output` events (cache hit, checkpoint reuse, compute).
- Frontend `KnownRunEvent` adds `execKey?: string` for `node_output` and reducer prefers provided `execKey`.
- Remove fallback assignment of `execKey := artifactId` in normal event path (keep temporary guarded compatibility path only if strictly needed).

### Tests
- Backend integration: verify `node_output` includes `execKey` for cache-hit and compute paths.
- Frontend reducer tests: `node_output.execKey` is consumed and lineage pair remains consistent.
- Required checkpoint gate tests.

### Commit
- `phase1: add node_output execKey contract and consume in reducer`

---

## Phase 2: Checkpoint Fingerprint Authority Cleanup

### Changes
- `canSaveCheckpoint` and `createCheckpoint` use only `memoState.memoKey` for fingerprint authority.
- Remove fingerprint fallback from lineage `execKey`.
- Ensure checkpoint creation error is explicit when memo key is missing.

### Tests
- Frontend: save allowed only with valid `memoState.memoKey`.
- Frontend: non-hex lineage `execKey` does not unlock checkpoint save.
- Frontend: cached-artifact reuse path with memo trace still enables save.
- Required checkpoint gate tests.

### Commit
- `phase2: enforce memoKey-only checkpoint fingerprint authority`

---

## Phase 3: Binding Normalization Tightening

### Changes
- Remove cross-fill coercion of `execKey/artifactId` in standard normalization.
- Keep isolated legacy migration function for old persisted shapes only.
- Assert partial pairs in dev/test as invariant violations.

### Tests
- Frontend: partial pairs fail invariant checks.
- Frontend: legacy migration path still loads old persisted data safely.
- Frontend: normal runtime paths produce full pairs only.
- Required checkpoint gate tests.

### Commit
- `phase3: tighten binding pair normalization and isolate legacy migration`

---

## Phase 4: Cache Authority Consolidation

### Changes
- Make artifact-store lookup by `execKey` canonical runtime authority.
- De-emphasize/deprecate `ExecutionCache` write path where redundant.
- Remove dead/unused `cache.store_artifact_id(...)` calls if no longer used by runtime read path.

### Tests
- Backend integration: cache hit behavior unchanged via canonical lookup.
- Backend unit/integration: no dependency on `ExecutionCache` index for runtime hit decisions.
- Required checkpoint gate tests.

### Commit
- `phase4: consolidate cache authority on artifact store execKey lookup`

---

## Phase 5: UX Clarity + Diagnostics

### Changes
- Node Inspector messaging clarifies:
  - checkpoint fingerprint source (memo key)
  - why checkpoint save is unavailable (missing memo key vs status vs artifact)
- Optional diagnostics fields in dev UI/logs: current `artifactId`, `execKey`, `memoKey`.

### Tests
- Frontend UI tests for explanatory messaging in unavailable-save states.
- End-to-end style regression tests for run-from-selected + cached + checkpoint save availability.
- Required checkpoint gate tests.

### Commit
- `phase5: add checkpoint/cache fingerprint diagnostics and user-facing clarity`

---

## Final Verification Suite

Backend:
- `python -m pytest tests/integration/test_run_from_selected_incremental.py`
- Any added backend tests for phase work.

Frontend:
- `npm run test -- src/lib/flow/store/graphStore.checkpointActions.test.ts src/lib/flow/store/graphStore.memoState.test.ts`
- Any added frontend tests for phase work.

Manual behavior checklist:
- Cached artifacts can still be checkpointed when memo fingerprint exists.
- Checkpoint options do not disappear after run-finished due to lifecycle mismatch.
- Checkpoint stale/valid outcomes remain visible and coherent.
