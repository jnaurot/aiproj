# SOURCE_SCHEMA_COMPLETION_AND_AUTHORING_GUARDRAILS_PLAN.md

## Goal
Complete source schema establishment for real sources, stabilize schema-aware authoring hints, align run-guard severity/messaging with edge authority, and add end-to-end regression coverage for source refresh policy.

## Outcomes
1. File/DB/API sources consistently produce non-opaque schema when feasible.
2. Source schema provenance is explicit and persisted (`sample`, `artifact`, `declared`, fallback `opaque`).
3. Derive/filter/sql editors consistently show fresh, propagated available-column hints.
4. Run-guard modal severity and wording match effective edge severity (warning vs error).
5. Source refresh policy is verified end-to-end (`on param change`, `manual refresh`, `on run start`).

---

## Phase 0 — Baseline Audit + Test Harness
**Commit message:** `test(schema): add baseline source-schema and run-guard parity assertions`

### Implementation
1. Add/expand baseline tests to lock current intended behavior:
- Source schema precedence (`declared > artifact > sample > opaque`).
- Schema assist propagation into derive/filter/sql editors.
- Run-guard result structure includes explicit severity for each finding.
2. Add a small fixture builder for source nodes and propagated schema snapshots to reduce repeated test setup.

### Integration tests
1. `INT-BASE-01`: mixed source graph resolves expected effective source schema precedence.
2. `INT-BASE-02`: run-guard returns structured findings with severity + code + message.

### Regression tests
1. `REG-BASE-01`: legacy graphs without provenance metadata continue to load and execute.

### Exit criteria
- Harness in place; no behavior change required in this phase.

---

## Phase 1 — Complete Source Schema Establishment (File + DB + API)
**Commit message:** `feat(source-schema): complete file/db/api schema acquisition with persisted provenance`

### Implementation
1. File sources:
- Ensure sample-derived schema capture path is always attempted when file format is schema-capable.
- Persist provenance + freshness metadata (`sourceProvenance=sample`, `updatedAt`, sample metadata if available).
2. DB sources:
- Ensure query/table introspection path persists `sourceProvenance=artifact` and updates staleness on query signature change.
- Preserve last successful artifact schema on introspection failure with stale reason.
3. API sources:
- Support declared JSON Schema path for API source output (`sourceProvenance=declared`) and validate parse/mapping.
- Ensure API without declaration remains `opaque` unless inferable via existing priming/sample pathway.
4. Verify schema fallback is intentional:
- Opaque only when no declared/artifact/sample schema is available or valid.

### Integration tests
1. `INT-SRC-REAL-01`: file source captures sample schema and downstream select validates clean.
2. `INT-SRC-REAL-02`: DB source introspection schema updates after query shape change.
3. `INT-SRC-REAL-03`: API declared JSON schema maps to table columns and drives downstream validation.
4. `INT-SRC-REAL-04`: introspection/sample failure preserves last schema as stale with reason.

### Regression tests
1. `REG-SRC-REAL-01`: unsupported source formats still execute and remain explicit `opaque` (no crash).
2. `REG-SRC-REAL-02`: existing node params survive schema refresh without destructive overwrite.

### Exit criteria
- File/DB/API schema establishment is consistent and provenance-backed.

---

## Phase 2 — Stabilize Schema-Aware Authoring (Derive/Filter/SQL)
**Commit message:** `feat(authoring): stabilize propagated column hints for derive/filter/sql editors`

### Implementation
1. Ensure editor hints source from propagated schema-plane input columns, not transient preview-only state.
2. Ensure hints refresh after upstream schema changes and run completion events.
3. Guarantee stable behavior across states:
- Fresh schema -> full hints.
- Stale schema -> hints still shown with stale indicator.
- Opaque schema -> empty hints + explanatory copy.
4. Keep insertion behavior safe:
- Derive/filter/sql column insertion should not mutate unrelated text and should preserve cursor semantics.

### Integration tests
1. `INT-AUTH-STABLE-01`: derive hints update after upstream schema adds/removes a column.
2. `INT-AUTH-STABLE-02`: filter hints include propagated columns and stay visible when schema marked stale.
3. `INT-AUTH-STABLE-03`: sql helper columns refresh after source schema refresh action.

### Regression tests
1. `REG-AUTH-STABLE-01`: editors remain usable with zero hints (opaque state).
2. `REG-AUTH-STABLE-02`: no draft/save regression while hints are updating.

### Exit criteria
- Derive/filter/sql hints are stable, reactive, and confidence-aware.

---

## Phase 3 — Run-Guard Messaging Severity Consistency
**Commit message:** `fix(run-guard): align modal severity and wording with effective schema severity`

### Implementation
1. Standardize run-guard finding severity derivation from effective edge/node authority:
- `error` for authoritative contract/schema-plane errors.
- `warning` for uncertainty (opaque/additional-properties uncertainty, etc.).
2. Update modal summary and line-item wording:
- "Schema validation found X errors" vs "X warnings" vs mixed summary.
3. Ensure warning-only findings do not present as errors in modal copy.
4. Add severity token into run-blocked payload for UI rendering consistency.

### Integration tests
1. `INT-GUARD-01`: warning-only path yields warning modal copy and non-error labeling.
2. `INT-GUARD-02`: error path yields blocking error modal copy.
3. `INT-GUARD-03`: mixed findings produce mixed summary with correct counts.

### Regression tests
1. `REG-GUARD-01`: join opaque uncertainty path remains non-blocking when intended.
2. `REG-GUARD-02`: true missing-column errors still block run.

### Exit criteria
- Run-guard text and behavior always match actual severity.

---

## Phase 4 — End-to-End Source Refresh Policy Regression
**Commit message:** `test(e2e): add source refresh policy end-to-end regression coverage`

### Implementation
1. Add E2E-style store test flow (single test file with run lifecycle + refresh triggers):
- configure source with `schema_refresh_on_param_change=true/false`
- configure source with `schema_refresh_on_run_start=true/false`
- invoke manual refresh action
- verify downstream schema/hints/guard update paths
2. Validate persistence of policy toggles through `updateNodeParamsValidated` and reload path.

### Integration tests
1. `INT-REFRESH-E2E-01`: param change with auto-refresh ON updates schema and clears stale.
2. `INT-REFRESH-E2E-02`: param change with auto-refresh OFF marks stale but does not refresh until manual action.
3. `INT-REFRESH-E2E-03`: run-start opt-in refresh executes for opted-in source nodes only.

### Regression tests
1. `REG-REFRESH-E2E-01`: refresh failures log warning and do not break run execution.
2. `REG-REFRESH-E2E-02`: legacy nodes without policy flags default to current conservative behavior.

### Exit criteria
- Refresh-policy behavior is deterministic and protected by end-to-end regression tests.

---

## Suggested Execution Order
1. Phase 0
2. Phase 1
3. Phase 2
4. Phase 3
5. Phase 4

---

## Definition of Done
1. All phase tests added and passing.
2. No fallback-to-opaque in file/db/api when declared/artifact/sample schema is available.
3. Derive/filter/sql hints demonstrably refresh from propagated schema after upstream change.
4. Run-guard modal always reflects actual severity semantics.
5. Source refresh policy is covered by end-to-end regression tests.

---

## Verification Command Set
Run at minimum:

```bash
npx vitest run \
  src/lib/flow/schema/schemaFunctions/source.test.ts \
  src/lib/flow/store/graphStore.schemaPlane.integration.test.ts \
  src/lib/flow/store/graphStore.schemaRunGuard.test.ts \
  src/lib/flow/store/graphStore.inspector.test.ts \
  src/lib/flow/store/graph.updateNodeParamsValidated.test.ts
```

And include any new phase-specific suites created during implementation.
