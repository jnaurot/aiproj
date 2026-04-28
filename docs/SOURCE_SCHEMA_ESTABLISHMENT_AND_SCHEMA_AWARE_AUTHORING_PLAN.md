# SOURCE_SCHEMA_ESTABLISHMENT_AND_SCHEMA_AWARE_AUTHORING_PLAN.md

## Goal
Deliver the highest-leverage schema-plane improvements in this order:
1. Source schema establishment (file + DB MVP first)
2. Schema-aware authoring hints/autocomplete in editors
3. Source schema provenance expansion + refresh/staleness controls

This plan is designed to eliminate opaque-source bottlenecks, reduce downstream schema false-ambiguity, and improve authoring velocity.

---

## Scope and Principles

1. Node IDs remain runtime authority across all planes.
2. Schema-plane stores source schema with explicit provenance metadata.
3. UI should surface confidence/provenance, not overstate certainty.
4. Prefer deterministic, testable behavior over implicit heuristics.
5. MVP prioritizes correctness and user trust over advanced inference breadth.

---

## Phase 0 — Contract and Data Model Baseline
**Commit message:** `docs(schema): define source schema provenance contract and rollout invariants`

### Implementation
1. Define source schema provenance enum and semantics:
	- `sample` (file sampling)
	- `artifact` (DB/query introspection)
	- `declared` (user-declared JSON Schema)
	- `inferred` (future reserved)
2. Document precedence rules for effective source schema:
	1. explicit declared override
	2. latest introspected artifact schema
	3. latest sampled schema
	4. opaque fallback
3. Document staleness/refresh model:
	- schema can be stale when source config changes
	- stale schema is still visible but confidence-labeled

### Integration tests
- None (doc/contract phase).

### Regression tests
- None (doc/contract phase).

### Exit criteria
- Contract accepted and references added in schema docs.

---

## Phase 1 — File Source Schema Acquisition (MVP)
**Commit message:** `feat(source-schema): sample file source columns and persist provenance=sample`

### Implementation
1. Add file source schema sampler path (CSV/JSON/parquet-like tabular inputs as supported by backend/frontend artifact preview path).
2. Persist sampled schema onto source node expected/effective schema with provenance:
	- `sourceProvenance: sample`
	- sampling timestamp
	- sample size metadata (rows scanned)
3. On source config mutation (file change/path change/options change), mark schema stale.
4. Ensure propagation uses sampled schema immediately after capture.

### Integration tests
1. `INT-SRC-01`: file source run captures non-opaque table schema with expected columns.
2. `INT-SRC-02`: downstream select using sampled column validates clean without manual schema edits.
3. `INT-SRC-03`: changing file source config marks schema stale and triggers re-sample on next run/refresh.

### Regression tests
1. Existing opaque fallback behavior remains for unsupported or unreadable file payloads.
2. Non-source nodes’ schema behavior unchanged.

### Exit criteria
- File sources no longer default to opaque when sampleable data exists.

---

## Phase 2 — DB Source Introspection (MVP)
**Commit message:** `feat(source-schema): introspect database source output schema with provenance=artifact`

### Implementation
1. Add DB source introspection hook after query/config is valid.
2. Persist introspected schema on source node with provenance:
	- `sourceProvenance: artifact`
	- query signature/hash for staleness comparison
	- introspection timestamp
3. If introspection fails, retain last known schema with stale warning and diagnostic reason.
4. Ensure schema propagation uses introspected schema without requiring data execution where possible.

### Integration tests
1. `INT-SRC-DB-01`: DB source with valid query publishes table schema and downstream validation succeeds.
2. `INT-SRC-DB-02`: query change invalidates prior schema (stale) and refresh updates columns.
3. `INT-SRC-DB-03`: introspection failure emits warning and keeps previous schema labeled stale.

### Regression tests
1. Existing run execution still works when introspection is unavailable.
2. DB nodes without query stay opaque and do not crash propagation.

### Exit criteria
- DB sources provide first-class schema when introspection succeeds.

---

## Phase 3 — Declared JSON Schema Path for Sources
**Commit message:** `feat(source-schema): support declared JSON Schema on source nodes with provenance=declared`

### Implementation
1. Add source-node declared JSON Schema input path (UI/editor + store action).
2. Reuse LLM JSON Schema mapping utility for top-level properties to schema-plane columns.
3. Apply precedence rule: declared overrides sampled/artifact for effective validation.
4. Keep underlying sampled/artifact schema available for comparison/diagnostics.

### Integration tests
1. `INT-SRC-DECL-01`: declared JSON Schema is converted to table schema and used downstream.
2. `INT-SRC-DECL-02`: declared schema override suppresses opaque warnings when valid.
3. `INT-SRC-DECL-03`: clearing declared schema reverts to artifact/sample schema.

### Regression tests
1. Invalid declared JSON Schema is rejected with actionable diagnostics.
2. Existing source nodes without declared schema unaffected.

### Exit criteria
- Users can establish explicit schema without edge-inspector workarounds.

---

## Phase 4 — Schema-Aware Authoring MVP (Hints First)
**Commit message:** `feat(authoring): expose propagated available columns in transform editors`

### Implementation
1. Add read-only `available columns` model to key editors:
	- derive
	- filter/select
	- SQL editor helper panel
2. Source of truth = propagated schema-plane columns at target node input.
3. Show provenance/confidence badge in editor context (`declared`, `artifact`, `sample`, `opaque`).
4. No autocomplete insertion yet; hints-only MVP.

### Integration tests
1. `INT-AUTH-01`: derive editor shows propagated columns from source schema.
2. `INT-AUTH-02`: filter/select editor column hint list updates after upstream schema change.
3. `INT-AUTH-03`: SQL helper panel shows current columns and refreshes reactively.

### Regression tests
1. Editors remain functional when schema is opaque (empty hints + explanatory state).
2. Existing editor save/draft behavior unchanged.

### Exit criteria
- Users can author against visible column context in major transforms.

---

## Phase 5 — Autocomplete and Validation Assist
**Commit message:** `feat(authoring): add schema-aware autocomplete for derive/filter/sql expressions`

### Implementation
1. Add autocomplete suggestions for column identifiers in derive/filter/sql editors.
2. Context-sensitive insertion rules:
	- quote/escape strategy aligned with existing expression grammar
	- avoid destructive edits to user text
3. Add lightweight pre-submit checks for obvious unknown-column references.
4. Keep checks warning-grade when provenance confidence is low (`sample`/`artifact` stale).

### Integration tests
1. `INT-AUTO-01`: derive editor autocomplete inserts valid column tokens.
2. `INT-AUTO-02`: filter/sql editors suggest and insert propagated columns.
3. `INT-AUTO-03`: unknown column warning appears pre-submit and clears after correction.

### Regression tests
1. Autocomplete disabled gracefully when no columns are available.
2. No cursor-jump or text-corruption regressions in expression editors.

### Exit criteria
- Core expression editors are schema-aware during authoring.

---

## Phase 6 — Provenance UX, Refresh Policies, and Staleness Controls
**Commit message:** `feat(schema-ux): add source schema refresh controls, stale indicators, and confidence labels`

### Implementation
1. Add per-source schema status UI:
	- provenance label (`declared`/`artifact`/`sample`/`opaque`)
	- last refresh time
	- stale badge and reason
2. Add manual refresh action for source schema acquisition.
3. Add optional auto-refresh policy toggles (conservative default):
	- refresh on source config change
	- refresh on run start (opt-in)
4. Ensure run guard and schema diagnostics consume provenance/staleness consistently.

### Integration tests
1. `INT-UX-01`: manual refresh updates source schema and clears stale state.
2. `INT-UX-02`: config change marks stale and displays reason.
3. `INT-UX-03`: run guard messaging references warning vs error consistent with provenance confidence.

### Regression tests
1. Existing schema view rendering remains stable when provenance metadata absent (legacy graphs).
2. No performance regression in large graph schema recompute baseline.

### Exit criteria
- Users can trust and control schema freshness/confidence lifecycle.

---

## Full Integration Test Matrix
1. File source sampling -> downstream validation success.
2. DB introspection -> downstream validation success.
3. Declared schema override precedence.
4. Editor hints reflect propagated schema and refresh after upstream change.
5. Autocomplete insertion correctness in derive/filter/sql.
6. Staleness/refresh UX and run-guard consistency.

## Full Regression Test Matrix
1. Legacy opaque-source graphs still execute.
2. Existing schema diagnostics/edge colors unchanged except where source schema is now known.
3. Editor draft/save behavior unchanged.
4. Non-source node schema propagation unaffected.
5. Large-graph schema recompute remains within current performance envelope.

---

## Suggested Execution Order
1. Phase 0
2. Phase 1
3. Phase 2
4. Phase 3
5. Phase 4
6. Phase 5
7. Phase 6

This order maximizes value/effort by unlocking reliable source schemas before investing in richer editor assist.
