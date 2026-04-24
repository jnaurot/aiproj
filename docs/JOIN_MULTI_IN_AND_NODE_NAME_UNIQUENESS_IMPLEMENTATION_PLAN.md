# JOIN_MULTI_IN_AND_NODE_NAME_UNIQUENESS_IMPLEMENTATION_PLAN.md

## Goal
Implement architecturally coherent `join` semantics where:

1. A `join` node can accept multiple `in` work edges as distinct table relations.
2. Join clauses reference relations by stable, globally unique relation names.
3. Schema-plane, runtime, and UI all use the same relation-authority model.
4. Node naming uniqueness is enforced:
	- unique within the same graph level/scope
	- globally unique via component-qualified names (`componentName.nodeName`) across nested scopes

---

## Problem Summary

Current behavior creates drift between graph wiring and join clause semantics:

1. Runtime identifies join inputs by node IDs/lookups while handle semantics are generic.
2. Schema propagation may treat same-handle inputs ambiguously.
3. Same-handle schema checks can conflict with legitimate join use-cases (different tables).
4. Clause references can drift from connected upstreams without deterministic enforcement.

Result: runtime can partially work while schema-plane and UX become inconsistent.

---

## Target Architecture

## Canonical Join Relation Model
1. Every incoming work edge to a join node contributes one relation candidate.
2. Relation identity is `relationName`, derived from canonical node name (or explicit alias).
3. Join clauses use:
	- `<relationName>.<columnName>`
4. Both schema-plane and runtime resolve clauses from the same relation map.

## Canonical Name/Scope Model
1. Same-scope node names must be unique.
2. Nested node global canonical name is:
	- `<componentName>.<nodeName>` (recursively if nested components are supported).
3. Clauses and diagnostics always display canonical names, never unstable internal IDs.

---

## Phase 0 - Contract and Fixture Baseline
**Commit message:** `docs: add join multi-in and node-name uniqueness implementation contract`

### Tasks
1. Add this plan.
2. Document relation-name and canonical-name invariants in code comments/spec docs.
3. Freeze baseline test fixtures for:
	- two-table join
	- three-input join graph
	- component-qualified naming graph

### Tests
- None required in this phase.

### Exit Criteria
- Contract and fixtures are documented and approved.

---

## Phase 1 - Node Name Uniqueness and Canonical Naming
**Commit message:** `feat(graph): enforce scoped node-name uniqueness and canonical qualified names`

### Tasks
1. Introduce canonical name resolver utility:
	- input: graph node identity + scope/component context
	- output: canonical name string
2. Enforce same-level uniqueness rule on create/rename/import.
3. Enforce deterministic qualification for component-internal node names.
4. Persist canonical names in graph metadata used by schema/runtime planners.

### Integration Tests
1. Same-level rename conflict:
	- Two sibling nodes in one graph scope.
	- Rename second to first’s name.
	- Expect validation error and no rename applied.
2. Different-level same local name:
	- Top-level node `Transform_A`.
	- Component `Comp1` contains node `Transform_A`.
	- Expect both allowed and canonical names differ (`Transform_A` vs `Comp1.Transform_A`).
3. Import/rehydrate graph preserves canonical naming deterministically.

### Regression Tests
1. Existing graphs without canonical name field are migrated safely.
2. Existing references by node ID continue to resolve while canonical names are introduced.

### Exit Criteria
- Uniqueness rules enforced.
- Canonical names are stable and available to downstream phases.

---

## Phase 2 - Join Relation Mapping for Multi-`in`
**Commit message:** `feat(join): build canonical multi-in relation map from incoming edges`

### Tasks
1. Build join relation map from all incoming `work` edges on `in`:
	- `relationName`
	- source node ID
	- source canonical name
	- inferred/declared schema
2. Ensure relation map is deterministic (sort by canonical name, tie-break by edge ID).
3. Add optional alias support (future-safe):
	- if alias set, use alias; else canonical name.
4. Reject ambiguous duplicate relation names at join configuration time.

### Integration Tests
1. Two-input join map:
	- `LLM_Describe`, `LLM_BusyBody` into one join node.
	- Expect two relations in map, both resolvable.
2. Three-input graph:
	- Verify map includes all three relations with deterministic ordering.
3. Same local names from different scopes:
	- `Top.A` and `Comp1.A` both connected to join.
	- Expect unique relation names, no collision.

### Regression Tests
1. Single-input non-join nodes unaffected.
2. Existing join configs using node IDs migrate to canonical relation references without data loss.

### Exit Criteria
- Join node has complete, stable multi-in relation map.

---

## Phase 3 - Schema Plane Alignment for Join
**Commit message:** `fix(schema-plane): propagate all join in-relations and validate clauses by relation names`

### Tasks
1. Update schema propagator to pass all relevant `in` relations for join nodes.
2. Replace “first edge per handle” behavior for join path with relation-map driven behavior.
3. Apply join-specific compatibility logic:
	- allow different input schemas across relations
	- validate clause columns exist in referenced relations
	- validate type compatibility for join keys
4. Improve diagnostics payload:
	- `missingColumns`, `availableColumns`, `relationName`, `paramPath`
	- actionable suggestion text

### Integration Tests
1. Valid join with different schemas:
	- Left has `id,text`, right has `id,summary`.
	- Clause `left.id = right.id`.
	- Expect schema status clean.
2. Missing column diagnostic:
	- Clause references `left.unknown_col`.
	- Expect warning/error with `missingColumns` and `availableColumns` including relation-qualified names.
3. Component-qualified relation diagnostic:
	- Missing column reported as `Comp1.NodeA.id` style canonical names.

### Regression Tests
1. Existing same-handle conflict checks remain active for non-join operators.
2. Join no longer fails due to generic same-handle-identical-schema check.

### Exit Criteria
- Join schema-plane matches runtime semantics and produces reliable diagnostics.

---

## Phase 4 - Runtime Join Execution Alignment
**Commit message:** `refactor(join-runtime): resolve clauses through canonical relation map authority`

### Tasks
1. Runtime join resolver uses canonical relation map (same source as schema-plane).
2. Clause parser expects `<relationName>.<columnName>`.
3. Runtime rejects unresolved relation names with explicit error code.
4. Runtime error payload mirrors schema-plane shape where possible.

### Integration Tests
1. End-to-end successful join:
	- Two upstream tables, valid clause by relation name.
	- Join executes successfully.
2. End-to-end unresolved relation:
	- Clause references non-existent relation.
	- Deterministic failure code and details.
3. End-to-end type mismatch:
	- `left.id` string vs `right.id` number with strict policy.
	- Deterministic mismatch failure.

### Regression Tests
1. Scheduler dependency behavior unchanged (upstream readiness still edge-driven).
2. Non-join transform execution unaffected.

### Exit Criteria
- Runtime and schema-plane share one join relation authority.

---

## Phase 5 - UI/Editor Consistency
**Commit message:** `feat(join-ui): show canonical relation names and clause-safe autocomplete`

### Tasks
1. Join editor relation picker/autocomplete uses canonical relation map.
2. Clause UI displays canonical names (or explicit alias when present).
3. Editor validation prevents selecting unresolved relations/columns.
4. Diagnostics panel links message fields directly to relation+column controls.

### Integration Tests
1. Join editor lists all incoming relations from multi-`in`.
2. Component-scoped names render clearly (`Comp1.NodeA`).
3. Selecting suggested fix updates clause and clears diagnostic when valid.

### Regression Tests
1. Existing node editors unaffected.
2. Existing join clauses migrate and remain editable.

### Exit Criteria
- UI semantics match runtime/schema semantics.

---

## Phase 6 - Migration and Backward Compatibility
**Commit message:** `chore(migration): migrate legacy join clauses and preserve execution compatibility`

### Tasks
1. Add migration for legacy join clauses keyed by internal node IDs.
2. Convert to canonical relation names on load/save.
3. Add one-time audit logs for migrated clauses.
4. Provide fallback resolver for old runs/history playback.

### Integration Tests
1. Legacy graph loads and executes after migration with equivalent behavior.
2. Saving migrated graph does not reintroduce ID-based references.

### Regression Tests
1. Replay of historical runs still resolves legacy clause references for read-only views.

### Exit Criteria
- Legacy graphs continue to work while new contract is canonical-name based.

---

## Full Integration Test Matrix (Must Pass)

1. Two-table join success (`left.id = right.id`).
2. Two-table missing key column diagnostic correctness.
3. Three-input relation map determinism.
4. Same local node names in different scopes resolve uniquely in join clauses.
5. Editor suggestion applies valid clause/schema fix and clears diagnostic.
6. Runtime + schema-plane error parity for unresolved relation and missing column.

---

## Full Regression Test Matrix (Must Pass)

1. Non-join same-handle schema checks remain unchanged.
2. Non-join schema propagation performance and behavior unchanged.
3. Existing join graphs (ID-based clauses) migrate without breakage.
4. Existing run-monitor/error payload consumers remain compatible.
5. Component execution and naming behavior unaffected outside join semantics.

---

## Proposed File Touch Map

Frontend:
- `src/lib/flow/store/graphStore.node-schema.ts`
- `src/lib/flow/store/graphStore.graph-edit.ts`
- `src/lib/flow/components/NodeInspector.svelte`
- `src/lib/flow/components/ui/*join*` (editor/autocomplete/suggestions)
- `src/lib/flow/store/*join*test.ts`
- `src/lib/flow/store/*name*uniqueness*test.ts`

Backend:
- `backend/app/runner/run.py` (join runtime resolver)
- `backend/app/runner/schemas.py` (if join param schema updates needed)
- `backend/tests/runner/test_join_*`
- `backend/tests/integration/test_join_*`

Docs:
- `docs/schema-diagnostics-contract.md`
- `docs/SCHEMA_PLANE_SPEC.md`
- this plan file

---

## Risks and Mitigations

1. Risk: breaking legacy joins.
- Mitigation: explicit migration + fallback resolver + regression fixtures.

2. Risk: canonical naming collisions during graph edits.
- Mitigation: strict same-scope uniqueness enforcement + deterministic qualification.

3. Risk: mismatch between UI alias and runtime relation name.
- Mitigation: one canonical relation map exposed to both UI and runtime layers.

4. Risk: excessive complexity in clause parser.
- Mitigation: centralized parser utility with exhaustive tests.

---

## Definition of Done

1. Join semantics are relation-based and consistent across schema-plane/runtime/UI.
2. Multi-`in` join schema propagation validates all connected relations.
3. Node naming uniqueness rules are enforced at same scope and globally qualified across component levels.
4. Integration and regression suites listed above are green.
5. Legacy join configs are migrated and remain executable.
