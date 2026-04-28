# RUN_GUARD_OPAQUE_JOIN_UNCERTAINTY_PLAN.md

## Goal
Prevent false blocking modal warnings for join clauses when clauses are asserted but upstream schemas are still unresolved/opaque.

## Problem
`assessSchemaGuard()` currently allows contract-severity errors to dominate blocking decisions even when schema-plane has already downgraded the same condition to uncertainty (`SHAPE_MISMATCH_OPAQUE` / `SHAPE_MISMATCH_ADDITIONAL_PROPERTIES`). This creates contradictory UX: Schema View can appear acceptable/warning-grade while run guard blocks as error.

## Phase 1 — Guard Authority Alignment
**Commit message:** `fix(run-guard): honor schema-plane uncertainty over contract edge errors`

### Implementation
1. In `assessSchemaGuard()` edge finding logic, introduce uncertainty-aware severity resolution.
2. When schema-plane returns warning code `SHAPE_MISMATCH_OPAQUE` or `SHAPE_MISMATCH_ADDITIONAL_PROPERTIES`, treat effective edge severity as warning (non-blocking) even if contract diagnostic says error.
3. Keep true contract/runtime-shape errors blocking when no schema-plane uncertainty signal exists.

### Tests
- Update run-guard tests to assert non-blocking behavior for opaque join clause assertions.
- Preserve existing blocking test behavior for true in-path schema errors.

---

## Phase 2 — Integration Coverage for Join Pre-Run Guard
**Commit message:** `test(run-guard): add integration coverage for join opaque clause pre-run behavior`

### Implementation
1. Add integration-style test scenario that mirrors real join setup:
	- two source inputs with unresolved schema
	- join clause asserted (`left.id = right.id`)
	- run from selected join
2. Assert run is allowed and no run-block reason is set.

### Regression tests
1. Ensure uncertainty warning still does not mask real hard mismatches:
	- non-opaque source with missing selected column remains blocking.
2. Ensure bypass path (`allowSchemaErrors`) still works unchanged.

---

## Phase 3 — Final Verification + Commit Hygiene
**Commit message:** `test(regression): lock run-guard uncertainty semantics for join and opaque upstream`

### Implementation
1. Remove any temporary xfail markers created for this case.
2. Run focused suites:
	- `graphStore.schemaRunGuard.test.ts`
	- `graphStore.schemaPlane.integration.test.ts`
	- `schemaFunctions/transform.test.ts`
3. Confirm no behavior drift in existing schema guard tests.

### Exit criteria
- Join opaque asserted clause no longer blocks pre-run.
- Real schema errors in run path still block.
- All listed tests pass.
