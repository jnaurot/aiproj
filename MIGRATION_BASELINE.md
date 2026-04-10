# MIGRATION_BASELINE

Date: 2026-04-10

## Frontend Baseline

- Vitest suites: 398/398 passed
- Vitest tests: 732/732 passed
- Source: `baseline_test_results.json`

### Pin/Freeze-related frontend tests (inventory)

- `src/lib/flow/store/graphStore.freeze.test.ts`
- `src/lib/flow/store/graphStore.runRemotePinnedHintsAfterReset.test.ts`
- `src/lib/flow/store/graphStore.resetRunUi.test.ts`
- `src/lib/flow/store/graphStore.component.test.ts`
- `src/lib/flow/store/graphStore.runScope.test.ts`
- `src/lib/flow/store/runScope.test.ts`
- `src/lib/flow/components/componentRevisionSavePath.test.ts`

## TypeScript Baseline

- `npx tsc --noEmit` error count: 318
- Source: `baseline_tsc_output.txt`

## Backend Baseline (pin behavior integration)

Command run from `backend/`:

- `python -m pytest tests/integration/test_run_from_selected_incremental.py -v`

Result:

- 11/11 passed

Tests:

- `test_run_from_selected_resolves_ancestors_from_cache`
- `test_run_selected_only_executes_selected_and_uses_cached_ancestors`
- `test_run_from_selected_uses_trusted_pinned_artifact_without_upstream_revalidation`
- `test_full_run_pinned_node_reuses_artifact_and_skips_recompute`
- `test_downstream_cache_identity_changes_with_pinned_artifact`
- `test_pinned_component_reuses_boundary_artifact_for_downstream`
- `test_run_from_selected_respects_pinned_internal_component_node_from_graph_meta`
- `test_run_from_selected_derives_nested_internal_pin_hints_from_component_lineage`
- `test_run_from_selected_pinned_artifact_missing_emits_pin_execute_fail_trace`
- `test_run_from_selected_pin_plan_marks_not_in_subgraph_reason`
- `test_run_from_selected_cache_only_without_trusted_pin_emits_fallback_recompute`

## Explicit Pin-Relevant Files Required by Migration Doc

- `src/lib/flow/store/graphStore.freeze.test.ts`
- `src/lib/flow/store/graphStore.runRemotePinnedHintsAfterReset.test.ts`
- `src/lib/flow/store/graphStore.resetRunUi.test.ts`
- `src/lib/flow/store/graphStore.component.test.ts`
- `src/lib/flow/store/graphStore.runScope.test.ts`
- `src/lib/flow/store/runScope.test.ts`
