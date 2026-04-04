# Code Fix Plan: Work-Item Identity Drift After Downstream Soft-Fail

## Ticket 1: Make transform execution consume the scheduler-resolved `input_refs` (authoritative) ✅
**Problem**
- Transform nodes currently re-run `resolve_input_refs(...)` during execution, after `_work_input_overrides` is reset.
- After localized downstream failures and ready-rebuild, this can resolve to mutable latest bindings instead of dequeued work-item identity.

**Implementation**
- In `backend/app/runner/run.py`, transform execution path must use the already resolved `input_refs` from `_resolve_node_execution(...)`.
- Remove/avoid the second `resolve_input_refs(...)` call inside transform branch execution.
- Keep contract validation and input loading logic, but against authoritative `input_refs` variable passed from scheduler resolution.

**Tests**
- `backend/tests/runner/test_identity_collapse_stage_localization.py`
  - Assert identity is preserved across:
    1) model outputs
    2) queue enqueue
    3) queue dequeue
    4) transform input resolution
- Existing regression tests for model/table identity remain passing.

## Ticket 2: Add targeted downstream-soft-fail regression coverage ✅
**Problem**
- The field case appears when downstream fails once, scheduler rebuilds ready nodes, and upstream streaming continues.

**Implementation**
- Keep/add a dedicated regression:
  - `backend/tests/runner/test_downstream_soft_fail_does_not_collapse_upstream_identity.py`
  - Graph shape mimics production pattern: source -> model_score(single_item) -> job_description(single_item transform) -> resume_builder(single_item on_error=skip_failed).
  - Inject one downstream failure; verify middle transform still consumes all distinct upstream artifacts.

**Tests**
- Run this test standalone and with the stage-localization test.

## Ticket 3: Validate no regressions in existing identity and queue behavior ✅
**Implementation**
- Run targeted suite:
  - `backend/tests/integration/test_model_table_rows_identity_integrity.py`
  - `backend/tests/runner/test_single_item_waiting_with_pending_queue.py`
  - new tests from Tickets 1 and 2.

**Done Criteria**
- All above tests pass.
- No unresolved identity collapse at transform input resolution stage.
