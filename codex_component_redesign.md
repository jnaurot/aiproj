# Component Redesign Tickets (Exposure-First, Native Handles)

## Why this is a good idea
This replaces ambiguous component JSON wrapping with explicit, versioned contracts so dataflow stays type-safe, debuggable, and migration-safe. It also preserves current author workflows (edit internals, return to main graph, load/save component versions) while making component boundaries predictable for runtime, replay, and UI.

## Guiding policy (agreed)
- Exposed handle identity is stable and revision-scoped.
- Each exposed handle stores:
	- `handle_id` (immutable within revision)
	- `alias` (user-facing)
	- `internal_source_path` (canonical)
	- `kind` (`data_input`, `data_output`, `param_input`, `control_input`)
	- native type/contract
	- lifecycle flags: `exposed`, `published`, `debug_visible`
- Published profile is minimal and contractual.
- Debug profile is expansive and non-contractual.
- Removed/retyped published handles are breaking by default.
- Dependent graphs hard-fail on incompatible upgrade unless explicit compatibility mapping exists.

---

## Ticket CMP-001: Add Component Exposure Registry schema
Goal: Introduce a first-class exposure registry in component revision metadata.

Scope:
- Add backend + shared schema types for exposure records.
- Persist exposure records on component revision save/load.
- Keep backward compatibility with existing component payloads via read-time defaulting.

Acceptance Criteria:
- Component revision includes normalized exposure registry.
- Old revisions without registry still load.
- Registry round-trips exactly on save/load.

Tests:
- Backend unit: `backend/tests/components/test_exposure_registry_schema_roundtrip.py`
- Backend unit: `backend/tests/components/test_exposure_registry_backward_compat.py`
- Frontend schema test: `src/lib/flow/components/exposureRegistry.schema.test.ts`

---

## Ticket CMP-002: Introduce handle lifecycle states (`exposed`, `published`, `debug_visible`)
Goal: Separate authoring visibility from published contract.

Scope:
- UI + backend model support for three distinct flags.
- Validation rules:
	- `published => exposed`
	- `debug_visible` may be true while `published` is false
- Add profile materialization helpers:
	- `published_profile`
	- `debug_profile`

Acceptance Criteria:
- Profiles are generated deterministically from lifecycle flags.
- Published profile excludes debug-only handles.

Tests:
- Backend unit: `backend/tests/components/test_exposure_profile_materialization.py`
- Frontend unit: `src/lib/flow/components/exposureProfiles.test.ts`
- Validation test: `backend/tests/components/test_exposure_lifecycle_validation.py`

---

## Ticket CMP-003: Native boundary routing (no monolithic JSON output wrapper)
Goal: Component boundary emits native artifacts per exposed output handle.

Scope:
- Runtime boundary resolver maps exposed output handle -> internal node output path.
- Remove forced JSON envelope behavior for component output publication path.
- Preserve internal graph execution behavior unchanged.

Acceptance Criteria:
- Downstream node receives native artifact type matching exposed output contract.
- Multi-output component can emit changed output independently.
- No accidental type coercion to JSON at boundary.

Tests:
- Backend integration: `backend/tests/runner/test_component_native_output_routing.py`
- Backend integration: `backend/tests/runner/test_component_multi_output_partial_change.py`
- Regression: `backend/tests/runner/test_component_no_json_wrapper_regression.py`

---

## Ticket CMP-004: Input boundary routing for data/param/control inputs
Goal: Support exposed data inputs and param/control inputs as typed boundary handles.

Scope:
- Route external connections to internal target paths by `internal_source_path`.
- Enforce kind-specific validation at bind time.
- Explicitly reject unsupported control outputs unless feature-flagged.

Acceptance Criteria:
- Data input mapping works with native contracts.
- Param/control input mapping works without crossing data-plane semantics.
- Invalid kind/type connections fail with clear diagnostics.

Tests:
- Backend integration: `backend/tests/runner/test_component_input_routing_by_kind.py`
- Backend unit: `backend/tests/components/test_component_bind_validation_errors.py`
- Frontend contract test: `src/lib/flow/components/componentBoundaryValidation.test.ts`

---

## Ticket CMP-005: Preserve component authoring workflow
Goal: Keep "edit internals -> return to parent graph" behavior fully intact.

Scope:
- Ensure internal edit sessions still target component draft/revision correctly.
- Ensure returning to main graph updates node contract projection from selected profile.
- Preserve unsaved-change + revision prompts.

Acceptance Criteria:
- Author can edit internals exactly as today.
- Returning to parent graph does not lose exposure lifecycle settings.

Tests:
- Frontend integration: `src/lib/flow/components/componentAuthoringRoundtrip.test.ts`
- Frontend integration: `src/lib/flow/components/componentReturnToGraphProjection.test.ts`

---

## Ticket CMP-006: Versioning, loading, and compatibility gates
Goal: Enforce safe upgrades when published contract changes.

Scope:
- On load/upgrade, compute compatibility diff between old and new published profiles.
- Hard-fail dependent graph binding on removed/retyped published handles.
- Support optional explicit compatibility mappings (author-declared shims).

Acceptance Criteria:
- Same revision: exact handle IDs remain authoritative.
- Breaking changes are blocked without mapping.
- Safe mappings permit migration.

Tests:
- Backend unit: `backend/tests/components/test_component_revision_compatibility_diff.py`
- Backend integration: `backend/tests/components/test_component_upgrade_hard_fail_without_mapping.py`
- Backend integration: `backend/tests/components/test_component_upgrade_with_compat_mapping.py`

---

## Ticket CMP-007: Migration tooling + diagnostics
Goal: Provide actionable migration output for dependent graphs.

Scope:
- Generate per-edge migration report (missing handle, retyped handle, remapped handle).
- Expose diagnostics to UI with node/edge references.

Acceptance Criteria:
- Upgrade failure includes concrete remap actions.
- UI can render concise migration guidance.

Tests:
- Backend unit: `backend/tests/components/test_component_migration_report_format.py`
- Frontend unit: `src/lib/flow/components/componentMigrationDiagnostics.test.ts`

---

## Ticket CMP-008: Replay and determinism integrity
Goal: Ensure replay correctness with new boundary model.

Scope:
- Snapshot/replay stores exposed handle IDs + resolved internal paths for the run revision.
- Replay validation checks revision + handle compatibility before execution.

Acceptance Criteria:
- Replay does not silently switch handle semantics.
- Incompatible replay fails fast with explicit reason.

Tests:
- Backend integration: `backend/tests/replay/test_component_replay_handle_identity.py`
- Backend integration: `backend/tests/replay/test_component_replay_incompatible_revision_fails.py`

---

## Ticket CMP-009: UI projection + monitor updates
Goal: Reflect published/debug exposure profiles in Node Inspector and monitor views.

Scope:
- Show which handles are published vs debug-visible.
- Ensure external node ports reflect selected published profile only.
- Debug profile visible in diagnostics panels without changing runtime contract.

Acceptance Criteria:
- Canvas ports are contractual (published).
- Debug-only handles appear in debug tools, not as regular connectable ports.

Tests:
- Frontend integration: `src/lib/flow/components/componentPortProjectionProfiles.test.ts`
- Frontend integration: `src/lib/flow/components/componentDebugVisibilityMonitor.test.ts`

---

## Ticket CMP-010: End-to-end regression suite (final gate)
Goal: Lock full redesign behavior with realistic graph scenarios.

E2E Scenarios:
1. Multi-output component where only one output changes across emissions.
2. Downstream typed LLM/transform nodes consume native output types without JSON mismatch.
3. Author edits internals, returns to main graph, saves new revision, loads old revision.
4. Upgrade with breaking published-handle change hard-fails dependents.
5. Upgrade with explicit compatibility mapping succeeds.
6. Replay old run after non-breaking revision change remains deterministic.

Acceptance Criteria:
- All scenarios pass in CI on clean checkout.
- No regression to "component output always appears as generic JSON wrapper" behavior.

Tests:
- E2E: `tests/e2e/components/component_boundary_native_handles.spec.ts`
- E2E: `tests/e2e/components/component_revision_upgrade_contract.spec.ts`
- E2E: `tests/e2e/components/component_replay_consistency.spec.ts`
- E2E: `tests/e2e/components/component_authoring_roundtrip.spec.ts`

---

## Suggested implementation order
1. CMP-001, CMP-002 (contract foundation)
2. CMP-003, CMP-004 (runtime boundary routing)
3. CMP-005 (workflow preservation)
4. CMP-006, CMP-007 (upgrade safety + migration diagnostics)
5. CMP-008 (replay integrity)
6. CMP-009 (UI/monitor profile projection)
7. CMP-010 (final E2E gate)
