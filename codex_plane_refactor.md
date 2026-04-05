# Plane Refactor Tickets (3-Plane Model)

## Objective
Standardize runtime/modeling to exactly 3 planes:
- `work` (data)
- `param`
- `control`

Treat `config` as payload/schema under `param` (not a separate plane).

## Why this is a good idea
- Removes ambiguity between `param` and `config`.
- Reduces scheduler/control-plane branching and regression surface.
- Makes component contracts and node inspector semantics consistent.
- Preserves capability while simplifying internal invariants.

## Global completion rule (applies to every ticket)
A ticket is complete only if all 4 test layers pass:
1. Focused unit tests for touched module(s)
2. Focused integration tests for touched runtime/scheduler paths
3. Focused component/graph contract tests
4. Focused E2E scenarios relevant to the ticket

---

## PLN-001: Canonical Plane Vocabulary and Types

### Scope
- Define canonical plane enum/type as `work | param | control` across frontend/backend/shared schema.
- Remove `config` as a plane discriminator from type unions and validators.
- Keep backward-compat parsing: legacy `config` plane values normalize to `param` with warning.

### Acceptance Criteria
- No canonical model persists `config` as a plane.
- Legacy payloads with `config` plane still load and are normalized to `param`.
- Diagnostics include deprecation warning when normalization occurs.

### Tests
- Backend unit: plane normalization tests (`config -> param`).
- Frontend schema/store unit: plane parsing + canonical serialization tests.
- Integration: graph load/save roundtrip with legacy `config` plane fixtures.
- E2E: import old graph containing `config` plane and run succeeds with warning.

---

## PLN-002: Port Declaration Refactor (Node/Edge Contracts)

### Scope
- Update port declaration models so all ports are bound to one of 3 planes only.
- Ensure inspector/authoring UI only offers `work`, `param`, `control`.
- Add explicit `param.config` guidance in UX copy to replace former `config` plane concept.

### Acceptance Criteria
- New/edited nodes cannot create `config` plane ports.
- Existing `config` ports render as `param` (with migration hint).
- Contract diff tooling treats `config -> param` as migration-normalized (non-breaking if only plane label changed).

### Tests
- Frontend unit: inspector port editor options and validation.
- Backend unit: contract parser rejects non-3-plane declarations.
- Integration: contract diff snapshots for migrated graphs.
- E2E: create node ports in UI and verify only 3 planes available.

---

## PLN-003: Runtime Queue and Scheduler Plane Enforcement

### Scope
- Enforce queue accounting and scheduler readiness using only 3 planes.
- Remove any `config`-plane branching in runtime admission, deq/enq accounting, and blocked reason derivation.
- Confirm no control-plane starvation regressions from this simplification.

### Acceptance Criteria
- Queue metrics are emitted only for `work|param|control`.
- Scheduler snapshots and blocked reasons remain correct under mixed-plane workloads.
- No regression in concurrent runnable-node behavior.

### Tests
- Runner unit: per-plane enqueue/dequeue metrics tests.
- Runner integration: mixed-plane fan-in/out scheduling regression tests.
- Control-plane regression: waiting/ready transitions with pending `param` inputs.
- E2E: representative multi-node graph with all 3 planes processes fully.

---

## PLN-004: Component Boundary Alignment to 3 Planes

### Scope
- Ensure component exposure kinds map cleanly to 3 planes:
  - `data_input/data_output -> work`
  - `param_input -> param`
  - `control_input -> control`
- Remove any legacy component handling that treats `config` as a separate plane.
- Keep component internals authoring workflow unchanged.

### Acceptance Criteria
- Component runtime expansion publishes/consumes only 3-plane handles.
- Component contracts remain deterministic and revision-safe.
- Existing components using `config` semantics continue via `param` mapping.

### Tests
- Backend unit: component exposure-kind to plane mapping.
- Backend integration: component input/output routing by kind and plane.
- Frontend integration: component API contract UI shows 3-plane mapping only.
- E2E: component graph run with work + param + control paths.

---

## PLN-005: Migration Tooling + Diagnostics

### Scope
- Add migration diagnostics for graphs/components using legacy `config` plane.
- Provide machine-readable migration report entries:
  - entity id
  - old plane
  - new plane
  - severity
  - auto-fix applied flag
- Provide CLI/API path for dry-run migration report.

### Acceptance Criteria
- Migration reports are deterministic and actionable.
- Auto-migration updates persisted graph payloads safely.
- Warnings are deduplicated and user-facing.

### Tests
- Backend unit: migration report structure + deterministic ordering.
- Integration: dry-run vs apply migration parity.
- Frontend unit: migration diagnostics rendering.
- E2E: run migration on legacy graph, save, reload, execute.

---

## PLN-006: API/SDK Contract Updates

### Scope
- Update API schemas, client typings, and public schema docs to 3-plane contract.
- Version API where required (or provide compatibility adapter path).
- Ensure import/export payloads are canonicalized.

### Acceptance Criteria
- OpenAPI/contracts reflect only 3 planes.
- Client SDK/types compile and pass tests with updated contracts.
- Exported graph payloads never emit `config` plane.

### Tests
- Backend API schema snapshot tests.
- Frontend client typing/contract tests.
- Integration: import/export roundtrip with canonical payloads.
- E2E: create graph, export, import, rerun successfully.

---

## PLN-007: UX Consistency and Terminology Cleanup

### Scope
- Update all UI labels/help text to describe `config` as `param` payload convention.
- Remove stale language implying `config` is a fourth plane.
- Update run monitor/tooltips/inspector to stay terminology-consistent.

### Acceptance Criteria
- No UI path presents `config` as a plane.
- Existing config editors remain available but clearly framed as `param` payload.
- User docs and inline hints align with runtime behavior.

### Tests
- Frontend unit: label/option snapshots for plane pickers.
- Frontend integration: node inspector and monitor state assertions.
- Contract docs tests (if present) or lint checks for banned term patterns.
- E2E: smoke test key editor flows for updated terminology.

---

## PLN-008: Final Regression and Stability Gate

### Scope
- Execute full targeted regression matrix for runtime, scheduler, components, and contracts.
- Add permanent guard tests preventing reintroduction of `config` as a plane.

### Acceptance Criteria
- All targeted suites green.
- New guard tests fail if any code path introduces non-3-plane discriminator.
- Release note/migration note prepared.

### Tests (must all pass)
- Full backend targeted suite for runner/control-plane/component tests.
- Full frontend targeted suite for graphStore/schema/component editor tests.
- Cross-version migration tests (legacy graph -> canonical graph).
- E2E stability pack for representative pipelines (single, batch, component-heavy).

---

## Suggested Implementation Order
1. PLN-001, PLN-002
2. PLN-003
3. PLN-004
4. PLN-005
5. PLN-006, PLN-007
6. PLN-008 final gate

## Rollout Safety
- Ship behind feature flag if desired (`STRICT_THREE_PLANE_MODEL`).
- During transition: read-compat (`config -> param`), write-canonical (only 3 planes).
- Remove compatibility shim after one release window and migration completion.
