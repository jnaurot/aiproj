# Component Authoring Boundary Tickets

## Why this is a good idea
Separating authoring from consumption preserves component contract trust: end users can safely use components without silently mutating published outputs, while component authors still control exposure during revision authoring.

## CAB-001: Consumer Inspector Read-Only Contract UI
Goal: In main-graph consumer context, component API output/exposure controls are read-only.

Scope:
- Pass editor context into `ComponentEditor` from `NodeInspector`.
- In consumer (`editingContext === "graph"`):
  - Disable output add/remove/rename/source/required/schema-edit controls.
  - Show explicit message: contract is authoring-only; use "Edit internals" or fork flow.
- In component-authoring context (`editingContext === "component"`), keep current editing behavior.

Regression tests:
- `src/lib/flow/components/editors/ComponentEditor/ComponentEditor.test.ts`
  - asserts read-only messaging and authoring/consumer mode wiring text exists.
- `src/lib/flow/components/NodeInspector.svelte` source assertion test to ensure context is passed to `ComponentEditor`.

E2E gate for this ticket:
- `src/lib/flow/store/graphStore.component.test.ts`
  - simulate selecting a component node in graph context and verify inspector accept validation still runs while UI-level contract editing is unavailable.

---

## CAB-002: Store-Level Guard Against Consumer Contract Mutation
Goal: Prevent component contract mutation in consumer context even if UI is bypassed.

Scope:
- Add defense-in-depth guard in `graphStore.updateNodeConfigImpl`:
  - when target node is `component` and `editingContext === "graph"`, reject mutations to:
    - `params.api`
    - `params.exposureRegistry`
    - `params.published_profile`
    - `params.debug_profile`
    - legacy `params.bindings.outputs`
  - allow unchanged values and allow non-contract fields (e.g. `componentRef`, `config`, debug flags).
- Add explicit internal override for system operations that must update contract in graph context (e.g. revision apply).

Regression tests:
- `src/lib/flow/store/graphStore.component.test.ts`
  - rejects `updateNodeConfig` contract changes in graph context with clear reason.
  - allows non-contract component param updates in graph context.
  - preserves existing revision-apply behavior (internal path still succeeds).

E2E gate for this ticket:
- `src/lib/flow/store/graphStore.component.test.ts`
  - flow test: apply revision to component node, verify contract updates still occur via internal override, then direct consumer mutation is blocked.

---

## CAB-003: Save/Publish Boundary Consistency + Messaging
Goal: Ensure authoring saves and graph saves reflect boundary rules clearly.

Scope:
- Keep graph-save preflight strict and add user-facing reason for blocked consumer contract mutation attempts.
- Ensure component revision save (authoring context) continues to permit exposure edits.
- Add audit log/inspector notice text when consumer contract mutation is blocked.

Regression tests:
- `src/lib/flow/store/graphStore.component.test.ts`
  - blocked mutation emits deterministic reason/error payload.
  - component authoring session can still edit and apply output contract.

E2E gate for this ticket:
- `src/lib/flow/store/graphStore.component.test.ts`
  - nested flow: open component revision for editing -> mutate API outputs -> return to graph -> consumer-side attempt to mutate same contract is blocked.

---

## Execution order
1. CAB-001 (UI boundary)
2. CAB-002 (store guard)
3. CAB-003 (boundary consistency + full test matrix)