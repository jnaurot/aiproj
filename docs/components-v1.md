# Components v1

`Component` nodes let you reuse a pinned graph revision as a single node in another graph.

## What It Is

- A `component` node references:
	- `componentId`
	- `revisionId` (pinned, required)
	- `apiVersion`
- Inputs/outputs are derived from the selected component revision API contract.

## Runtime Model

- Compile step expands component internals into namespaced runtime nodes:
	- `cmp:{instanceNodeId}:{internalNodeId}`
- Runtime executes expanded nodes using normal DAG scheduling/caching.
- Parent lifecycle events are emitted:
	- `component_started`
	- `component_finished`
	- `component_failed`

## Determinism and Caching

- Component execution keys include component instance context.
- Changing `revisionId` changes execution keys and invalidates cache as expected.
- Internal nodes still cache independently when eligible.

## Export/Import (v2 packages)

- Graph export includes component dependencies in:
	- `manifest.dependencies.components[]`
- Import validates dependency presence against local component store.

## Known Limits (v1)

- Nested components are rejected (`COMPONENT_NESTING_UNSUPPORTED`).
- Import does not auto-install missing component revisions; it reports unresolved dependencies.
