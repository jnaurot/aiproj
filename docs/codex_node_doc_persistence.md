# Node Doc AI Persistence Ticket Stack

## NDP-001 Persisted AI description schema + type contract [x]
- Goal: add a persisted node-doc generated explanation payload to node meta contract.
- Scope:
	- `src/lib/flow/schema/nodeDocs.ts`
	- `src/lib/flow/schema/base.ts`
	- `src/lib/flow/types/nodeDocs.ts`
	- `src/lib/flow/types/base.ts`
- Requirements:
	- persist `generated` under `meta.nodeDoc.generated`
	- shape includes `summary`, `settings_explained`, `context_notes`, `generated_at`, `signature_key`, optional provider meta
	- preserve `.strip()` policy
- Tests:
	- extend node-doc schema tests to validate `generated` in override

## NDP-002 Store APIs for save/clear generated AI description [x]
- Goal: centralize persistence mutations in graph store.
- Scope:
	- `src/lib/flow/store/graphStore.ts`
- Requirements:
	- add `setNodeDocGeneratedExplanation(nodeId, generated)`
	- add `clearNodeDocGeneratedExplanation(nodeId)`
	- no-op when unchanged
	- persist graph on change
- Tests:
	- regression tests in graph store to confirm save + clear are reflected in resolved node docs

## NDP-003 Resolve + reuse persisted AI explanation [x]
- Goal: reuse saved AI description when still valid.
- Scope:
	- `src/lib/flow/components/nodeDocsViewModel.ts`
	- `src/lib/flow/components/NodeDocTooltip.svelte`
	- `src/lib/flow/nodes/BaseNode.svelte`
- Requirements:
	- expose persisted generated explanation in resolved node doc
	- if persisted `signature_key === current llm signature`, use persisted explanation first
	- if signature mismatch, invalidate persisted generated explanation and regenerate
- Tests:
	- node-doc resolved selector test includes persisted generated payload

## NDP-004 Invalidation semantics for bad feedback [x]
- Goal: bad feedback must not lock stale AI text.
- Scope:
	- `src/lib/flow/components/NodeDocTooltip.svelte`
	- `src/lib/flow/components/nodeDocLlmCache.ts`
- Requirements:
	- on `bad`, clear in-memory cache entry and persisted generated explanation
	- next open/regenerate should fetch fresh AI explanation
- Tests:
	- cache eviction regression (existing)
	- tooltip flow regression for bad feedback invalidation path

## NDP-005 End-to-end regression sweep [x]
- Goal: ensure no UX/runtime regressions.
- Run:
	- `src/lib/flow/components/nodeDocLlmCache.test.ts`
	- `src/lib/flow/components/NodeDocTooltip.test.ts`
	- `src/lib/flow/store/graphStore.nodeDocsResolved.test.ts`
	- `src/lib/flow/schema/nodeDocs.test.ts`
