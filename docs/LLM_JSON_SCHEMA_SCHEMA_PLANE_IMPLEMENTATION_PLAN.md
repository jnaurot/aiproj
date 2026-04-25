# LLM JSON Schema -> Schema Plane Implementation Plan

## Objective
Use existing `llm` node configuration (`params.output.mode === 'json'` + `params.output.jsonSchema`) to propagate structured schema into the schema plane so downstream transforms can validate fields/types without requiring new user configuration.

## Scope
1. In scope:
	1. `kind: 'llm'` schema function behavior only.
	2. JSON Schema top-level `type: "object"` with `properties` + `required`.
	3. Primitive mapping:
		1. `string -> string`
		2. `number|integer -> number`
		3. `boolean -> boolean`
		4. `array|object|unknown -> unknown`
	4. `additionalProperties` uncertainty handling:
		1. Persist on schema-plane output metadata.
		2. Downgrade missing-column shape mismatch to warning-grade uncertainty on affected edges.
2. Out of scope:
	1. Nested flattening.
	2. `oneOf`/`anyOf`/`allOf`.
	3. Non-`llm` model kinds (`vision`, `audio`, `embedding`, `reranker`, `multimodal`) remain unchanged/opaque.

## Architectural Rules
1. Runtime/internal authority remains nodeId-first.
2. This is schema-plane wiring; no runtime output contract changes.
3. `output.jsonSchema` is treated as required when `output.mode === 'json'` (already enforced by `LlmParamsSchema`).

---

## Phase 1 - Add LLM Schema Function + Registry Wiring
**Commit message:** `feat(schema-plane): propagate llm json output schema as table columns`

### Implementation
1. Add `src/lib/flow/schema/schemaFunctions/llm.ts`.
2. Implement `schemaFn_llm` behavior:
	1. Non-json output mode -> return `OPAQUE_SCHEMA`.
	2. Json output mode:
		1. Parse top-level object `properties`.
		2. Build `SchemaPlaneColumn[]` from top-level fields.
		3. Apply `required` -> `nullable: false`; otherwise `nullable: true`.
		4. Set `output.mode = 'table'`.
		5. Set output metadata:
			1. `properties.additional_properties: boolean` derived from JSON Schema `additionalProperties` (default `true`).
			2. `properties.source: 'llm_json_schema'`.
3. Register `schemaFn_llm` for `kind: 'llm'` in `schemaRegistry.ts`.
4. Keep `kind: 'model'` fallback registration unchanged.

### Tests
1. Unit tests (`schemaFunctions/llm.test.ts`):
	1. Json schema primitives map correctly to schema plane columns.
	2. `required` mapping to nullable flags works.
	3. Non-object or missing properties in json mode yields safe table with no columns (not crash).
	4. Text mode remains opaque.
	5. `additionalProperties` default/explicit mapping captured in output metadata.

### Exit Criteria
1. `llm` json mode emits typed table columns.
2. `llm` text mode remains opaque.
3. All new unit tests pass.

---

## Phase 2 - Edge Validation Policy for additionalProperties Uncertainty
**Commit message:** `fix(schema-plane): downgrade missing-column errors to warning when llm schema allows additional properties`

### Implementation
1. Update edge validation in `graphStore.schemaPlane.ts`:
	1. Existing behavior (`SHAPE_MISMATCH` + opaque upstream => warning) remains.
	2. New behavior:
		1. If target node emits `SHAPE_MISMATCH` and upstream edge schema has `properties.additional_properties === true`, return warning state (not error) with dedicated code:
			1. `SHAPE_MISMATCH_ADDITIONAL_PROPERTIES`.
		2. Warning message should explain uncertainty (field may exist at runtime because upstream allows additional properties).
2. Update run guard logic in `graphStore.run.ts` so uncertainty warnings do not re-surface as blocking residual node errors:
	1. Keep hard errors blocking.
	2. Exclude residual node `SHAPE_MISMATCH` findings when all relevant inbound edge diagnostics for that node are warning-grade uncertainty (`SHAPE_MISMATCH_OPAQUE` or `SHAPE_MISMATCH_ADDITIONAL_PROPERTIES`).

### Tests
1. Integration tests (`graphStore.schemaPlane.integration.test.ts`):
	1. `llm(json)` with declared columns validates downstream select on declared column.
	2. Missing downstream column with `additionalProperties: true` yields edge warning (not error).
	3. Missing downstream column with `additionalProperties: false` remains edge error.
2. Regression tests (`graphStore.schemaRunGuard.test.ts`):
	1. Run guard does not block for in-path uncertainty warnings (`SHAPE_MISMATCH_ADDITIONAL_PROPERTIES`).
	2. Run guard still blocks for true in-path schema errors.

### Exit Criteria
1. Warning/error behavior matches uncertainty policy.
2. Run guard behavior is consistent with edge severity.

---

## Phase 3 - End-to-End Coverage + Docs
**Commit message:** `test(schema-plane): add llm json propagation integration and guard regressions`

### Implementation
1. Expand integration scenario in schema plane tests:
	1. `source -> llm(json) -> transform(select)` success path.
	2. `source -> llm(text) -> transform(select)` retains opaque warning behavior.
2. Update docs references:
	1. Add concise notes in this plan’s completion section.

### Tests
1. Targeted test runs:
	1. `src/lib/flow/schema/schemaFunctions/llm.test.ts`
	2. `src/lib/flow/store/graphStore.schemaPlane.integration.test.ts`
	3. `src/lib/flow/store/graphStore.schemaRunGuard.test.ts`
	4. `src/lib/flow/store/graphStore.schemaPlane.test.ts`
2. Confirm no regressions in existing transform schema tests:
	1. `src/lib/flow/schema/schemaFunctions/transform.test.ts`

### Exit Criteria
1. New and existing targeted suites pass.
2. Behavior verified for `llm` json/text modes and run guard.

---

## Completion Checklist
1. Phase 1 implemented and committed. [x]
2. Phase 2 implemented and committed. [x]
3. Phase 3 implemented and committed. [x]
4. Final status note appended below. [x]

## Final Status
- Completed on `main`.
- Commits:
	1. `5df7fce` - `feat(schema-plane): propagate llm json output schema as table columns`
	2. `b70cd79` - `fix(schema-plane): downgrade missing-column errors to warning when llm schema allows additional properties`
	3. `test(schema-plane): add llm json propagation integration and guard regressions`
