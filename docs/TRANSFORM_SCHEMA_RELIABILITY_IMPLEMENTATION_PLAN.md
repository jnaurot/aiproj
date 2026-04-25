# Transform Schema Reliability Implementation Plan

## Objective

Improve schema-plane correctness for transforms by:

1. Fixing incorrect `derive` output type inference for string operations.
2. Enforcing join clause-to-edge consistency so schema validation cannot succeed on disconnected clause node references.
3. Adding an opt-in SQL output schema declaration path to reduce opaque propagation for common SQL workflows.

## Scope

- Frontend schema plane and transform schema functions only.
- No backend execution contract changes.
- Runtime identity remains nodeId-authoritative.

## Phase 1 — Fix Derive String-Operation Type Inference

### Commit Message

`Fix derive schema inference for string formula operators`

### Implementation Steps

1. Update `schemaFn_transform` derive branch to infer output type per formula op:
	- `add/sub/mul/div` -> `number`
	- `concat/lower/upper/trim` -> `string`
2. Preserve existing nullability/properties defaults for derived columns.
3. Keep unknown/custom/unsupported operation fallback conservative (`unknown`) if needed.

### Tests

#### Unit

- File: `src/lib/flow/schema/schemaFunctions/transform.test.ts`
- Add test cases proving:
	- `concat` derives `string`
	- `lower` derives `string`
	- numeric ops still derive `number`

#### Integration

- File: `src/lib/flow/store/graphStore.schemaPlane.integration.test.ts`
- Add graph test where derive adds a string column and downstream schema reads it as `string`.

#### Regression

- Add/extend test asserting no false numeric typing for string derive ops.

### Exit Criteria

- All derive string formula outputs are propagated as `string`.
- No existing derive numeric behavior regresses.

---

## Phase 2 — Enforce Join Clause/Edge Consistency in Schema Plane

### Commit Message

`Validate join clauses against connected inputs in schema propagation`

### Implementation Steps

1. In join schema function, when nodeId-qualified clauses are provided and `__schemaInputRefs` is present:
	- Fail schema validation if `leftNodeId` or `rightNodeId` is not in connected input refs.
2. Keep legacy fallback behavior for old positional/no-nodeId clauses where appropriate.
3. Ensure error messaging is explicit (clause references disconnected input node).

### Tests

#### Unit

- File: `src/lib/flow/schema/schemaFunctions/transform.test.ts`
- Add test:
	- Clause references disconnected nodeId -> schema error.
- Keep existing passing test for connected nodeId-qualified clauses.

#### Integration

- File: `src/lib/flow/store/graphStore.schemaPlane.integration.test.ts`
- Add graph where join clause nodeId drifts from connected edge source; assert schema error on join node.

#### Regression

- Add test ensuring legacy positional join clause (without nodeIds) still behaves as before for compatible two-input joins.

### Exit Criteria

- Join schema cannot silently validate against disconnected clause node references.
- Existing valid join cases continue to pass.

---

## Phase 3 — Add SQL Opt-In Declared Output Schema Path

### Commit Message

`Add declared SQL output columns to schema propagation`

### Implementation Steps

1. Extend `TransformSqlParamsSchema` with optional declared output columns:
	- Example field: `declared_output_columns: [{ name, type?, nullable? }]`
2. Update transform defaults to include empty declaration list.
3. In SQL schema function path:
	- If declared columns exist -> return explicit table schema from declaration.
	- Else retain current passthrough behavior.

### Tests

#### Unit

- File: `src/lib/flow/schema/schemaFunctions/transform.test.ts`
- Add tests:
	- SQL with `declared_output_columns` produces explicit table schema.
	- SQL without declaration preserves existing behavior.

#### Integration

- File: `src/lib/flow/store/graphStore.schemaPlane.integration.test.ts`
- Add graph where SQL declared output enables downstream select/derive validation.

#### Regression

- Add test verifying existing SQL nodes (without declarations) remain non-breaking.

### Exit Criteria

- SQL can participate in schema validation when declaration is provided.
- No regression for existing SQL config payloads.

---

## Validation Matrix (Run After Each Phase + Final)

1. Targeted tests for touched files (unit/integration/regression).
2. Re-run full transform schema-function test file.
3. Re-run relevant schema-plane integration test file subset.

---

## Completion Checklist

- [x] Phase 1 implemented, tested, committed.
- [x] Phase 2 implemented, tested, committed.
- [x] Phase 3 implemented, tested, committed.
- [x] Final targeted test sweep green.
- [x] Plan checklist fully checked and up to date.
