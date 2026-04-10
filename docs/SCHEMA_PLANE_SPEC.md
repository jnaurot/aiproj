# Schema Plane — Codex Implementation Specification

**Version:** 1.0.0  
**Status:** Implementation Ready  
**Date:** April 2026

---

## Overview

This document is the complete, step-by-step implementation guide for the **Schema Plane**: a second, independent computational layer that runs continuously alongside the existing execution plane, propagating abstract type and shape information through the pipeline graph **without executing any node computations**.

The schema plane eliminates the configure → run → discover error → reconfigure → re-run cycle by treating schema propagation as a separate, always-on, computationally free operation. Every time the user edits a node parameter the schema plane immediately re-propagates type and shape information forward through the graph. Incompatibilities appear as inline errors in real time, before any data is processed.

---

## The Two Planes

| Property | Execution Plane | Schema Plane |
|---|---|---|
| When it runs | On explicit run command | Continuously, on every parameter change |
| What it computes | Full data transformations; produces real artifacts | Abstract type / shape metadata only; no data touched |
| Speed | Seconds to hours | Milliseconds; purely synchronous pure functions |
| Cost | CPU/GPU compute; storage I/O | Negligible; in-process function calls |
| Answers | What did this pipeline produce? | Is this pipeline structurally valid? |
| Errors caught | Data quality; convergence; runtime exceptions | Type mismatches; shape incompatibilities; missing connections; semantic contract violations |

---

## Three-Tier Validation Hierarchy

| Tier | Name | Trigger | Cost | Catches |
|---|---|---|---|---|
| 1 | **Schema Plane** | Every parameter edit; instantaneous | Zero | Shape/type mismatches; structural errors; missing ports |
| 2 | **Sample Plane** | On Preview click; 50-row samples | Low | Statistical property violations; normalization errors; data distribution mismatches |
| 3 | **Execution Plane** | On explicit Run; full data | High | Convergence; runtime failures; data quality at scale |

---

## Architecture

### Schema Function Contract

Every node kind must provide a **schema function**: a pure, side-effect-free TypeScript function that maps upstream schemas and current node parameters to a derived output schema. The function must be deterministic: identical inputs always produce identical outputs.

```typescript
// src/lib/flow/types/schemaPlane.ts

export type SchemaPlaneColumn = {
  name:       string;
  type:       "string" | "number" | "boolean" | "datetime" | "binary" | "tensor" | "unknown";
  nullable:   boolean;
  properties: AbstractProperties;
};

export type SchemaPlaneOutput = {
  mode:    "table" | "tensor" | "text" | "binary" | "model_artifact" | "opaque";
  columns: SchemaPlaneColumn[];
  shape?:  (number | string)[];   // tensor shape; strings = symbolic dims (e.g. "B", "T")
  dtype?:  "float32" | "float16" | "int32" | "int64" | "uint8";
};

export type SchemaFunction = (
  inputs: SchemaPlaneOutput[],    // one per connected input handle, in handle order
  params: Record<string, unknown> // node.data.params at time of call
) => SchemaPlaneResult;

export type SchemaPlaneResult =
  | { ok: true;  output: SchemaPlaneOutput }
  | { ok: false; error: SchemaError };

export type SchemaError = {
  code:    SchemaErrorCode;
  message: string;
  handles: string[]; // which input/output handles are implicated
};

export type SchemaErrorCode =
  | "SHAPE_MISMATCH"
  | "TYPE_MISMATCH"
  | "MISSING_REQUIRED_INPUT"
  | "CARDINALITY_CONFLICT"
  | "PROPERTY_VIOLATION"
  | "OPAQUE_DEPENDENCY"
  | "CYCLE_DETECTED";
```

### Abstract Properties

Schema functions may optionally propagate abstract data properties beyond basic types. These enable deeper structural validation without running any computation.

```typescript
export type AbstractProperties = {
  range?:        [number, number] | null;
  normalized?:   boolean;
  device?:       "cpu" | "gpu" | "any";
  dtype?:        "float32" | "float16" | "int32" | "int64" | "uint8";
  non_negative?: boolean;
  cardinality?:  "one" | "many" | "stream";
  class_set?:    string[] | null;
  consume_once?: boolean;
  sample_rate?:  number;
  architecture_signature?: string;
  [key: string]: unknown; // open for extension
};
```

| Property | Example Value | Use Case |
|---|---|---|
| `range` | `[0, 1]` | Detect feeding unnormalized data into a layer expecting `[0,1]` |
| `normalized` | `true` | Warn if normalization step is missing before batch norm |
| `device` | `"gpu"` | Detect CPU tensor fed into GPU-only model layer |
| `dtype` | `"float16"` | Detect precision mismatch between encoder and decoder |
| `non_negative` | `true` (after ReLU) | Validate `log()` inputs; flag impossible negative audio energy |
| `cardinality` | `"stream"` | Distinguish single-item vs batch vs streaming output contracts |
| `class_set` | `["yes","no","maybe"]` | Verify model output classes match evaluation node expected classes |

### Propagation Algorithm

```typescript
// Pseudocode: src/lib/flow/schema/schemaPropagator.ts
function propagateSchemas(state: GraphState): SchemaPlaneState {
  const order = topologicalSort(state.nodes, state.edges);
  const outputs = new Map<nodeId, SchemaPlaneResult>();

  for (const nodeId of order) {
    const node    = state.nodes.find(n => n.id === nodeId);
    const fn      = schemaRegistry.get(node.data.kind);

    if (!fn) {
      outputs.set(nodeId, { ok: true, output: OPAQUE_SCHEMA });
      continue;
    }

    const upstreams = getUpstreamEdges(nodeId, state.edges)
      .map(e => outputs.get(e.source)?.output ?? UNKNOWN_SCHEMA);

    outputs.set(nodeId, fn(upstreams, node.data.params));
  }

  return buildSchemaPlaneState(outputs, state.edges);
}
```

- Cycles are detected via DFS (white/grey/black coloring).
- All nodes in a cycle emit `CYCLE_DETECTED` error.
- Downstream nodes of the cycle receive `OPAQUE_SCHEMA` and continue propagating.
- Nodes with no registered schema function return `OPAQUE_SCHEMA` with `ok: true` (not an error).

### Schema Registry

```typescript
// src/lib/flow/schema/schemaRegistry.ts
const registry = new Map<string, SchemaFunction>();

export const OPAQUE_SCHEMA:  SchemaPlaneOutput = { mode: "opaque",   columns: [] };
export const UNKNOWN_SCHEMA: SchemaPlaneOutput = { mode: "opaque",   columns: [] }; // disconnected input

export function registerSchemaFunction(kind: string, fn: SchemaFunction): void {
  registry.set(kind, fn);
}

export function getSchemaFunction(kind: string): SchemaFunction | undefined {
  return registry.get(kind);
}

// Called once at app init before the first propagation
export function registerAllBuiltinSchemaFunctions(): void {
  // Registers source, transform, audio, ml schema functions
}
```

---

## New Files to Create

| File | Purpose | Key Exports |
|---|---|---|
| `src/lib/flow/types/schemaPlane.ts` | Core runtime types | `SchemaPlaneOutput`, `SchemaFunction`, `SchemaPlaneResult`, `SchemaPlaneState`, `AbstractProperties` |
| `src/lib/flow/schema/schemaPlane.ts` | Zod validation schemas | `SchemaPlaneOutputSchema`, `SchemaErrorSchema`, `AbstractPropertiesSchema` |
| `src/lib/flow/schema/schemaRegistry.ts` | Function registry | `registerSchemaFunction`, `getSchemaFunction`, `OPAQUE_SCHEMA`, `UNKNOWN_SCHEMA` |
| `src/lib/flow/schema/schemaPropagator.ts` | Propagation engine | `propagateSchemas`, `buildSchemaPlaneState` |
| `src/lib/flow/schema/schemaFunctions/source.ts` | Source node schema fns | `schemaFn_source_file`, `schemaFn_source_api`, `schemaFn_source_stream` |
| `src/lib/flow/schema/schemaFunctions/transform.ts` | Transform schema fns (34 kinds) | One function per transform kind |
| `src/lib/flow/schema/schemaFunctions/audio.ts` | Audio node schema fns | `schemaFn_audio_source`, `schemaFn_spectrogram`, `schemaFn_audio_augment` |
| `src/lib/flow/schema/schemaFunctions/ml.ts` | ML training schema fns | `schemaFn_training_job`, `schemaFn_diffusion`, `schemaFn_moe_router` |
| `src/lib/flow/store/graphStore.schemaPlane.ts` | Store integration | `createSchemaPlaneManager`, `selectNodeSchema`, `selectEdgeSchema` |
| `src/lib/flow/components/ui/SchemaPlaneOverlay.svelte` | UI overlay component | Schema view mode toggle; edge schema labels; error highlights |
| `src/lib/flow/components/ui/ConfigurationOracle.svelte` | Config hints from schema | Auto-populate param fields from upstream schema |

---

## Implementation Steps

> **Baseline requirement:** Before beginning, run `tsc --noEmit` and record the error count as `BASELINE`. After every step, verify error count ≤ `BASELINE`. Any step that increases the count must be fixed before proceeding.

---

### Step 1 — Type Foundation

**Goal:** Establish the complete TypeScript type surface and Zod schema surface. No runtime code, no store changes, no UI. Purely additive type declarations.

**Files to create:**
- `src/lib/flow/types/schemaPlane.ts`
- `src/lib/flow/schema/schemaPlane.ts`
- `src/lib/flow/schema/schemaRegistry.ts`

**Implementation notes:**
- `AbstractProperties` must use `Record` with optional keys and the Zod schema must call `.passthrough()` to allow future properties without breaking validation.
- `SchemaPlaneOutput.shape` is a mixed array: numbers for concrete dimensions, strings for symbolic (e.g., `"B"` for batch, `"T"` for variable time axis). The engine never resolves symbolic dimensions — it passes them forward unchanged.
- `SchemaFunction` takes a readonly array of `SchemaPlaneOutput` (one per connected input handle). A disconnected handle passes `UNKNOWN_SCHEMA`. Source nodes receive an empty array.
- `SchemaErrorCode` is an exported enum or union of string literals.
- `SchemaPlaneState` shape: `{ nodeSchemas: Record<string, SchemaPlaneResult>; edgeSchemas: Record<string, SchemaPlaneOutput>; }`.

**Tests — `src/lib/flow/schema/schemaPlane.test.ts`:**
1. `SchemaPlaneOutputSchema.parse()` accepts a minimal valid output (`mode: "table"`, `columns: []`)
2. `SchemaPlaneOutputSchema.parse()` accepts a tensor output with `shape: [32, "T", 128]`
3. `SchemaPlaneOutputSchema.parse()` accepts `opaque` mode with no columns or shape
4. `AbstractPropertiesSchema` uses passthrough: unknown extra keys are preserved
5. `SchemaErrorSchema.parse()` rejects missing `handles` field
6. `OPAQUE_SCHEMA` sentinel passes `SchemaPlaneOutputSchema.parse()`
7. `UNKNOWN_SCHEMA` sentinel passes `SchemaPlaneOutputSchema.parse()`
8. `registerSchemaFunction` followed by `getSchemaFunction` returns the same function reference

**Regression gate:** `tsc --noEmit` ≤ BASELINE. No imports of `schemaPlane` in any existing store or UI file.

---

### Step 2 — Schema Propagation Engine

**Goal:** Implement `schemaPropagator.ts`: the topological forward-pass engine. This step has no side effects on the existing store — it is a pure function called explicitly in Step 3.

**File to create:** `src/lib/flow/schema/schemaPropagator.ts`

**Implementation notes:**
- `propagateSchemas(nodes, edges, params, registry)` accepts nodes and edges from `GraphState`. **Do not import `graphStore` directly** — keep the engine pure and independently testable.
- Cycle detection: DFS with white/grey/black coloring. Nodes in a cycle emit `SchemaError` with `CYCLE_DETECTED`. Downstream nodes of the cycle receive `OPAQUE_SCHEMA` and continue propagating normally.
- `buildSchemaPlaneState(outputs, edges)` derives the edge schema map by reading each edge's source node result. Every edge gets the source node's output schema as its schema.

**Tests — `src/lib/flow/schema/schemaPropagator.test.ts`:**
1. Empty graph → empty `SchemaPlaneState` with no errors
2. Single source node with registered function → returns that function's output
3. Single source node with no registered function → `OPAQUE_SCHEMA`, `ok: true`
4. Two nodes A → B: B receives A's output as its first input
5. Three nodes A → B → C: C receives B's derived output (which was derived from A)
6. Graph with cycle A → B → A: both nodes report `CYCLE_DETECTED`; downstream node C of B receives `OPAQUE_SCHEMA`
7. Diamond pattern A → B, A → C, B+C → D: D receives both B and C outputs
8. Schema error in B does not block C (sibling branch propagates independently)
9. Schema error in B marks B's edge to D as carrying an error schema
10. Disconnected input handle produces `UNKNOWN_SCHEMA` at that position in the input array

**Regression gate:** `tsc --noEmit` unchanged. No changes to any existing file.

---

### Step 3 — GraphState Integration

**Goal:** Add `schemaPlane: SchemaPlaneState` to `GraphState` and wire the propagation engine to run reactively whenever nodes or edges change. Schema propagation must be **synchronous, in-process, never async, never debounced**.

**Files to change:**
- `src/lib/flow/types/base.ts` — add `schemaPlane: SchemaPlaneState` to `GraphState`
- `src/lib/flow/store/graphStore.ts` — call `propagateSchemas` after any state mutation that changes nodes or edges

**File to create:** `src/lib/flow/store/graphStore.schemaPlane.ts`

**Implementation notes:**
- `schemaPlane` is initialized to an empty state in `emptyGraph` and `hardResetGraph`.
- `schemaPlane` is **NOT persisted to localStorage** — it is always recomputed from node/edge structure on load.
- The propagation trigger must be added to `withGraphMeta` (or the equivalent centralized state mutation wrapper). Every state mutation passing through `withGraphMeta` triggers a synchronous re-propagation. This ensures schema state is always consistent without manual trigger calls at each mutation site.
- Call `registerAllBuiltinSchemaFunctions()` once in the store's init path before the first propagation runs.
- `createSchemaPlaneManager(deps)` follows the same factory pattern as other manager modules. Exposes read-only selectors: `getNodeSchemaResult(nodeId)`, `getEdgeSchema(edgeId)`, `getSchemaErrors()`, `hasSchemaErrors()`, `getConfigurationHints(nodeId)`.

**Tests — `src/lib/flow/store/graphStore.schemaPlane.test.ts`:**
1. State after `hardResetGraph` has `schemaPlane` field that is not null/undefined
2. After `addNode`, `schemaPlane` contains an entry for the new nodeId
3. After `addEdge` connecting A → B, `schemaPlane.edgeSchemas` contains an entry for that edgeId
4. After `updateNodeConfig` with a param change, `schemaPlane` is immediately updated (synchronous)
5. After `removeNode`, `schemaPlane` entry for that nodeId is absent
6. After `removeEdge`, `schemaPlane` entry for that edgeId is absent
7. `loadGraphDocument` triggers a full re-propagation on load
8. `hasSchemaErrors()` returns `false` for an empty graph
9. `schemaPlane` field is NOT written to localStorage (`persist` does not include it)
10. After `loadGraphFromLocalStorage`, `schemaPlane` is recomputed from loaded nodes/edges

**Regression gate:** `tsc --noEmit` ≤ BASELINE + 0. All existing `graphStore` tests pass unchanged. The new `schemaPlane` field must not break any existing destructured `GraphState` usage.

---

### Step 4 — Core Transform Schema Functions (all 34 transform kinds)

**Goal:** Implement and register schema functions for all 34 transform node kinds.

**File to create:** `src/lib/flow/schema/schemaFunctions/transform.ts`

**Transform categories and schema behavior:**

| Category | Examples | Schema Behavior |
|---|---|---|
| **Passthrough** | filter, sort, dedupe, limit, sample | Output schema === input schema |
| **Reducing** | select, rename, drop_columns, cast | Output is subset/rename of input; error if column not found |
| **Expanding** | derive, aggregate, join, pivot, embed, encode | Output extends or merges input; derived from params |
| **Opaque** | custom_script, llm_transform, external_api | Returns `OPAQUE_SCHEMA`; does not block propagation |

**Key implementation notes:**
- **Passthrough:** Return first input's schema unchanged. If no input connected, return `MISSING_REQUIRED_INPUT`.
- **Select:** Output has only `params.columns`. If a referenced column does not exist in the input schema, return `SHAPE_MISMATCH` with `handles` set to the input edge's handle name. This provides column-not-found errors at configuration time.
- **Join:** Requires two input schemas. Merges left and right schemas. Right columns prefixed by `params.right_prefix` if set. Key column types must match — return `TYPE_MISMATCH` if they differ.
- **Aggregate:** Output schema = `params.group_by` columns (same type as input) + one column per aggregation (float64 for numeric aggregations). Validates numeric types for sum/mean.
- **Embed:** Output = input columns minus the text column + one new `tensor` column with `shape: [params.output_dim]`.
- **Pivot:** Returns `OPAQUE_SCHEMA` (pivot columns depend on data values, cannot be statically determined).

**Tests — `src/lib/flow/schema/schemaFunctions/transform.test.ts`:**

One `describe` block per transform kind. Minimum 3 tests per kind:
1. Happy path: valid input schema + valid params → correct output schema
2. Missing required input: no upstream connected → `MISSING_REQUIRED_INPUT` error
3. Invalid params: params referencing non-existent column → `SHAPE_MISMATCH` error with correct `handles` field

Additional tests for expanding transforms:
- join: left + right schemas produce merged schema with correct column names
- aggregate: output has only group_by + aggregation columns, not all input columns
- embed: output has original columns minus text column plus one tensor column of correct shape

**Regression gate:** `tsc --noEmit` unchanged. No changes to any existing transform Zod schema file.

---

### Step 5 — Source Node Schema Functions

**Goal:** Implement schema functions for all source node kinds. Source nodes have no inputs (empty inputs array) and derive their output schema from params alone or from the priming sample if available.

**File to create:** `src/lib/flow/schema/schemaFunctions/source.ts`

**Implementation notes:**

- **Source file (CSV, JSON, Parquet, Excel, Text):** Derive output schema from `params.priming.sample_schema` if present (using the existing `SourcePrimingSchema` system). If no priming sample is available, return `OPAQUE_SCHEMA` with an annotation: `"Run source node to infer schema."` Do not block propagation.
- **Source API:** Same as file source. Sources with `memoizable: false` must propagate `properties: { cardinality: "one", consume_once: true }` on their output.
- **Source database:** If `params.declared_schema` is set, derive output columns from it. If not set, return `OPAQUE_SCHEMA`.
- **Source stream:** Always return `{ cardinality: "stream", consume_once: true }`. Downstream nodes expecting batch input receive a `CARDINALITY_CONFLICT` error.
- Source nodes that receive a non-empty `inputs` array must return `MISSING_REQUIRED_INPUT` (sources must have no inputs).

**Tests — `src/lib/flow/schema/schemaFunctions/source.test.ts`:**
1. CSV source with priming sample: output schema matches sample column names and types
2. CSV source with no priming sample: output is `OPAQUE_SCHEMA`, `ok: true`
3. API source with `memoizable: false`: output properties include `consume_once: true`
4. Stream source: output properties include `cardinality: "stream"`
5. Database source with `declared_schema`: output columns derived from `declared_schema`
6. Source node receives non-empty inputs array: returns `MISSING_REQUIRED_INPUT`

**Regression gate:** `tsc --noEmit` unchanged. Priming system is read-only from the schema function — no writes to `params.priming`.

---

### Step 6 — Edge Schema Validation and Inline Error Display

**Goal:** Display schema errors directly on edges in the graph UI. This is the first visible user-facing change.

**Files to change:**
- Edge component(s) in `src/lib/flow/components/edges/` — read `schemaPlane.edgeSchemas[edgeId]` and render validation state
- `src/lib/flow/store/graphStore.schemaPlane.ts` — add `getEdgeValidationState(edgeId)` selector

**Edge validation states:**

| State | Color | Condition | User Message |
|---|---|---|---|
| `valid` | Green | Both ends have schemas; types are compatible | (none) |
| `error` | Red | `SchemaError` on source or destination due to this edge | Error code + `SchemaError.message` |
| `warning` | Amber | `OPAQUE_SCHEMA` at source | `"Schema unverified: upstream output is opaque. Run source to infer."` |
| `neutral` | Grey | `UNKNOWN_SCHEMA`: input handle not connected | (no message; edge does not exist) |

**Tests:**
1. Edge between two nodes with compatible schemas renders in `valid` state
2. Edge where source schema has a `SchemaError` renders in `error` state
3. Edge where source schema is `OPAQUE_SCHEMA` renders in `warning` state
4. `SchemaError` tooltip displays `error.message` text
5. Changing a parameter that resolves a schema error updates edge to `valid` state synchronously
6. Changing a parameter that creates a schema error updates edge to `error` state synchronously

**Regression gate:** All existing edge rendering tests pass. Schema validation state must not affect execution-plane edge behavior (active, stale, idle states remain independent).

---

### Step 7 — Schema Plane UI Mode (Separate View Toggle)

**Goal:** Add a "Schema View" toggle to the toolbar that switches the canvas to a dedicated schema visualization mode.

**Files to create/change:**
- `src/lib/flow/components/ui/SchemaPlaneOverlay.svelte` — new overlay component
- Toolbar component — add Schema View toggle button
- `src/lib/flow/store/graphStore.ts` — add `viewMode: "execution" | "schema"` to `GraphState`

**Schema view behavior:**
- Edge labels show the schema flowing through them: table schemas show top 3 column names and types; tensor schemas show shape string e.g. `"(32, T, 128) float32"`; opaque shows `"?"`.
- Node cards show compact schema summary: input handles annotated with expected schema; output handles annotated with declared schema.
- Nodes with schema errors render with a red border and error count badge.
- Status bar shows total schema errors. If zero: `"Schema valid — structure verified."`.
- In schema view, run/stop/reset controls remain visible but are dimmed.
- `viewMode` defaults to `"execution"` — existing behavior unchanged on load.

**Tests:**
1. Toggling to schema view sets `viewMode: "schema"` in store
2. Toggling back sets `viewMode: "execution"`
3. In schema view, edge with table schema shows column names in label
4. In schema view, edge with tensor schema shows shape string
5. In schema view, node with schema error has visible error indicator
6. Schema errors count in status bar matches `getSchemaErrors().length`
7. `"Schema valid"` message shown when `hasSchemaErrors()` returns `false`

**Regression gate:** `viewMode` defaults to `"execution"`. No existing keyboard shortcuts conflict with schema view toggle.

---

### Step 8 — Abstract Property Propagation

**Goal:** Extend schema function implementations to propagate abstract properties through transforms. This elevates the schema plane from structural type checking to semantic contract validation.

**Files to change:** `src/lib/flow/schema/schemaFunctions/transform.ts` (extend existing functions)

**Property propagation rules by transform:**

| Transform | Rule | Detail |
|---|---|---|
| `filter` | Passthrough | All properties preserved unchanged |
| `normalize` | Sets `normalized: true`, `range: [0,1]` | Output carries `normalized: true` regardless of input |
| `relu` | Sets `non_negative: true` | Output range lower-bounded to 0. If input range was `[a, b]`, output is `[max(0,a), b]` |
| `batch_norm` | Sets `normalized: true`, clears `range` | Output is approximately zero-mean unit-variance; previous range annotation cleared |
| `to_gpu` | Sets `device: "gpu"` | All output tensors carry `device: "gpu"` |
| `to_cpu` | Sets `device: "cpu"` | All output tensors carry `device: "cpu"` |
| `cast_dtype` | Sets `dtype` from params | Validates `params.target_dtype` is a known dtype value |

**Property violation detection:** Schema functions for nodes that declare property requirements must emit `SchemaError` with code `PROPERTY_VIOLATION` when a required property is not met. Example: a `log()` transform requiring `non_negative: true` emits `PROPERTY_VIOLATION` if the input column's `non_negative` is `undefined` or `false`.

**Tests:**
1. `normalize` transform: output column carries `range: [0,1]` and `normalized: true`
2. `batch_norm` after `normalize`: output carries `normalized: true`, `range` is cleared
3. `relu` after `normalize`: output `non_negative: true`, range lower-bounded to 0
4. `to_gpu`: all output tensors carry `device: "gpu"`
5. `log()` on non-guaranteed-positive input: `SchemaError PROPERTY_VIOLATION`
6. `log()` on input with `non_negative: true`: no error
7. Chain: `normalize → relu → model`: model receives `normalized`, `non_negative`, correct `dtype`

**Regression gate:** `tsc --noEmit` unchanged. All existing transform tests pass. Abstract properties are additive — no existing schema function return value is broken.

---

### Step 9 — Configuration Oracle

**Goal:** When a user opens a node's configuration editor, any field derivable from the upstream schema is pre-populated automatically.

**Files to change:**
- `src/lib/flow/components/editors/` (each editor Svelte component) — inject upstream `SchemaPlaneOutput` as a prop; use it to pre-populate fields
- `src/lib/flow/store/graphStore.schemaPlane.ts` — add `getConfigurationHints(nodeId): ConfigurationHints`

**ConfigurationHints type:**
```typescript
export type ConfigurationHints = {
  suggestions:      Record<string, unknown>; // param path → suggested value
  availableColumns: string[];                // column names from upstream schema
  upstreamShape?:   (number | string)[];     // tensor shape from upstream
  upstreamDtype?:   string;
  upstreamClassSet?: string[];
};
```

**Editor integration rules:**
- Fields are pre-populated from hints but remain fully editable.
- A small `"from schema"` indicator marks auto-populated fields.
- If a param already has a non-default user-set value, hints display as a suggestion badge (`"Schema suggests: 128"`) but do **not** replace the existing value.
- Hints apply only to empty/default fields on first connect.

**High-value oracle integrations:**
- `SpectrogramTransform` editor: pre-populates `sample_rate` from upstream `AudioSource` schema property
- `TrainingJob` architecture editor: pre-populates `input_dim` from upstream schema shape; pre-populates `num_classes` from upstream `class_set`
- `Join` transform editor: column picker for join key shows only columns present in **both** left and right upstream schemas (intersection)
- `Select`/`Rename` editors: column pickers populated with upstream column names; user cannot reference non-existent columns
- `Aggregate` editor: `group_by` picker shows upstream column names; aggregation target picker shows only numeric columns

**Tests:**
1. `getConfigurationHints` returns `availableColumns` matching upstream schema columns
2. `getConfigurationHints` returns `upstreamShape` when upstream output is tensor
3. `getConfigurationHints` returns `upstreamClassSet` when upstream has `class_set` property
4. Editor renders `"from schema"` indicator on pre-populated field
5. User editing a pre-populated field removes the indicator without error
6. Hints do not overwrite a non-default user-set value
7. Join key picker shows only columns present in both left and right upstream schemas

**Regression gate:** All existing editor tests pass. Default param values are not changed by hint injection.

---

### Step 10 — Audio and ML Schema Functions

**Goal:** Implement schema functions for audio and ML node kinds. These have the highest schema complexity and highest user benefit.

**Files to create:**
- `src/lib/flow/schema/schemaFunctions/audio.ts`
- `src/lib/flow/schema/schemaFunctions/ml.ts`

**Schema function specifications:**

**`AudioSource`**
- Input: empty (source node)
- Output: `mode: "tensor"`, `shape: ["B", "samples"]`, `dtype: "float32"`, `properties: { device: "cpu", cardinality: "many", sample_rate: params.sample_rate }`
- If no file path and no priming sample: return `OPAQUE_SCHEMA` with annotation

**`SpectrogramTransform`**
- Input: `AudioSource` output (tensor, shape `["B", "samples"]`)
- Validation: input mode must be `"tensor"`, input dtype must be `float32`; if not, return `SHAPE_MISMATCH`
- Output: `mode: "tensor"`, `shape: ["B", "T", params.n_mels]`, `dtype: "float32"`, `properties: { device: input.device, normalized: false, non_negative: true }`

**`AudioPartition`**
- Three output handles: `train`, `validation`, `test`; each has same schema as input
- Equivalent to split for schema purposes

**`TrainingJob`**
- Input: `[train_data, validation_data]`
- Validation: both inputs must be connected; if `params.architecture_config.input_dim` is set and upstream tensor shape last dim differs → `SHAPE_MISMATCH`
- Output: `mode: "model_artifact"`, `properties: { architecture_signature: hash(params.architecture_config), input_shape: upstream shape, num_classes: params.num_classes, dtype: params.precision }`

**`DiffusionTrainingJob`**
- Output: `mode: "model_artifact"`, `properties: { architecture_signature, diffusion_steps: params.num_steps, noise_schedule: params.noise_schedule }`

**`DiffusionSampler`**
- Input: `[model_artifact]`
- Output: tensor with shape from `params.output_shape`; requires model_artifact input with matching signature

**`MoERouter`**
- Output: routing weights tensor `["B", params.n_experts]`

**`EvaluationNode`**
- Input: `[model_artifact, test_data]`
- Validation: if `test_data.class_set` is defined and `model.num_classes` does not match `class_set.length` → `PROPERTY_VIOLATION`
- Output: `mode: "table"`, columns = metric columns from `params.metrics_config`

**Tests — `audio.test.ts` and `ml.test.ts`:**
1. `AudioSource`: output shape is `["B", "samples"]`, `non_negative: false`
2. `SpectrogramTransform`: output shape is `["B", "T", 128]` when `n_mels=128`
3. `SpectrogramTransform`: input of wrong mode (table, not tensor) → `SHAPE_MISMATCH`
4. `SpectrogramTransform`: output has `non_negative: true`
5. `TrainingJob`: disconnected validation handle → `MISSING_REQUIRED_INPUT`
6. `TrainingJob`: `params.input_dim` mismatch with upstream shape → `SHAPE_MISMATCH`
7. `EvaluationNode`: `class_set` length 10 vs `num_classes` 8 → `PROPERTY_VIOLATION`
8. `EvaluationNode`: output is table with metric columns from `params.metrics_config`

**Regression gate:** `tsc --noEmit` unchanged. Audio and ML schema functions are in isolated files and do not alter existing transform or source schema functions.

---

### Step 11 — Schema-Aware Pre-Run Guard

**Goal:** Before dispatching any run, check `hasSchemaErrors()`. If schema errors exist on nodes in the run path, show an inline warning. This is **informational, not a hard block**.

**File to change:** `src/lib/flow/store/graphStore.run.ts`

**Behavior:**
- `hasSchemaErrors()` is `false`: proceed normally, no interruption.
- Errors exist only in nodes **outside** the run path: show dismissible amber warning but proceed automatically.
- Errors exist in nodes **on** the run path: show blocking amber dialog listing each error node by name and message. Two actions: `"Proceed Anyway"` and `"Cancel Run to Fix Schema"`.
- User choice is remembered per session (suppressed after 3 consecutive dismissals).
- `"Cancel Run to Fix Schema"` does not dispatch; opens the schema plane panel automatically.

**Tests:**
1. Run with no schema errors: dispatches immediately, no dialog
2. Run with schema errors outside run path: amber warning shown, run proceeds
3. Run with schema errors inside run path: blocking dialog shown
4. `"Proceed Anyway"`: run dispatches
5. `"Cancel Run to Fix Schema"`: run does not dispatch, schema panel opens
6. Dialog suppression: after 3 consecutive `"Proceed Anyway"` actions, dialog is suppressed for session

**Regression gate:** All existing run dispatch tests pass. The pre-run guard adds zero latency when no schema errors exist.

---

### Step 12 — Component Internal Schema Propagation

**Goal:** When a component node is wired into the parent graph, the schema plane propagates through the component boundary. The component's internal structure determines its output schema.

**Files to change:**
- `src/lib/flow/schema/schemaPropagator.ts` — extend to handle `component` kind
- `src/lib/flow/store/graphStore.persistence.ts` — expose component internal graph to propagator

**Implementation notes:**
- When the propagation engine encounters a node with `kind === "component"`, look up the component's revision graph from `componentContractDraftCache` or the committed revision.
- Run a **nested** `propagateSchemas` pass on the component's internal nodes/edges.
- The output schema of the component's terminal internal node becomes the component node's output schema in the parent graph.
- If no committed revision: return `OPAQUE_SCHEMA` for the component node.
- If internal propagation produces errors: component node carries `COMPONENT_INTERNAL_ERRORS` state with a count. Parent graph edge from the component shows `warning`, not `error`.
- Component node badge must show schema error count when internal schema errors are present.
- The schema plane is **read-only** from `graphStore.persistence.ts` — no writes to the revision.

**Tests:**
1. Component with two internal nodes: parent graph schema for the component output matches the internal terminal node's schema
2. Component with schema error in internal node: component badge shows schema error count
3. Component with no revision: parent graph schema for component is `OPAQUE_SCHEMA`
4. Parent node downstream of component receives component's output schema as its input
5. Schema error in parent node due to component output mismatch: error correctly attributed to parent-component edge

**Regression gate:** All existing component tests pass. Component schema propagation does not trigger any write to the component revision.

---

## Integration Tests

### Suite 1 — Pipeline Structural Validation

**File:** `src/lib/flow/store/graphStore.schemaPlane.integration.test.ts`

| ID | Test Name | Fixture | Expected |
|---|---|---|---|
| INT-01 | Source → filter → select: all valid | CSV source with priming sample, filter on column A, select [A, B] | Zero schema errors; select output has columns A, B only |
| INT-02 | Select references non-existent column | CSV source, select references column Z not in priming sample | `SHAPE_MISMATCH` error on select node; edge red |
| INT-03 | Join with incompatible key types | Source A: key is string. Source B: key is number. Join on that key. | `TYPE_MISMATCH` on join node |
| INT-04 | Full audio preprocessing chain | `AudioSource → SpectrogramTransform(n_mels=128) → Normalize → TrainingJob(input_dim=128)` | Zero errors; TrainingJob receives shape `["B","T",128]`, `normalized: true` |
| INT-05 | n_mels mismatch with model input_dim | `SpectrogramTransform(n_mels=64) → TrainingJob(input_dim=128)` | `SHAPE_MISMATCH` on TrainingJob; edge red |
| INT-06 | Missing validation data for TrainingJob | TrainingJob with train connected, validation disconnected | `MISSING_REQUIRED_INPUT` on TrainingJob |
| INT-07 | Class set mismatch in evaluation | Source with `class_set` of 5 classes; `TrainingJob(num_classes=10)` | `PROPERTY_VIOLATION` on EvaluationNode |
| INT-08 | Opaque transform does not block downstream | `Source → custom_script → filter` | `custom_script` is OPAQUE; filter edge is amber warning; no hard error |
| INT-09 | Log of negative values detected | `Source → Log` transform (no prior relu/normalize) | `PROPERTY_VIOLATION`: input may be negative |
| INT-10 | Log after relu is valid | `Source → ReLU → add_epsilon(0.001) → Log` | Zero errors; `non_negative` preserved through chain |
| INT-11 | GPU-CPU mismatch detected | `Source → to_gpu → TrainingJob(device: "cpu")` | `PROPERTY_VIOLATION`: device mismatch on TrainingJob input |
| INT-12 | Parameter change updates schema immediately | `SpectrogramTransform(n_mels=64)` connected to `TrainingJob(input_dim=128)`; then change `n_mels` to 128 | Error present before change; zero errors after change; update is synchronous |
| INT-13 | Symbolic dimension propagates through chain | `AudioSource → Spectrogram → Reshape(keep_time=true)` | Output shape contains `"T"` as symbolic dimension at time axis |
| INT-14 | Component propagation: parent sees internal schema | Component with internal `Spectrogram(n_mels=128)`; parent `TrainingJob(input_dim=128)` | Zero errors; parent correctly infers component output shape |
| INT-15 | Schema valid message in status bar | Simple 2-node graph with compatible schemas | `hasSchemaErrors()` = false; status bar shows `"Schema valid"` |

### Suite 2 — Configuration Oracle

**File:** `src/lib/flow/store/graphStore.configOracle.integration.test.ts`

| ID | Test Name | Fixture | Expected |
|---|---|---|---|
| ORC-01 | TrainingJob input_dim pre-populated from upstream tensor shape | `SpectrogramTransform(n_mels=128) → TrainingJob(input_dim: default)` | `getConfigurationHints` returns `upstreamShape: ["B","T",128]`; `input_dim` suggestion is 128 |
| ORC-02 | num_classes pre-populated from upstream class_set | Source with `class_set` of 7 classes → TrainingJob | `num_classes` suggestion is 7 |
| ORC-03 | sample_rate oracle for spectrogram | `AudioSource(sample_rate: 22050) → SpectrogramTransform` | SpectrogramTransform hints include `sample_rate: 22050` |
| ORC-04 | Column picker populated from upstream schema | CSV source with columns [A, B, C] → Select node | `getConfigurationHints` returns `availableColumns: ["A","B","C"]` |
| ORC-05 | Join key picker shows only shared columns | Source L: [id, name, val]. Source R: [id, desc, price]. Join node. | `availableColumns` for join key = `["id"]` (intersection only) |
| ORC-06 | Hints do not overwrite existing non-default value | TrainingJob with user-set `input_dim: 256`; upstream schema suggests 128 | Hint shows as suggestion badge; `input_dim` param remains 256 |
| ORC-07 | Upstream dtype hint propagated to precision field | Pipeline with `to_fp16` transform → TrainingJob | Hint for precision field = `"float16"` |

### Suite 3 — Pre-Run Guard

**File:** `src/lib/flow/store/graphStore.runGuard.integration.test.ts`

| ID | Test Name | Fixture | Expected |
|---|---|---|---|
| RG-01 | Clean pipeline runs without dialog | 3-node pipeline, zero schema errors | Run dispatches immediately |
| RG-02 | In-path schema error triggers blocking dialog | Schema error on node in run scope | `runBlockedReason` set; run not dispatched |
| RG-03 | Out-of-path schema error shows warning only | Schema error on node NOT in run scope | Amber warning emitted; run proceeds |
| RG-04 | Proceed anyway unblocks run | Blocking dialog shown; user calls `proceedWithSchemaErrors()` | Run dispatches; `runBlockedReason` cleared |
| RG-05 | Fixing schema error before run removes guard | Schema error present; user fixes param; run dispatched | No dialog; run dispatches cleanly |

---

## Regression Tests

### Suite 1 — Execution Plane Independence

The schema plane must have **zero effect** on run behavior, artifact production, or memoization.

| ID | Test Name | Fixture | Expected |
|---|---|---|---|
| REG-01 | Run succeeds even when schema errors exist | Graph with schema error; run dispatched via "Proceed Anyway" | Run completes normally; artifact produced; schema error unchanged |
| REG-02 | Schema errors do not affect memoization keys | Node A schema error; Node B downstream; B has memo hit | B gets memo hit regardless of A's schema state |
| REG-03 | Schema plane does not appear in persisted DTO | Save graph to localStorage; reload | `checkpointRegistry` present; `schemaPlane` field absent from DTO |
| REG-04 | Schema state recomputed on load | Load graph with known schema error configuration | After load, `hasSchemaErrors()` is true with correct errors |
| REG-05 | Schema plane does not affect run payload | Build run payload with schema errors in graph | Payload does not contain any schema plane fields |
| REG-06 | Schema plane does not affect nodeBindings | Schema error on node A; A runs and succeeds | `nodeBindings[A].status === "succeeded_up_to_date"` |
| REG-07 | Schema plane does not affect stale propagation | Mark node A stale; B depends on A (A has schema error) | B becomes stale as normal; schema error is independent |

### Suite 2 — Existing Node Configuration Regression

The configuration oracle must not corrupt existing node params or change default values.

| ID | Test Name | Fixture | Expected |
|---|---|---|---|
| REG-08 | Existing transform params unchanged after schema plane init | Load graph with all 34 transform kinds configured | Each node's params are byte-identical after schema plane activation |
| REG-09 | `loadGraphDocument` does not alter node params | Load a saved graph document; inspect all node params | All params match the saved values; no hint injection |
| REG-10 | Default values for new nodes unchanged | Add an LLM node; check its default params | Default params match `transformDefaults.ts` entries exactly |
| REG-11 | Column picker shows hints but does not auto-select | CSV source with columns [A,B,C] → Select node (no columns chosen yet) | `availableColumns` hint present; `params.columns` remains empty until user selects |
| REG-12 | No editor re-renders triggered by schema plane updates | Graph with no param changes; schema propagation runs | Zero Svelte component updates triggered by schema-only state changes |

### Suite 3 — Component System Regression

| ID | Test Name | Fixture | Expected |
|---|---|---|---|
| REG-13 | Component edit session does not corrupt parent graph schema | Enter component edit; change internal param; return | Parent graph schema correctly updates to reflect internal change |
| REG-14 | Component with no revision shows OPAQUE, not error | New empty component node in parent graph | Component schema state is `OPAQUE_SCHEMA`; no `SchemaError`; no red edge |
| REG-15 | `openComponentRevisionForEditing` does not reset schema state | Parent graph has valid schema; enter component edit | Parent graph `schemaPlane` field is preserved; internal graph gets its own fresh propagation |
| REG-16 | `returnFromComponentEditSession` triggers re-propagation | Change internal node param while in component edit; return | Parent graph schema is re-propagated on return; component output schema updated |

### Suite 4 — Performance Regression

| ID | Test Name | Fixture | Expected |
|---|---|---|---|
| REG-17 | Schema propagation completes in < 5ms for 50-node graph | 50 nodes, 60 edges, all with registered schema functions | `propagateSchemas()` returns in < 5ms (measured via `performance.now()`) |
| REG-18 | Schema propagation completes in < 20ms for 200-node graph | 200 nodes, 250 edges | `propagateSchemas()` returns in < 20ms |
| REG-19 | Rapid param edits do not queue or batch schema updates | 10 rapid param changes in sequence | Each change triggers synchronous propagation; no deferred updates; final state consistent with last change |
| REG-20 | Schema plane state size is bounded | 100-node graph; each column has 5 abstract properties | `schemaPlane` serialized size < 500KB |

### Suite 5 — TypeScript Compilation Regression

> `tsc --noEmit` error count must never exceed the BASELINE established before implementation begins. Checked after every step.

---

## Schema Function Reference

Complete reference of all node kinds requiring schema function registration. Kinds marked `Opaque` must still be registered (returning `OPAQUE_SCHEMA`) to prevent propagation engine warnings.

| Node Kind | Category | Output Mode | Schema Behavior |
|---|---|---|---|
| `source (file)` | Source | table | Derived from `priming.sample_schema`; `OPAQUE_SCHEMA` if no priming |
| `source (api)` | Source | table | Derived from priming; properties include `consume_once: true` |
| `source (db)` | Source | table | Derived from `declared_schema` param; `OPAQUE_SCHEMA` if absent |
| `source (stream)` | Source | table | `cardinality: "stream"`, `consume_once: true`; columns from priming |
| `filter` | Passthrough | table | Output schema === input schema |
| `sort` | Passthrough | table | Output schema === input schema |
| `dedupe` | Passthrough | table | Output schema === input schema |
| `limit` | Passthrough | table | Output schema === input schema; `cardinality` changes to `"one"` if `limit=1` |
| `sample` | Passthrough | table | Output schema === input schema |
| `select` | Reducing | table | Output has only `params.columns`; `SHAPE_MISMATCH` if column not in input |
| `rename` | Reducing | table | Output column names updated per `params.renames` map |
| `drop_columns` | Reducing | table | Output is input minus `params.columns`; `SHAPE_MISMATCH` if column not in input |
| `cast` | Reducing | table | Output column types updated per `params.casts`; validates target types |
| `derive` | Expanding | table | Input schema + new column `params.output_column` of type `params.output_type` |
| `aggregate` | Expanding | table | `group_by` columns + aggregation result columns; validates numeric types for sum/mean |
| `join` | Expanding | table | Merged left+right schemas; key column type must match; `TYPE_MISMATCH` on key mismatch |
| `pivot` | Expanding | table | `OPAQUE_SCHEMA` (pivot columns depend on data values) |
| `embed` | Expanding | tensor | Input cols minus text col plus tensor col of `shape: [params.output_dim]` |
| `categorical_encode` | Expanding | table | Input schema + one column per category value; `class_set` property set |
| `split` | Expanding | table | Three output handles (train/val/test); each has same schema as input |
| `audio_source` | Audio/ML | tensor | `["B","samples"]` float32; `sample_rate` property from params |
| `spectrogram` | Audio/ML | tensor | `["B","T",n_mels]` float32; `non_negative: true`; input must be tensor |
| `audio_augment` | Audio/ML | tensor | Passthrough of tensor schema; `memoizable: false` (stochastic) |
| `audio_partition` | Audio/ML | table | Three output handles; each has same schema as input |
| `training_job` | Audio/ML | model_artifact | model_artifact with `architecture_signature`, `input_shape`, `num_classes`, `dtype` |
| `diffusion_train` | Audio/ML | model_artifact | model_artifact with `diffusion_steps`, `noise_schedule` properties |
| `diffusion_sample` | Audio/ML | tensor | Output shape from `params.output_shape`; requires model_artifact input |
| `moe_router` | Audio/ML | tensor | Routing weights `["B", n_experts]`; `n_experts` from params |
| `evaluation` | Audio/ML | table | Metric columns from `params.metrics_config`; validates class count vs model |
| `custom_script` | Opaque | opaque | Always `OPAQUE_SCHEMA`; no schema validation possible |
| `llm_transform` | Opaque | opaque | Always `OPAQUE_SCHEMA` |
| `component` | Composite | derived | Schema derived from internal terminal node via nested propagation |

---

## Implementation Summary

| Step | Title | New Files | Changed Files | Test File(s) |
|---|---|---|---|---|
| 1 | Type Foundation | `types/schemaPlane.ts`, `schema/schemaPlane.ts`, `schema/schemaRegistry.ts` | None | `schema/schemaPlane.test.ts` |
| 2 | Propagation Engine | `schema/schemaPropagator.ts` | None | `schema/schemaPropagator.test.ts` |
| 3 | GraphState Integration | `store/graphStore.schemaPlane.ts` | `types/base.ts`, `store/graphStore.ts` | `store/graphStore.schemaPlane.test.ts` |
| 4 | Transform Schema Functions | `schema/schemaFunctions/transform.ts` | None | `schema/schemaFunctions/transform.test.ts` |
| 5 | Source Schema Functions | `schema/schemaFunctions/source.ts` | None | `schema/schemaFunctions/source.test.ts` |
| 6 | Edge Schema Validation UI | None | `components/edges/*`, `store/graphStore.schemaPlane.ts` | `components/edges/*.test.ts` |
| 7 | Schema Plane UI Mode | `components/ui/SchemaPlaneOverlay.svelte` | `components/toolbar/*`, `types/base.ts` | `components/ui/SchemaPlaneOverlay.test.ts` |
| 8 | Abstract Property Propagation | None | `schema/schemaFunctions/transform.ts` (extend) | Abstract property tests in `transform.test.ts` |
| 9 | Configuration Oracle | `components/ui/ConfigurationOracle.svelte` | `components/editors/*` (extend with hints prop) | `store/graphStore.configOracle.integration.test.ts` |
| 10 | Audio and ML Schema Functions | `schema/schemaFunctions/audio.ts`, `schema/schemaFunctions/ml.ts` | None | `audio.test.ts`, `ml.test.ts` |
| 11 | Schema-Aware Pre-Run Guard | None | `store/graphStore.run.ts` | `store/graphStore.runGuard.integration.test.ts` |
| 12 | Component Internal Schema Propagation | None | `schema/schemaPropagator.ts`, `store/graphStore.persistence.ts` | `graphStore.component.schemaPlane.test.ts` |

---

## Success Criteria

1. `tsc --noEmit` error count never exceeds BASELINE at any commit.
2. All existing tests pass at every step — zero regressions.
3. Schema propagation for a 50-node graph completes in < 5ms.
4. Inline schema errors appear on edges **synchronously** with parameter changes.
5. Configuration oracle populates hints correctly for `TrainingJob`, `Join`, `Select`, and `Spectrogram` nodes.
6. A user can configure a complete audio ML pipeline (`Source → Spectrogram → Partition → Training → Evaluation`) with zero schema form filling, relying entirely on oracle hints.
7. All 15 structural validation integration tests pass.
8. All 20 regression tests pass.
