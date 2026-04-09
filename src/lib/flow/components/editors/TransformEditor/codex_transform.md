Now I have everything I need. Here is the plan.

---

## Transform System Improvement Plan

### Current Situation

The system has 34 transform kinds with solid Zod schemas, good defaults, and mostly dedicated editor components. The problems are not in the schema layer — they're concentrated in three areas: **7 transforms with no real editor**, **UI cognitive load from raw inputs and implicit constraints**, and **missing discoverability infrastructure** (groups, descriptions, inline help).

---

## Step 1 — Give the 7 stub transforms real editors

**Problem:** `leakage_detect`, `quality_profile`, `drift_compare`, `determinism_profile`, `fit_state_registry`, `pii_guard`, `inference_parity` all share `TransformMlPreAdvancedEditor.svelte`, a single 253-line god component that uses runtime `op` branching with plain text inputs and no inline help.

**Plan:**

- Create 7 dedicated Svelte components, one per kind, following the same pattern as the existing small editors (`TransformCategoricalEncodeEditor.svelte` is a clean reference — 72 lines, typed props, structured fields)
- Each gets: a one-sentence description at the top of the section, properly typed field controls (checkboxes for booleans, `<select>` for enums, number inputs with `min`/`max` for bounded numbers, not raw text for everything)
- Delete `TransformMlPreAdvancedEditor.svelte` once all 7 are replaced
- Update `TransformEditor.ts` to point each kind to its dedicated component

**Files touched:** 7 new `.svelte` files, `TransformEditor.ts` (lines 60–66), delete `TransformMlPreAdvancedEditor.svelte`

---

## Step 2 — Replace free-text column inputs with token-chip inputs

**Problem:** At least 12 editors collect `columns[]` or `keyColumns[]` as comma-separated text in a plain `<Input>`. This pattern: (a) silently accepts malformed input, (b) gives no feedback when columns don't exist in upstream schema, (c) is awkward to edit (delete one column in the middle of a string).

**Plan:**

- Build a single reusable `ColumnTokenInput.svelte` component: accepts `value: string[]`, emits change events, renders each column as a removable chip, has a text input for adding new entries
- Optionally accepts a `schema: string[]` prop (column names from upstream node's inferred schema, available via `deriveNodeIoForData`) to offer autocomplete suggestions
- Replace the comma-text pattern in: `TransformMlPreAdvancedEditor` (all 5 column fields), `TransformNullPolicyEditor`, `TransformOutlierPolicyEditor`, `TransformTextCleanEditor`, `TransformNlpNormalizeEditor`, `TransformTokenizeChunkEditor`, `TransformEmbeddingEditor`, `TransformFeatureSelectionEditor`, `TransformCategoricalEncodeEditor`, `TransformNumericScaleEditor`, `TransformPiiGuardEditor` (new), `TransformQualityProfileEditor` (new)

**Files touched:** new `ColumnTokenInput.svelte`, ~12 editor components updated

---

## Step 3 — Surface cross-field constraints inline

**Problem:** Several transforms have params that are meaningless or invalid unless other params have specific values — but the UI shows all fields all the time with no visual connection. Examples:

- `numeric_scale`: `clipMin`/`clipMax` only matter when `clip: true`
- `dataset_split`: `stratifyColumn` only matters when `strategy: 'stratified'`; `groupColumn` for `'group'`; `timeColumn` for `'time'`
- `tokenize_chunk`: `tokenPattern` only matters when `tokenizer: 'regex'`
- `split`: `pattern`/`flags` only for `mode: 'regex'`; `delimiter` only for `mode: 'delimiter'`
- `null_policy`: `fillValue` only for `mode: 'fill_constant'`; `stat` only for `mode: 'fill_stat'`
- `outlier_policy`: `iqrMultiplier` only for `method: 'iqr'`; `zscoreThreshold` only for `method: 'zscore'`; quantile fields only for `method: 'quantile'`

**Plan:**

- Use conditional rendering (`{#if ...}`) already pattern-matched in some editors, but apply it consistently to **hide** irrelevant fields rather than show them greyed-out
- Add a small `<ConditionalHint>` inline message wherever a key param selection changes what's needed (e.g., directly below the `strategy` dropdown in `dataset_split`: "Requires a stratify column below")
- Audit each of the 8 affected editors, add condition guards to every dependent field

**Files touched:** `TransformNumericScaleEditor`, `TransformDatasetSplitEditor`, `TransformTokenizeChunkEditor`, `TransformSplitEditor`, `TransformNullPolicyEditor`, `TransformOutlierPolicyEditor` (6 editors, targeted changes)

---

## Step 4 — Add bounded number inputs for all numeric params

**Problem:** Many numeric params use `<Input type="text">` with `toNumber()` conversion on blur. There is no visual constraint feedback. Examples: `limit.n` (min 1), `embedding.dimensions` (1–4096), `tokenize_chunk.overlap` (must be < `maxTokens`), `dataset_split` ratios (each 0–1, sum to 1).

**Plan:**

- All scalar numeric params get `<Input type="number" min={...} max={...} step={...}>` with the appropriate bounds from the schema (derivable directly from the Zod schema: `z.number().min(1)` → `min="1"`)
- For ratio fields that must sum to 1.0 (`dataset_split.trainRatio`, `valRatio`, `testRatio`): add a live running total display beneath the three inputs (e.g., "total: 1.0 ✓" or "total: 0.95 ✗") — recomputed reactively, no server round-trip needed
- For `overlap < maxTokens` constraint in `tokenize_chunk`: show inline warning when violated

**Files touched:** `TransformLimitEditor`, `TransformEmbeddingEditor`, `TransformTokenizeChunkEditor`, `TransformDatasetSplitEditor`, `TransformOutlierPolicyEditor`, `TransformLeakageDetectEditor` (new)

---

## Step 5 — Add a transform kind description map and display it in the editor

**Problem:** When a user opens a transform node editor, there's no indication of what the transform does. The section title in `TransformMlPreAdvancedEditor` literally says "ML-Pre Advanced: leakage_detect" — meaningless to a non-expert.

**Plan:**

- Create `src/lib/flow/schema/transformMeta.ts`: a `Record<TransformKind, { label: string; description: string; category: string }>` map
  - Categories: `'reshape'` (select, rename, sort, limit, dedupe, join), `'compute'` (filter, derive, aggregate, sql), `'text'` (text_clean, nlp_normalize, tokenize_chunk, split), `'convert'` (json_to_table, text_to_table, table_to_json, json_filter), `'ml_prep'` (dataset_split, class_imbalance, categorical_encode, numeric_scale, embedding, feature_selection), `'quality'` (null_policy, outlier_policy, quality_profile, quality_gate, drift_compare, leakage_detect, pii_guard, determinism_profile, ml_contract, fit_state_registry, inference_parity)
  - Short description for each: plain English, one sentence, no jargon (e.g., `filter`: "Keep only rows that match the specified conditions")
- Each editor's `<Section>` title becomes the `label` from this map, and a subtitle or tooltip shows the `description`
- The node picker (wherever transforms are added to the canvas) groups by category and shows the description on hover

**Files touched:** new `transformMeta.ts`, each editor (update `<Section title={...}>`), node-picker component (wherever transforms are added)

---

## Step 6 — Simplify `derive` and `filter` dual-mode UI

**Problem:** `filter` and `derive` each expose two entirely different UIs (rules builder vs. SQL) under a mode toggle. The rules builder is powerful but has high cognitive overhead. The SQL mode is a raw text field. Users end up in one mode, get confused, switch to the other, lose their work from the first mode.

**Plan:**

- Make the mode toggle more prominent: a segmented button (`Rules | SQL`) rather than a dropdown, so it's visually obvious that they are alternatives
- Add a warning banner when switching modes: "Switching to SQL will not carry over your current rules. Continue?" (with a cancel option) — state from the other mode is preserved in params but hidden
- For the SQL editor: add basic snippet buttons for the most common patterns ("WHERE condition", "SELECT CASE WHEN", "GROUP BY") to reduce blank-page friction
- For the rules builder: move the `Add Condition` button above the existing conditions rather than only below (reduces scrolling for large rule trees)

**Files touched:** `TransformFilterEditor.svelte`, `TransformDeriveEditor.svelte`, `FilterRulesBuilder.svelte`, `TransformSqlEditor.svelte`

---

## Step 7 — Add node summary improvements for 5 low-information transforms

**Problem:** `TransformNode.svelte` (lines 9–71) generates good summaries for most transforms but several show nearly nothing:

- `sql`: shows nothing useful — just "sql"
- `drift_compare`: shows nothing — no baseline ref, no metric
- `quality_gate`: shows "0 checks" when empty even after configuration
- `rename`: shows "rename" with no column count
- `null_policy`: shows the mode but not the column scope

**Plan:**

- In `TransformNode.svelte`, add summary cases for each:
  - `sql`: first line of the query (trimmed to 40 chars) — `"SELECT * FROM input…"`
  - `drift_compare`: `"${metric} vs ${baselineRef || 'baseline'}, threshold=${threshold}"`
  - `quality_gate`: `"${checks.length} check${checks.length !== 1 ? 's' : ''}"` plus first check kind
  - `rename`: `"${Object.keys(map).length} column${...} renamed"`
  - `null_policy`: `"${mode}${columns.length ? ` (${columns.length} col)` : ' (all)'}"`

**Files touched:** `TransformNode.svelte` (lines 9–71 — extend existing switch/if blocks)

---

## Step 8 — Consolidate `quality_gate` check builder into sub-components

**Problem:** `TransformQualityGateEditor.svelte` is 14KB handling 5 discriminated check types in a single file. Each check type has completely different fields. The file is hard to read, hard to extend, and re-renders the entire editor on any change.

**Plan:**

- Extract 5 sub-components: `QualityCheckNullPct.svelte`, `QualityCheckRange.svelte`, `QualityCheckUniqueness.svelte`, `QualityCheckClassBalance.svelte`, `QualityCheckLeakage.svelte`
- Each accepts `check: <CheckType>` and emits `onChange(updatedCheck)`
- `TransformQualityGateEditor.svelte` becomes a thin coordinator: renders the check list, dispatches to the right sub-component per check kind, handles add/remove
- Result: each sub-component is ~50–80 lines, the coordinator is ~100 lines

**Files touched:** `TransformQualityGateEditor.svelte` (refactor), 5 new sub-component files

---

## Implementation Order

| Step | What                                      | Effort               | Payoff                                               |
| ---- | ----------------------------------------- | -------------------- | ---------------------------------------------------- |
| 5    | Add `transformMeta.ts` description map    | Low                  | Unblocks other steps; immediate discoverability gain |
| 7    | Fix node summaries                        | Low                  | Immediately visible on canvas                        |
| 3    | Inline conditional field hiding           | Medium               | Directly reduces cognitive load per-editor           |
| 4    | Bounded number inputs + ratio sum display | Medium               | Prevents silent invalid configs                      |
| 1    | 7 dedicated editors replacing the stub    | Medium               | Removes the most embarrassing UX gap                 |
| 2    | `ColumnTokenInput` shared component       | Medium               | Shared infrastructure; do after editors are stable   |
| 6    | Dual-mode UX improvements                 | Medium               | Polish on the two most-used complex editors          |
| 8    | `quality_gate` sub-component split        | Low effort, low risk | Code quality; easier future check additions          |

Steps 5 and 7 are independent of everything else — start there. Steps 1, 3, and 4 are the highest cognitive-load fixes. Step 2 (shared `ColumnTokenInput`) is infrastructure that pays off when done after the editors in Step 1 are written (so it gets used in new editors from the start). Steps 6 and 8 are polish.
