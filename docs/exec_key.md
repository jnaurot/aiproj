# Exec Key Specification

`exec_key` is the deterministic cache identity for a node execution.

## Definition

`exec_key = sha256_hex(canonical_json(exec_fingerprint))`

Where `canonical_json` uses:

- UTF-8 JSON
- sorted object keys
- separators `(",", ":")` (no extra whitespace)
- deterministic list ordering by caller contract

## Inputs By Node Kind

`exec_key` MUST depend only on determinism inputs:

- `node_kind`
- canonical/normalized params
- ordered input artifact identity:
  - generic path: sorted `input_artifact_ids`
  - transform/tool path: stable `port -> artifact_id` ordering via `input_refs`
- `build_version` / `execution_version`

Optional (only if intentionally desired): engine/runtime version for explicit cache busting.

## Source / LLM / Generic

Fingerprint object:

```json
{
  "build_version": "<execution_version>",
  "node_kind": "<kind>",
  "normalized_params": { "...": "..." },
  "input_artifact_ids": ["...sorted..."]
}
```

## Transform

Fingerprint object:

```json
{
  "build_version": "<execution_version>",
  "node_kind": "transform",
  "normalized_params": { "...": "..." },
  "input_artifact_ids": ["...from sorted input refs..."]
}
```

## Tool

Tool uses a dedicated fingerprint:

```json
{
  "build": "<execution_version>",
  "kind": "tool",
  "tool_name": "...",
  "tool_version": "...",
  "side_effect_mode": "pure|idempotent|effectful",
  "params": { "...sanitized..." },
  "inputs": "<stable input fingerprint>",
  "environment": { "...non-secret environment fingerprint..." }
}
```

## Normalization Rules

- Params are normalized before hashing.
- Secret-like keys are excluded for tool fingerprints.
- Input artifact IDs are sorted before hashing.
- `port_type`, `mime_type`, and `payload_schema` are excluded from `exec_key`.
- `run_id`, timestamps, node status, and UI-only inspector state are excluded.
- Canonical JSON rules:
  - object keys sorted
  - separators `(",", ":")`
  - no whitespace
- Transform op payload ordering is hashed as-is after normalization; semantically equivalent reorders are treated as distinct keys.

## Exclusions (Must Not Affect `exec_key`)

- Output-derived fields (`payload_schema`, `mime_type`, artifact metadata)
- Runtime timestamps, run IDs
- UI-only draft/editor state

## Golden Test Vectors

- source_v1: `29be5bf9738874feb8c1b002543cdee3b7539941e8bd8990da22301e3478b7cd`
- llm_v1: `78dbb1e58fe8dd996bbf005b06ab6c8409b04d2aa93408012109307dd1bef5e4`
- tool_v1: `a6e5cd6111d004b1da44cd3554fba4b46c805d82eda525b28b44cb815796a3ae`
- transform_v1: `9ad49fe5637b750ebc82477a1ed648eff7cf6e369c519771929e5e559e2b7fdd`

Behavioral invariants (covered by tests):

- same params + same inputs => same key
- same params with key reordering => same key
- input-to-port swap (transform/tool) => different key
- build version change => different key
- join `withNodeId` change => different key
- transform op list reorder => different key (current policy)
