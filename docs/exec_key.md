# Exec Key Determinism Contract

`exec_key` is the cache identity for a specific node execution.

## Definition

`exec_key = sha256(canonical_json(key_material))`

Canonical JSON rules:

- UTF-8 encoding
- object keys sorted
- separators `(",", ":")`
- stable ordering for input refs (`inputHandle`, then `artifactId`)

## Required Key Material

`exec_key` includes:

- `build_version` (`execution_version`)
- `graph_id`
- `node_id`
- `node_kind`
- `node_impl_version`
- `node_state_hash` (canonical node params + canonical cache-relevant schema view)
- `upstreamArtifactKeys` (sorted IDs)
- `inputBindings` (stable handle->artifact pairs)
- `determinism_env` (normalized, non-secret, sorted)

## Node State Hash Contract

`node_state_hash` is `sha256(canonical_json(state))` and includes:

- execution version
- node kind
- normalized/sanitized params
- canonical cache schema view
- source fingerprint for source nodes

Canonical cache schema view excludes runtime-observed drift channels by default:

- excludes `observedSchema`
- excludes volatile metadata fields (`updatedAt`, `state`, `source`, `schemaFingerprint`)

This prevents rerun cache churn from runtime observation timestamps/state while still invalidating on authored/inferred schema changes.

## Explicit Exclusions

These must not change `exec_key`:

- runtime artifact metadata
- output payload metadata (`mimeType`, payload schema envelope)
- run IDs, timestamps, transient status
- UI/editor-only draft state

## Cache Miss Reason Contract

When a cache entry is missing for the new key, miss-reason classification compares latest prior artifact metadata:

- `BUILD_CHANGED`: code hash or node impl version changed
- `PARAMS_CHANGED`: `paramsFingerprint` changed
- `INPUT_CHANGED`: upstream artifact IDs changed
- `ENV_CHANGED`: determinism/profile lock changed
- `CACHE_ENTRY_MISSING`: insufficient comparable metadata or no prior entry

Legacy artifacts without `paramsFingerprint` do not emit false `PARAMS_CHANGED`; they degrade to `CACHE_ENTRY_MISSING`.

## Debugging Unexpected Misses

Set `CACHE_KEY_DEBUG=1` to emit structured cache-key diff logs.

- Output category: `[cache-key-diff]`
- Includes bounded fingerprints/digests only (no raw artifact IDs or secrets)
- Shows previous vs current summary for params, upstream set, determinism, build/version signals
