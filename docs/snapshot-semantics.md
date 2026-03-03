# Snapshot Semantics For Source File Nodes

## Canonical decisions

- A dropped file becomes an immutable snapshot artifact.
- Snapshot identity is `contentHash = sha256(file_bytes)`.
- Snapshot artifact ID equals `contentHash` (content-addressed).
- Snapshot metadata (`importedAt`, `originalFilename`, `byteSize`, `mimeType`) is not part of identity.
- Source file nodes reference snapshots through `snapshotId`.
- Source execution still produces exactly one immutable output artifact per execution (`artifact_id == exec_key`).

## Determinism and exec_key inputs

For `sourceKind=file` snapshot-backed nodes, deterministic inputs are:

- `snapshotId`
- parse params (`file_format`, `delimiter`, `encoding`, `sheet_name`)
- output contract (`output_mode` / port contract)

The following are explicitly excluded from cache identity / exec inputs:

- `importedAt`
- `originalFilename`
- UI history fields (for example `recentSnapshotIds`, `snapshotMetadata`)

## Product terminology

- "Previous uploads" means previously uploaded snapshots.
- Selecting a previous upload reuses saved bytes from the artifact store.
- It does not read from current disk state for that filename/path.
