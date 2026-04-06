# GC Spec (Mark-and-Sweep)

This document defines global artifact blob garbage collection semantics.

## Scope

GC operates on blob files (`*.bin`) in the artifact blob store and the `artifacts` metadata table.

- Blob identity: `content_hash` (`sha256` hex).
- Blob storage key: `blobs/sha256/<h0h1>/<h2h3>/<fullhash>.bin`.
- Metadata reference source: `artifacts.content_hash`.

## Invariant

Referenced blobs are never deleted.

A blob is considered **referenced** iff its hash appears in:

```sql
SELECT DISTINCT content_hash FROM artifacts WHERE content_hash IS NOT NULL
```

## Algorithm

`gc_orphan_blobs(mode, limit, max_seconds)`:

1. Build `referenced` set from metadata (`artifacts.content_hash`).
2. Walk blob directory and extract blob hash from filename (`<hash>.bin`).
3. Mark as orphan when hash is not in `referenced`.
4. Optional bounds:
   - stop when `limit` orphan hashes collected
   - stop when `max_seconds` elapsed
5. If `mode=delete`, delete orphan blob files; if `mode=dry_run`, delete nothing.

## Modes

- `dry_run`: report-only, no file deletion.
- `delete`: delete orphan blobs found by the scan.

## Dry-Run Report Format

Maintenance API (`POST /maintenance/gc?mode=dry_run`) returns:

```json
{
  "mode": "dry_run",
  "referenced_count": 12,
  "orphan_count": 3,
  "blobs_deleted": 0,
  "scanned_blobs": 27,
  "orphan_samples": ["<hash1>", "<hash2>"]
}
```

When `verbose=true`, the response also includes full `orphan_hashes`.

## Safety Notes

- Hard run deletion with `gc=unreferenced` is candidate-based cleanup.
- Global mark-and-sweep is maintenance cleanup for crash leftovers/orphans.
- Deletion failures are non-fatal and can be retried by later GC runs.
