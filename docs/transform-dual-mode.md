# Transform Dual Mode: Filter and Derive

Filter and Derive support two config modes:

- `rules` (default for new nodes)
- `sql` (advanced/manual; backward-compatible with legacy graphs)

## Filter

`filter.mode`

- `rules`: rule AST compiled to DuckDB SQL.
- `sql`: direct `filter.expr` WHERE fragment.

Rules operators (v1):

- `eq`, `ne`, `gt`, `gte`, `lt`, `lte`
- `contains`, `in`, `not_in`, `regex`
- `is_null`, `not_null`

Value sources:

- literal values
- `valueFrom` with `handle=param_config` and dot-path `path`

Group logic:

- nested groups with `op=all|any`

## Derive

`derive.mode`

- `rules`: formula DSL (`op + args`) compiled to DuckDB SQL.
- `sql`: direct SQL expressions in `derive.columns[].expr`.

Formula ops (v1):

- `add`, `sub`, `mul`, `div`
- `concat`
- `lower`, `upper`, `trim`, `length`
- `coalesce`

Argument sources:

- literal value
- `{ "column": "<name>" }`
- `{ "valueFrom": { "handle": "param_config", "path": "a.b.c" } }`

## Migration and Canonicalization

Legacy behavior is preserved:

- if `filter.expr` exists and `mode` is missing, effective mode is `sql`.
- if `derive.columns[].expr` exists and `mode` is missing, effective mode is `sql`.

Ambiguous payload policy:

- if both SQL and rules payloads exist and `mode` is missing, canonicalization resolves to `mode=sql` and emits warning notes:
  - `TRANSFORM_FILTER_MODE_AMBIGUOUS_RESOLVED`
  - `TRANSFORM_DERIVE_MODE_AMBIGUOUS_RESOLVED`

## Runtime diagnostics

Transform output metadata includes:

- `filter_compile`: `{ mode, whereSql, bindingsCount, paramPaths }`
- `derive_compile`: `{ mode, selectSql, bindingsCount, paramPaths }`

