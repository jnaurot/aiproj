# Schema Diagnostics Contract

## Join Relation Authority

- Join relations are identified by canonical relation names, not transient runtime node IDs.
- Canonical relation name format:
	- top-level node: `<nodeName>`
	- component-internal node: `<componentName>.<nodeName>`
- Join clause column refs use `<relationName>.<columnName>`.

## Authority Boundaries

- Schema plane is authoritative for schema compatibility severity.
- Runtime is authoritative for execution success/failure.
- Runtime diagnostics for join clause resolution should mirror schema-plane payload shape where feasible:
	- `errorCode`
	- `paramPath`
	- `missingColumns`
	- `availableColumns`

## Opaque Handling

- Opaque upstream contracts are uncertainty signals.
- Missing-field checks against opaque upstream are warnings (or lower) unless strict policy explicitly elevates.

## Join Validation Invariants

- Multi-`in` join nodes consume all incoming work edges as relation candidates.
- Relations are deterministically ordered by canonical relation name, tie-break edge ID.
- Non-join same-handle compatibility rules remain unchanged.
