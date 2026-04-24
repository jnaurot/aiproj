# Join Relation Fixtures

## Fixture A: Two-table Join

- `LeftTable` -> `Join.in`
- `RightTable` -> `Join.in`
- clause:
	- `LeftTable.id = RightTable.id`

Expected:
- relation map has `LeftTable`, `RightTable`
- deterministic ordering by relation name
- schema/runtimes both resolve the same two relations

## Fixture B: Three-input Join Graph

- `A` -> `Join.in`
- `B` -> `Join.in`
- `C` -> `Join.in`
- clauses form a connected chain:
	- `A.id = B.id`
	- `B.id = C.id`

Expected:
- relation map includes all three names
- clause validation uses relation-qualified refs

## Fixture C: Component-qualified Names

- top-level: `Transform_A`
- component: `Comp1` contains `Transform_A`
- both connect to `Join.in`

Expected:
- relation names are globally unique:
	- `Transform_A`
	- `Comp1.Transform_A`
- no ambiguity in clause resolution
