# CSS Ownership

## Theme Contract

1. `src/lib/flow/styles/tokens.css` defines semantic variables only.
2. `src/lib/flow/styles/themes.css` defines concrete palette values (light/dark).
3. Components must reference semantic variables; avoid concrete color literals outside `themes.css`.
4. `prefers-color-scheme` is fallback only; explicit `data-theme` takes precedence.

## Scope Rules

1. `FlowCanvas.svelte` owns canvas chrome, layout, and edge visuals only.
2. `Field.svelte` is the single layout authority for `.field`, `.k`, `.v` (including `stacked` behavior).
3. Inspector form skin rules live in `src/lib/flow/styles/inspectorForm.css`.
4. `inspectorForm.css` may set skin-only properties for controls:
   typography, colors, borders, radius, focus, box-sizing.
5. `inspectorForm.css` must not set `.field/.k/.v` display/layout/positioning.
6. Editor-specific CSS stays in each editor component and must be namespaced by an editor root class.
7. Global overrides are allowed only when namespaced (for example `.inspector`, `.portsTheme`).

## Primitive Contracts

1. `.k` is label cell, `.v` is value cell.
2. `.v` must maintain `min-width: 0`.
3. Child controls in `.v` must not exceed container width unless explicitly scrollable.
4. Compound editors requiring alternative layout must opt into `stacked` or an explicit namespaced contract.

## Svelte Scoping Rules

1. Primitive components: scoped styles, minimal `:global`.
2. Shell components: can use namespaced `:global` within a root namespace.
3. Editors: no global selectors except namespaced under that editor root.
4. One-off global fixes are forbidden unless time-boxed and tracked.

## Notes

- Avoid introducing `.sourceFileEditor` styling outside `SourceFileEditor.svelte` (or shared editor style files intentionally imported there).
- Keep selector reach stable when moving CSS between files; move-only refactors should not broaden selector scope.
- `FlowCanvas.svelte` may keep overflow safety selectors such as `.editorScroll :global(input/select/textarea)` that do not alter `.field/.k/.v` layout.
