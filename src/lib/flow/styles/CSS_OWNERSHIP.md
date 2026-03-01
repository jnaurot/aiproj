# CSS Ownership

## Scope Rules

1. `FlowCanvas.svelte` owns canvas chrome, layout, and edge visuals only.
2. `Field.svelte` is the single layout authority for `.field`, `.k`, `.v` (including `stacked` behavior).
3. Inspector form skin rules live in `src/lib/flow/styles/inspectorForm.css` and must not set `.field/.k/.v` layout.
4. Editor-specific CSS stays in each editor component and must be namespaced by editor container class.
5. Global overrides are allowed only when namespaced (for example `.inspector`, `.portsTheme`).
6. `PortsEditor.svelte` is an intentional local theme layer and may override `.k/.v` within `.portsTheme`.

## Notes

- Avoid introducing `.sourceFileEditor` styling outside `SourceFileEditor.svelte` (or shared editor style files intentionally imported there).
- Keep selector reach stable when moving CSS between files; move-only refactors should not broaden selector scope.
- `FlowCanvas.svelte` may keep overflow safety selectors such as `.editorScroll :global(input/select/textarea)` that do not alter `.field/.k/.v` layout.
