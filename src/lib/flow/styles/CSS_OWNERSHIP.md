# CSS Ownership

## Scope Rules

1. `FlowCanvas.svelte` owns canvas chrome, layout, and edge visuals only.
2. Inspector form layout rules live in `src/lib/flow/styles/inspectorForm.css`.
3. Editor-specific CSS stays in each editor component and must be namespaced by editor container class.
4. Global overrides are allowed only when namespaced (for example `.inspector`, `.portsTheme`).
5. `PortsEditor.svelte` is an intentional local theme layer and may override `.k/.v` within `.portsTheme`.

## Notes

- Avoid introducing `.sourceFileEditor` styling outside `SourceFileEditor.svelte` (or shared editor style files intentionally imported there).
- Keep selector reach stable when moving CSS between files; move-only refactors should not broaden selector scope.
