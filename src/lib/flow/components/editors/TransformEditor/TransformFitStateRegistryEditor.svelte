<script lang="ts">
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import type { TransformFitStateRegistryParams } from '$lib/flow/schema/transform';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	export let params: Partial<TransformFitStateRegistryParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformFitStateRegistryParams>) => void;
	export let onCommit: (patch: Partial<TransformFitStateRegistryParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	const nested = (params as any)?.fit_state_registry ?? params ?? {};
	$: mode = String((nested as any)?.mode ?? 'fit') as TransformFitStateRegistryParams['mode'];
	$: stateKey = String((nested as any)?.stateKey ?? 'default');
	$: includeColumns = Array.isArray((nested as any)?.includeColumns)
		? uniqueStrings((nested as any).includeColumns.map((v: unknown) => String(v ?? '').trim()).filter(Boolean))
		: [];
	$: schemaColumns = uniqueStrings(
		inputSchemaColumns.map((c) => String(c?.name ?? '').trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: fallbackColumns = uniqueStrings(inputColumns.map((c) => String(c ?? '').trim()).filter(Boolean)).sort(
		(a, b) => a.localeCompare(b)
	);
	$: columnOptions = schemaColumns.length > 0 ? schemaColumns : fallbackColumns;
	const meta = getTransformMeta('fit_state_registry');

	function commitPatch(next: Partial<TransformFitStateRegistryParams>, immediate = false): void {
		const merged = { mode, stateKey, includeColumns, ...next };
		onDraft({ op: 'fit_state_registry', fit_state_registry: merged } as unknown as Partial<TransformFitStateRegistryParams>);
		if (immediate) onCommit({ op: 'fit_state_registry', fit_state_registry: merged } as unknown as Partial<TransformFitStateRegistryParams>);
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />

	<Field label="mode">
		<select value={mode} on:change={(e) => commitPatch({ mode: (e.currentTarget as HTMLSelectElement).value as TransformFitStateRegistryParams['mode'] }, true)}>
			<option value="fit">fit</option>
			<option value="apply">apply</option>
		</select>
	</Field>

	<Field label="state key">
		<Input value={stateKey} onInput={(e) => commitPatch({ stateKey: (e.currentTarget as HTMLInputElement).value })} />
	</Field>

	<Field label="include columns">
		<ColumnTokenInput value={includeColumns} schema={columnOptions} onChange={(next) => commitPatch({ includeColumns: next })} placeholder="Add include column" />
	</Field>
</Section>
