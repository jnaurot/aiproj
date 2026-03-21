<script lang="ts">
	import type { TransformNumericScaleParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let params: Partial<TransformNumericScaleParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformNumericScaleParams>) => void;
	export let onCommit: (patch: Partial<TransformNumericScaleParams>) => void;

	const nested = (params as any)?.numeric_scale ?? params ?? {};
	$: method = String((nested as any)?.method ?? 'standard');
	$: clip = Boolean((nested as any)?.clip ?? false);
	$: columnsText = Array.isArray((nested as any)?.columns) ? (nested as any).columns.join(', ') : '';
	$: clipMin = (nested as any)?.clipMin;
	$: clipMax = (nested as any)?.clipMax;

	function parseColumns(raw: string): string[] {
		return raw.split(',').map((v) => v.trim()).filter((v) => v.length > 0);
	}

	function commitPatch(next: Partial<TransformNumericScaleParams>) {
		const merged = { method, clip, columns: parseColumns(columnsText), clipMin, clipMax, ...next };
		onDraft({ op: 'numeric_scale', numeric_scale: merged } as unknown as Partial<TransformNumericScaleParams>);
		onCommit({ op: 'numeric_scale', numeric_scale: merged } as unknown as Partial<TransformNumericScaleParams>);
	}
</script>

<Section title="Numeric Scale">
	<Field label="columns (comma-separated)">
		<Input value={columnsText} onInput={(e) => commitPatch({ columns: parseColumns((e.currentTarget as HTMLInputElement).value) })} />
	</Field>
	<Field label="method">
		<select value={method} on:change={(e) => commitPatch({ method: (e.currentTarget as HTMLSelectElement).value as any })}>
			<option value="standard">standard</option>
			<option value="minmax">minmax</option>
			<option value="robust">robust</option>
		</select>
	</Field>
	<Field label="clip">
		<input type="checkbox" checked={clip} on:change={(e) => commitPatch({ clip: (e.currentTarget as HTMLInputElement).checked })} />
	</Field>
	{#if clip}
		<Field label="clip min">
			<Input value={clipMin === undefined ? '' : String(clipMin)} onInput={(e) => commitPatch({ clipMin: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
		</Field>
		<Field label="clip max">
			<Input value={clipMax === undefined ? '' : String(clipMax)} onInput={(e) => commitPatch({ clipMax: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
		</Field>
	{/if}
</Section>
