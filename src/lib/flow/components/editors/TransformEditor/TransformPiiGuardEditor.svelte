<script lang="ts">
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import type { TransformPiiGuardParams } from '$lib/flow/schema/transform';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import ConditionalHint from './ConditionalHint.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	export let params: Partial<TransformPiiGuardParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformPiiGuardParams>) => void;
	export let onCommit: (patch: Partial<TransformPiiGuardParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	const nested = (params as any)?.pii_guard ?? params ?? {};
	$: columns = Array.isArray((nested as any)?.columns)
		? uniqueStrings((nested as any).columns.map((v: unknown) => String(v ?? '').trim()).filter(Boolean))
		: [];
	$: action = String((nested as any)?.action ?? 'report') as TransformPiiGuardParams['action'];
	$: failOnDetect = Boolean((nested as any)?.failOnDetect ?? false);
	$: schemaColumns = uniqueStrings(
		inputSchemaColumns.map((c) => String(c?.name ?? '').trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: fallbackColumns = uniqueStrings(inputColumns.map((c) => String(c ?? '').trim()).filter(Boolean)).sort(
		(a, b) => a.localeCompare(b)
	);
	$: columnOptions = schemaColumns.length > 0 ? schemaColumns : fallbackColumns;
	const meta = getTransformMeta('pii_guard');

	function commitPatch(next: Partial<TransformPiiGuardParams>, immediate = false): void {
		const merged = { columns, action, failOnDetect, ...next };
		onDraft({ op: 'pii_guard', pii_guard: merged } as unknown as Partial<TransformPiiGuardParams>);
		if (immediate) onCommit({ op: 'pii_guard', pii_guard: merged } as unknown as Partial<TransformPiiGuardParams>);
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />

	<Field label="columns">
		<ColumnTokenInput value={columns} schema={columnOptions} onChange={(next) => commitPatch({ columns: next })} placeholder="Add PII column" />
	</Field>

	<Field label="action">
		<select value={action} on:change={(e) => commitPatch({ action: (e.currentTarget as HTMLSelectElement).value as TransformPiiGuardParams['action'] }, true)}>
			<option value="report">report</option>
			<option value="mask">mask</option>
			<option value="drop_rows">drop_rows</option>
		</select>
	</Field>

	<Field label="fail on detect">
		<input type="checkbox" checked={failOnDetect} on:change={(e) => commitPatch({ failOnDetect: (e.currentTarget as HTMLInputElement).checked }, true)} />
	</Field>
</Section>
