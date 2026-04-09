<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformNullPolicyParams } from '$lib/flow/schema/transform';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	type NullPolicyMode = TransformNullPolicyParams['mode'];

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformNullPolicyParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformNullPolicyParams>) => void;
	export let onCommit: (patch: Partial<TransformNullPolicyParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	$: void selectedNode?.id;
	$: void onCommit;
	$: nested = readNested(params);
	$: mode = (nested.mode ?? 'report') as NullPolicyMode;
	$: columns = normalizeColumns(nested.columns);
	$: fillValue = nested.fillValue ?? '';
	$: stat = (nested.stat ?? 'mean') as TransformNullPolicyParams['stat'];
	$: schemaColumns = uniqueStrings(
		inputSchemaColumns.map((c) => String(c?.name ?? '').trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: fallbackColumns = uniqueStrings(inputColumns.map((c) => String(c ?? '').trim()).filter(Boolean)).sort(
		(a, b) => a.localeCompare(b)
	);
	$: columnOptions = schemaColumns.length > 0 ? schemaColumns : fallbackColumns;
	const meta = getTransformMeta('null_policy');

	function isObject(v: unknown): v is Record<string, unknown> {
		return Boolean(v) && typeof v === 'object' && !Array.isArray(v);
	}

	function readNested(raw: unknown): Partial<TransformNullPolicyParams> {
		if (!isObject(raw)) return {};
		if (isObject(raw.null_policy)) return raw.null_policy as Partial<TransformNullPolicyParams>;
		return raw as Partial<TransformNullPolicyParams>;
	}

	function isWrapped(raw: unknown): boolean {
		return isObject(raw) && ('op' in raw || 'null_policy' in raw);
	}

	function normalizeColumns(raw: unknown): string[] {
		if (!Array.isArray(raw)) return [];
		return uniqueStrings(raw.map((v) => String(v ?? '').trim()).filter((v) => v.length > 0));
	}

	function commitPatch(next: Partial<TransformNullPolicyParams>): void {
		const merged = {
			mode,
			columns,
			fillValue,
			stat,
			rules: Array.isArray(nested.rules) ? nested.rules : [],
			...next
		};
		if (isWrapped(params)) {
			onDraft({ op: 'null_policy', null_policy: merged } as unknown as Partial<TransformNullPolicyParams>);
			return;
		}
		onDraft(merged);
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />

	<Field label="mode">
		<select
			value={mode}
			on:change={(event) =>
				commitPatch({ mode: (event.currentTarget as HTMLSelectElement).value as NullPolicyMode })}
		>
			<option value="report">report</option>
			<option value="drop_rows">drop_rows</option>
			<option value="fill_constant">fill_constant</option>
			<option value="fill_stat">fill_stat</option>
		</select>
	</Field>

	<Field label="columns">
		<ColumnTokenInput value={columns} schema={columnOptions} onChange={(next) => commitPatch({ columns: next })} placeholder="Add nullable column" />
	</Field>

	{#if mode === 'fill_constant'}
		<ConditionalHint text="fill_constant mode requires a fill value." />
		<Field label="fill value">
			<Input
				value={String(fillValue ?? '')}
				placeholder="0"
				onInput={(event) => commitPatch({ fillValue: (event.currentTarget as HTMLInputElement).value })}
			/>
		</Field>
	{/if}

	{#if mode === 'fill_stat'}
		<ConditionalHint text="fill_stat mode requires a statistic." />
		<Field label="stat">
			<select
				value={stat}
				on:change={(event) =>
					commitPatch({ stat: (event.currentTarget as HTMLSelectElement).value as TransformNullPolicyParams['stat'] })}
			>
				<option value="mean">mean</option>
				<option value="median">median</option>
				<option value="mode">mode</option>
			</select>
		</Field>
	{/if}
</Section>
