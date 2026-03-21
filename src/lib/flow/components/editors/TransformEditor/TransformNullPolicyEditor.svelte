<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformNullPolicyParams } from '$lib/flow/schema/transform';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	type NullPolicyMode = TransformNullPolicyParams['mode'];

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformNullPolicyParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformNullPolicyParams>) => void;
	export let onCommit: (patch: Partial<TransformNullPolicyParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	$: void selectedNode?.id;
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
			onCommit({ op: 'null_policy', null_policy: merged } as unknown as Partial<TransformNullPolicyParams>);
			return;
		}
		onDraft(merged);
		onCommit(merged);
	}

	function addColumn(col: string): void {
		const value = String(col ?? '').trim();
		if (!value) return;
		commitPatch({ columns: uniqueStrings([...columns, value]) });
	}

	function removeColumn(col: string): void {
		const value = String(col ?? '').trim();
		commitPatch({ columns: columns.filter((c) => c !== value) });
	}
</script>

<Section title="Null Policy">
	<div class="hint">Report, drop, or fill nulls with deterministic policy controls.</div>

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
		<div class="colControls">
			<select on:change={(event) => addColumn((event.currentTarget as HTMLSelectElement).value)}>
				<option value="">Select column...</option>
				{#each columnOptions as col (col)}
					<option value={col}>{col}</option>
				{/each}
			</select>
		</div>
		{#if columns.length > 0}
			<div class="chips">
				{#each columns as col (col)}
					<button type="button" class="chip" on:click={() => removeColumn(col)}>{col} ×</button>
				{/each}
			</div>
		{/if}
	</Field>

	{#if mode === 'fill_constant'}
		<Field label="fill value">
			<Input
				value={String(fillValue ?? '')}
				placeholder="0"
				onInput={(event) => onDraft({ fillValue: (event.currentTarget as HTMLInputElement).value })}
				onBlur={() => commitPatch({ fillValue })}
			/>
		</Field>
	{/if}

	{#if mode === 'fill_stat'}
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

<style>
	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}

	.colControls {
		display: grid;
		gap: 8px;
	}

	.chips {
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
		margin-top: 8px;
	}

	.chip {
		border: 1px solid rgba(148, 163, 184, 0.4);
		background: rgba(15, 23, 42, 0.45);
		color: #e2e8f0;
		border-radius: 999px;
		padding: 2px 10px;
		font-size: 12px;
		cursor: pointer;
	}
</style>
