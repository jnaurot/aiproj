<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformOutlierPolicyParams } from '$lib/flow/schema/transform';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	type OutlierMode = TransformOutlierPolicyParams['mode'];
	type OutlierMethod = TransformOutlierPolicyParams['method'];

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformOutlierPolicyParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformOutlierPolicyParams>) => void;
	export let onCommit: (patch: Partial<TransformOutlierPolicyParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	$: void selectedNode?.id;
	$: nested = readNested(params);
	$: mode = (nested.mode ?? 'clip') as OutlierMode;
	$: method = (nested.method ?? 'iqr') as OutlierMethod;
	$: columns = normalizeColumns(nested.columns);
	$: iqrMultiplier = Number(nested.iqrMultiplier ?? 1.5);
	$: zscoreThreshold = Number(nested.zscoreThreshold ?? 3);
	$: lowerQuantile = Number(nested.lowerQuantile ?? 0.01);
	$: upperQuantile = Number(nested.upperQuantile ?? 0.99);
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

	function readNested(raw: unknown): Partial<TransformOutlierPolicyParams> {
		if (!isObject(raw)) return {};
		if (isObject(raw.outlier_policy)) return raw.outlier_policy as Partial<TransformOutlierPolicyParams>;
		return raw as Partial<TransformOutlierPolicyParams>;
	}

	function isWrapped(raw: unknown): boolean {
		return isObject(raw) && ('op' in raw || 'outlier_policy' in raw);
	}

	function normalizeColumns(raw: unknown): string[] {
		if (!Array.isArray(raw)) return [];
		return uniqueStrings(raw.map((v) => String(v ?? '').trim()).filter((v) => v.length > 0));
	}

	function commitPatch(next: Partial<TransformOutlierPolicyParams>): void {
		const merged = {
			mode,
			method,
			columns,
			iqrMultiplier,
			zscoreThreshold,
			lowerQuantile,
			upperQuantile,
			...next
		};
		if (isWrapped(params)) {
			onDraft({ op: 'outlier_policy', outlier_policy: merged } as unknown as Partial<TransformOutlierPolicyParams>);
			onCommit({ op: 'outlier_policy', outlier_policy: merged } as unknown as Partial<TransformOutlierPolicyParams>);
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

<Section title="Outlier Policy">
	<div class="hint">Clip, winsorize, or drop outliers with explicit threshold controls.</div>

	<Field label="mode">
		<select
			value={mode}
			on:change={(event) =>
				commitPatch({ mode: (event.currentTarget as HTMLSelectElement).value as OutlierMode })}
		>
			<option value="clip">clip</option>
			<option value="winsorize">winsorize</option>
			<option value="drop">drop</option>
		</select>
	</Field>

	<Field label="method">
		<select
			value={method}
			on:change={(event) =>
				commitPatch({ method: (event.currentTarget as HTMLSelectElement).value as OutlierMethod })}
		>
			<option value="iqr">iqr</option>
			<option value="zscore">zscore</option>
			<option value="quantile">quantile</option>
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

	{#if method === 'iqr'}
		<Field label="iqr multiplier">
			<Input
				type="number"
				min="0.1"
				step="0.1"
				value={iqrMultiplier}
				onInput={(event) =>
					onDraft({ iqrMultiplier: Number((event.currentTarget as HTMLInputElement).value || 1.5) })}
				onBlur={() => commitPatch({ iqrMultiplier: Math.max(0.1, Number(iqrMultiplier) || 1.5) })}
			/>
		</Field>
	{/if}

	{#if method === 'zscore'}
		<Field label="zscore threshold">
			<Input
				type="number"
				min="0.1"
				step="0.1"
				value={zscoreThreshold}
				onInput={(event) =>
					onDraft({ zscoreThreshold: Number((event.currentTarget as HTMLInputElement).value || 3) })}
				onBlur={() => commitPatch({ zscoreThreshold: Math.max(0.1, Number(zscoreThreshold) || 3) })}
			/>
		</Field>
	{/if}

	{#if method === 'quantile'}
		<Field label="lower quantile">
			<Input
				type="number"
				min="0"
				max="1"
				step="0.01"
				value={lowerQuantile}
				onInput={(event) =>
					onDraft({ lowerQuantile: Number((event.currentTarget as HTMLInputElement).value || 0.01) })}
				onBlur={() => commitPatch({ lowerQuantile: Math.min(0.99, Math.max(0, Number(lowerQuantile) || 0.01)) })}
			/>
		</Field>
		<Field label="upper quantile">
			<Input
				type="number"
				min="0"
				max="1"
				step="0.01"
				value={upperQuantile}
				onInput={(event) =>
					onDraft({ upperQuantile: Number((event.currentTarget as HTMLInputElement).value || 0.99) })}
				onBlur={() => commitPatch({ upperQuantile: Math.min(1, Math.max(0.01, Number(upperQuantile) || 0.99)) })}
			/>
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
