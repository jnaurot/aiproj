<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformOutlierPolicyParams } from '$lib/flow/schema/transform';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	type OutlierMode = TransformOutlierPolicyParams['mode'];
	type OutlierMethod = TransformOutlierPolicyParams['method'];

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformOutlierPolicyParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformOutlierPolicyParams>) => void;
	export let onCommit: (patch: Partial<TransformOutlierPolicyParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	$: void selectedNode?.id;
	$: void onCommit;
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
	$: quantileValid = lowerQuantile < upperQuantile;
	const meta = getTransformMeta('outlier_policy');

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
		<ColumnTokenInput value={columns} schema={columnOptions} onChange={(next) => commitPatch({ columns: next })} placeholder="Add outlier column" />
	</Field>

	{#if method === 'iqr'}
		<ConditionalHint text="IQR method requires an IQR multiplier." />
		<Field label="iqr multiplier">
			<Input
				type="number"
				min="0.1"
				step="0.1"
				value={iqrMultiplier}
				onInput={(event) =>
					commitPatch({ iqrMultiplier: Number((event.currentTarget as HTMLInputElement).value || 1.5) })}
			/>
		</Field>
	{/if}

	{#if method === 'zscore'}
		<ConditionalHint text="Z-score method requires a z-score threshold." />
		<Field label="zscore threshold">
			<Input
				type="number"
				min="0.1"
				step="0.1"
				value={zscoreThreshold}
				onInput={(event) =>
					commitPatch({ zscoreThreshold: Number((event.currentTarget as HTMLInputElement).value || 3) })}
			/>
		</Field>
	{/if}

	{#if method === 'quantile'}
		<ConditionalHint text="Quantile method requires both lower and upper quantiles." />
		<Field label="lower quantile">
			<Input
				type="number"
				min="0"
				max="1"
				step="0.01"
				value={lowerQuantile}
				onInput={(event) =>
					commitPatch({ lowerQuantile: Number((event.currentTarget as HTMLInputElement).value || 0.01) })}
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
					commitPatch({ upperQuantile: Number((event.currentTarget as HTMLInputElement).value || 0.99) })}
			/>
		</Field>
		{#if !quantileValid}
			<ConditionalHint tone="warn" text="Upper quantile must be greater than lower quantile." />
		{/if}
	{/if}
</Section>
