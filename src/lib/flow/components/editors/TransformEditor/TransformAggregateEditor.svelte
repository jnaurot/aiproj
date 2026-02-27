<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformAggregateParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';

	type Metric = NonNullable<TransformAggregateParams['metrics']>[number];

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformAggregateParams>;
	export let onDraft: (patch: Partial<TransformAggregateParams>) => void;
	export let onCommit: (patch: Partial<TransformAggregateParams>) => void;

	const defaultGroupBy = ['category'];
	const defaultMetrics: Metric[] = [
		{ as: 'row_count', expr: 'count(*)' },
		{ as: 'avg_length', expr: 'avg(length(text))' }
	];

	$: void selectedNode?.id;
	$: groupBy = params?.groupBy ?? defaultGroupBy;
	$: metrics = params?.metrics?.length ? params.metrics : defaultMetrics;
	$: groupByText = groupBy.join(', ');

	function normalizeMetrics(items: Metric[]): Metric[] {
		return items
			.map((item) => ({ as: item.as.trim(), expr: item.expr.trim() }))
			.filter((item) => item.as.length > 0 && item.expr.length > 0);
	}

	function updateMetric(index: number, key: keyof Metric, value: string): void {
		const next = metrics.map((metric, current) => (current === index ? { ...metric, [key]: value } : metric));
		onDraft({ metrics: normalizeMetrics(next) });
	}
</script>

<Section title="Aggregate">
	<div class="hint">Group by columns (optional) and define one or more aggregate metrics.</div>

	<Field label="groupBy">
		<Input
			value={groupByText}
			placeholder="e.g. category, user_id"
			onInput={(event) =>
				onDraft({ groupBy: uniqueStrings((event.currentTarget as HTMLInputElement).value.split(',')) })}
			onBlur={() => onCommit({ groupBy: uniqueStrings(groupByText.split(',')), metrics: normalizeMetrics(metrics) })}
		/>
	</Field>

	<div class="subTitle">Metrics</div>
	{#each metrics as metric, index}
		<div class="row">
			<Input
				value={metric.as}
				placeholder="alias"
				onInput={(event) => updateMetric(index, 'as', (event.currentTarget as HTMLInputElement).value)}
				onBlur={() => onCommit({ groupBy: uniqueStrings(groupByText.split(',')), metrics: normalizeMetrics(metrics) })}
			/>
			<Input
				value={metric.expr}
				placeholder="expression"
				onInput={(event) => updateMetric(index, 'expr', (event.currentTarget as HTMLInputElement).value)}
				onBlur={() => onCommit({ groupBy: uniqueStrings(groupByText.split(',')), metrics: normalizeMetrics(metrics) })}
			/>
			<button class="small danger" type="button" on:click={() => onDraft({ metrics: normalizeMetrics(metrics.filter((_, current) => current !== index)) })}>
				x
			</button>
		</div>
	{/each}

	<div class="actions">
		<button class="small" type="button" on:click={() => onDraft({ metrics: normalizeMetrics([...metrics, { as: '', expr: '' }]) })}>
			+ Add metric
		</button>
		<button
			class="small ghost"
			type="button"
			on:click={() => {
				onDraft({ groupBy: defaultGroupBy, metrics: defaultMetrics });
				onCommit({ groupBy: defaultGroupBy, metrics: defaultMetrics });
			}}
		>
			Reset defaults
		</button>
	</div>

	{#if !params?.metrics || params.metrics.length === 0}
		<div class="warn">At least one metric is required.</div>
	{/if}

	<div class="preview">
		<div class="subTitle">Preview</div>
		<pre>SELECT
{#if uniqueStrings(groupByText.split(',')).length > 0}{uniqueStrings(groupByText.split(',')).join(', ')},{/if}
{normalizeMetrics(metrics).map((item) => `${item.expr} AS ${item.as}`).join(', ')}
FROM input
{#if uniqueStrings(groupByText.split(',')).length > 0}GROUP BY {uniqueStrings(groupByText.split(',')).join(', ')}{/if}</pre>
	</div>
</Section>

<style>
	.row {
		display: grid;
		grid-template-columns: minmax(160px, 0.7fr) minmax(0, 1.3fr) auto;
		gap: 8px;
		align-items: center;
		margin: 8px 0;
	}

	.subTitle {
		margin-top: 10px;
		font-size: 13px;
		font-weight: 600;
	}

	.hint,
	.warn {
		font-size: 12px;
		margin-top: 6px;
	}

	.hint {
		opacity: 0.75;
	}

	.warn {
		color: #fca5a5;
	}

	.actions {
		display: flex;
		gap: 8px;
		justify-content: flex-end;
		margin-top: 8px;
		flex-wrap: wrap;
	}

	.preview {
		margin-top: 12px;
	}

	pre {
		white-space: pre-wrap;
		word-break: break-word;
		padding: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		border-radius: 10px;
		font-size: 12px;
	}

	button.small {
		padding: 6px 10px;
		font-size: 12px;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.06);
		color: inherit;
		cursor: pointer;
	}

	button.ghost {
		background: transparent;
	}

	button.danger {
		border-color: rgba(239, 68, 68, 0.5);
		background: rgba(239, 68, 68, 0.14);
	}
</style>
