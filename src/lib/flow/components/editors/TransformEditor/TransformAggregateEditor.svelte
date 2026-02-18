<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { TransformNodeData } from '$lib/flow/types/transform';
	import type { TransformAggregateParams } from '$lib/flow/schema/transform';

	export let selectedNode: Node<TransformNodeData>;
	export let params: TransformAggregateParams;
	export let onDraft: (patch: Partial<TransformAggregateParams>) => void;
	export let onCommit: (patch: Partial<TransformAggregateParams>) => void;

	type Metric = { as: string; expr: string };

	const DEFAULT_GROUP_BY: string[] = ['category'];
	const DEFAULT_METRICS: Metric[] = [
		{ as: 'row_count', expr: 'count(*)' },
		{ as: 'avg_length', expr: 'avg(length(text))' }
	];

	$: groupBy = params?.groupBy ?? DEFAULT_GROUP_BY;
	$: metrics = (params?.metrics ?? []) as Metric[];
	$: metricsView = metrics.length ? metrics : DEFAULT_METRICS;

	function normCols(list: string[]) {
		// allow empty => whole-table aggregate
		const cleaned = list.map((s) => s.trim()).filter(Boolean);
		return cleaned;
	}

	function normMetrics(list: Metric[]) {
		const cleaned = list
			.map((m) => ({ as: (m.as ?? '').trim(), expr: (m.expr ?? '').trim() }))
			.filter((m) => m.as.length > 0 && m.expr.length > 0);
		return cleaned;
	}

	function draftGroupByText(text: string) {
		const next = normCols(text.split(','));
		onDraft({ groupBy: next as any });
	}

	function draftMetricAs(i: number, value: string) {
		const next = metricsView.map((m, idx) => (idx === i ? { ...m, as: value } : m));
		onDraft({ metrics: normMetrics(next) as any });
	}

	function draftMetricExpr(i: number, value: string) {
		const next = metricsView.map((m, idx) => (idx === i ? { ...m, expr: value } : m));
		onDraft({ metrics: normMetrics(next) as any });
	}

	function addMetric() {
		const next = [...metricsView, { as: '', expr: '' }];
		onDraft({ metrics: normMetrics(next) as any });
	}

	function removeMetric(i: number) {
		const next = metricsView.filter((_, idx) => idx !== i);
		onDraft({ metrics: normMetrics(next) as any });
	}

	function resetDefaults() {
		onDraft({ groupBy: DEFAULT_GROUP_BY as any, metrics: DEFAULT_METRICS as any });
		onCommit({ groupBy: DEFAULT_GROUP_BY as any, metrics: DEFAULT_METRICS as any });
	}

	function commit() {
		onCommit({
			groupBy: normCols(groupBy) as any,
			metrics: normMetrics(metricsView) as any
		});
	}

	$: groupByText = (groupBy ?? []).join(', ');
</script>

<div class="section">
	<div class="sectionTitle">Aggregate</div>

	<div class="hint">
		Group by columns (optional) and define one or more aggregate metrics.
		Expressions run in DuckDB.
	</div>

	<div class="field">
		<div class="k">groupBy</div>
		<div class="v">
			<input
				value={groupByText}
				placeholder="e.g. category, user_id (leave empty for whole-table)"
				on:input={(e) => draftGroupByText((e.currentTarget as HTMLInputElement).value)}
				on:blur={commit}
			/>
		</div>
	</div>

	<div class="subTitle">Metrics</div>

	{#each metricsView as m, i}
		<div class="row">
			<div class="field">
				<div class="k">as</div>
				<div class="v">
					<input
						value={m.as}
						placeholder="e.g. row_count"
						on:input={(e) => draftMetricAs(i, (e.currentTarget as HTMLInputElement).value)}
						on:blur={commit}
					/>
				</div>
			</div>

			<div class="field grow">
				<div class="k">expr</div>
				<div class="v">
					<input
						value={m.expr}
						placeholder="e.g. count(*)"
						on:input={(e) => draftMetricExpr(i, (e.currentTarget as HTMLInputElement).value)}
						on:blur={commit}
					/>
				</div>
			</div>

			<button class="danger small" title="Remove metric" on:click={() => removeMetric(i)}>✕</button>
		</div>
	{/each}

	<div class="actions">
		<button class="small" on:click={addMetric}>+ Add metric</button>
		<button class="small ghost" on:click={resetDefaults}>Reset defaults</button>
	</div>

	{#if !params?.metrics || params.metrics.length === 0}
		<div class="warn">At least one metric (as + expr) is required.</div>
	{/if}

	<div class="preview">
		<div class="label">Preview</div>
		<pre>
SELECT
{#if normCols(groupBy).length}{normCols(groupBy).join(', ')},{/if}
{normMetrics(metricsView).map((x) => `${x.expr} AS ${x.as}`).join(', ')}
FROM input
{#if normCols(groupBy).length}
GROUP BY {normCols(groupBy).join(', ')}
{/if}
		</pre>
	</div>
</div>

<style>
	.section {
		margin-top: 8px;
	}

	.sectionTitle {
		font-weight: 600;
		margin-bottom: 6px;
	}

	.subTitle {
		margin-top: 10px;
		font-weight: 600;
		font-size: 13px;
		opacity: 0.95;
	}

	.hint {
		font-size: 12px;
		opacity: 0.8;
		margin-bottom: 10px;
	}

	.row {
		display: flex;
		gap: 8px;
		align-items: center;
		margin-bottom: 8px;
	}

	.field {
		display: flex;
		gap: 6px;
		align-items: center;
		margin-bottom: 8px;
	}

	.field.grow .v {
		width: 100%;
	}

	.grow {
		flex: 1;
	}

	.k {
		width: 60px;
		font-size: 12px;
		opacity: 0.8;
	}

	.v {
		width: 220px;
	}

	input {
		width: 100%;
		padding: 4px 6px;
		border-radius: 4px;
		border: 1px solid #ccc;
		box-sizing: border-box;
	}

	.actions {
		display: flex;
		gap: 8px;
		margin-top: 4px;
	}

	button.small {
		padding: 4px 8px;
		font-size: 12px;
	}

	button.ghost {
		background: transparent;
		border: 1px solid #ccc;
		border-radius: 4px;
		cursor: pointer;
	}

	button.danger {
		background: #f44336;
		color: white;
		border: none;
		border-radius: 4px;
		cursor: pointer;
		padding: 4px 8px;
	}

	.warn {
		margin-top: 8px;
		font-size: 12px;
		color: #b00020;
		white-space: pre-wrap;
	}

	.preview {
		margin-top: 10px;
	}

	.label {
		font-weight: 700;
		margin-bottom: 6px;
	}

	pre {
		white-space: pre-wrap;
		word-break: break-word;
		padding: 8px;
		border: 1px solid #ddd;
		border-radius: 6px;
		font-size: 12px;
		opacity: 0.95;
	}
</style>
