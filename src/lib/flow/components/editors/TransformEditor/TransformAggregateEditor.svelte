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

			<button class="danger small" title="Remove metric" on:click={() => removeMetric(i)}>âœ•</button>
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
		border: 1px solid rgba(255, 255, 255, 0.08);
		border-radius: 12px;
		padding: 12px;
		background: rgba(255, 255, 255, 0.03);
		margin-top: 8px;
	}

	.sectionTitle {
		font-weight: 650;
		font-size: 14px;
		margin-bottom: 10px;
		opacity: 0.9;
	}

	.subTitle {
		margin-top: 10px;
		font-weight: 600;
		font-size: 13px;
		opacity: 0.95;
	}

	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-bottom: 10px;
		line-height: 1.35;
	}

	.row {
		display: flex;
		gap: 8px;
		align-items: flex-start;
		margin-bottom: 8px;
	}

	.line {
		display: flex;
		gap: 8px;
		align-items: center;
		margin-bottom: 8px;
	}

	.field {
		display: grid;
		grid-template-columns: 100px minmax(0, 1fr);
		align-items: start;
		gap: 8px;
		margin-bottom: 10px;
	}

	.field.grow {
		flex: 1;
	}

	.field.dir {
		grid-template-columns: 70px minmax(0, 1fr);
	}

	.k,
	.label {
		font-size: 14px;
		opacity: 0.85;
		padding-top: 8px;
		font-weight: 400;
	}

	.v {
		min-width: 0;
		width: 100%;
	}

	.colInput {
		flex: 1;
	}

	.arrow {
		opacity: 0.75;
		padding-top: 8px;
	}

	.toggle {
		display: inline-flex;
		gap: 8px;
		align-items: center;
	}

	input,
	select,
	textarea,
	.readonly,
	.code {
		width: 100%;
		box-sizing: border-box;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		background: rgba(0, 0, 0, 0.2);
		color: inherit;
		padding: 8px 10px;
		font-size: 14px;
		outline: none;
		min-height: 40px;
	}

	textarea,
	.code {
		resize: vertical;
		line-height: 1.35;
		min-height: 96px;
	}

	input[type='checkbox'] {
		width: auto;
		min-height: 0;
		padding: 0;
	}

	input:focus,
	select:focus,
	textarea:focus,
	.code:focus,
	.readonly:focus {
		border-color: rgba(255, 255, 255, 0.25);
	}

	.actions,
	.snips {
		margin-top: 8px;
		display: flex;
		gap: 8px;
		justify-content: flex-end;
		flex-wrap: wrap;
	}

	.snipsTitle {
		font-size: 12px;
		opacity: 0.8;
		align-self: center;
	}

	.snipRow {
		display: flex;
		gap: 8px;
		width: 100%;
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
		color: #fecaca;
	}

	.warn {
		margin-top: 8px;
		font-size: 12px;
		color: #fca5a5;
		white-space: pre-wrap;
	}

	.warn ul {
		margin: 6px 0 0 16px;
		padding: 0;
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
		opacity: 0.95;
	}

	code {
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
		font-size: 12px;
	}
</style>

