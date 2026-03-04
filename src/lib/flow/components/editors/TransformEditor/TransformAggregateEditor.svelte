<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformAggregateParams } from '$lib/flow/schema/transform';
	import type { NodeExecutionError } from '$lib/flow/store/graphStore';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { toSchemaColumns } from './columnSelectionModel';
	import {
		AGG_OPS,
		availableAggregateColumnsFromError,
		defaultMetricName,
		missingAggregateColumnsFromError,
		normalizeAggregateParams,
		opNeedsColumn,
		validateAggregateDraft,
		type AggregateMetric,
		type AggregateOp
	} from './aggregateModel';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformAggregateParams>;
	export let onDraft: (patch: Partial<TransformAggregateParams>) => void;
	export let onCommit: (patch: Partial<TransformAggregateParams>) => void;
	export let inputColumns: string[] = [];
	export let nodeError: NodeExecutionError | null = null;

	let groupBy: string[] = [];
	let metrics: AggregateMetric[] = [];
	let groupByDraft = '';
	let lastNodeId = '';
	let lastParamsSignature = '';
	let suppressParamSync = false;

	$: void selectedNode?.id;
	$: normalized = normalizeAggregateParams(params);
	$: paramsSignature = JSON.stringify(normalized);
	$: errorAvailableColumns = availableAggregateColumnsFromError(nodeError);
	$: schemaColumns = toSchemaColumns([...inputColumns, ...errorAvailableColumns]);
	$: hasKnownSchema = schemaColumns.length > 0;
	$: knownColumns = toSchemaColumns([...schemaColumns, ...groupBy]);
	$: availableGroupByColumns = knownColumns.filter((col) => !groupBy.includes(col));
	$: missingFromError = missingAggregateColumnsFromError(nodeError);
	$: validation = validateAggregateDraft({ groupBy, metrics });

	$: if ((selectedNode?.id ?? '') !== lastNodeId) {
		lastNodeId = selectedNode?.id ?? '';
		groupBy = [...normalized.groupBy];
		metrics = normalized.metrics.map((m) => ({ ...m }));
		lastParamsSignature = paramsSignature;
	}
	$: if (!suppressParamSync && (selectedNode?.id ?? '') === lastNodeId && paramsSignature !== lastParamsSignature) {
		groupBy = [...normalized.groupBy];
		metrics = normalized.metrics.map((m) => ({ ...m }));
		lastParamsSignature = paramsSignature;
	}

	function markLocalEdit() {
		suppressParamSync = true;
		queueMicrotask(() => {
			suppressParamSync = false;
		});
	}

	function commitAggregate(nextGroupBy: string[], nextMetrics: AggregateMetric[]): void {
		const merged = normalizeAggregateParams({ groupBy: nextGroupBy, metrics: nextMetrics });
		markLocalEdit();
		groupBy = [...merged.groupBy];
		metrics = merged.metrics.map((m) => ({ ...m }));
		onDraft(merged);
		onCommit(merged);
	}

	function addGroupByColumn(col: string): void {
		const next = String(col ?? '').trim();
		if (!next || groupBy.includes(next)) return;
		commitAggregate([...groupBy, next], metrics);
	}

	function removeGroupByColumn(col: string): void {
		commitAggregate(
			groupBy.filter((c) => c !== col),
			metrics
		);
	}

	function uniqueMetricName(base: string): string {
		const cleanBase = String(base ?? '').trim() || 'metric';
		const used = new Set(metrics.map((m) => String(m.name ?? '').trim()).filter(Boolean));
		if (!used.has(cleanBase)) return cleanBase;
		let i = 2;
		while (used.has(`${cleanBase}_${i}`)) i += 1;
		return `${cleanBase}_${i}`;
	}

	function addMetric(op: AggregateOp = 'count_rows'): void {
		const name = uniqueMetricName(defaultMetricName(op));
		const metric: AggregateMetric = {
			name,
			op,
			column: opNeedsColumn(op) ? '' : null
		};
		commitAggregate(groupBy, [...metrics, metric]);
	}

	function removeMetric(index: number): void {
		const next = metrics.filter((_, i) => i !== index);
		commitAggregate(groupBy, next.length > 0 ? next : [{ name: 'row_count', op: 'count_rows', column: null }]);
	}

	function updateMetric(index: number, patch: Partial<AggregateMetric>): void {
		const next = metrics.map((m, i) => {
			if (i !== index) return m;
			const merged: AggregateMetric = { ...m, ...patch };
			if (!opNeedsColumn(merged.op)) {
				merged.column = null;
			} else {
				merged.column = String(merged.column ?? '').trim();
			}
			merged.name = String(merged.name ?? '').trim();
			return merged;
		});
		commitAggregate(groupBy, next);
	}

	function metricColumnOptions(metric: AggregateMetric): string[] {
		const options = [...knownColumns];
		const current = String(metric.column ?? '').trim();
		if (current && !options.includes(current)) options.push(current);
		return options.sort((a, b) => a.localeCompare(b));
	}
</script>

<Section title="Aggregate">
	<div class="hint">Group rows by selected columns and compute deterministic aggregate metrics.</div>

	<div class="groupByWrap">
		<div class="groupTitle">Group By</div>
		{#if !hasKnownSchema}
			<div class="addRow">
				<Input
					value={groupByDraft}
					placeholder="type column name"
					onInput={(event) => (groupByDraft = (event.currentTarget as HTMLInputElement).value)}
					onKeydown={(event) => {
						if ((event as KeyboardEvent).key !== 'Enter') return;
						event.preventDefault();
						const next = groupByDraft.trim();
						addGroupByColumn(next);
						groupByDraft = '';
					}}
				/>
				<button
					class="small"
					type="button"
					on:click={() => {
						const next = groupByDraft.trim();
						addGroupByColumn(next);
						groupByDraft = '';
					}}
				>
					+
				</button>
			</div>
		{/if}
		<div class="selectorGrid">
			<div class="listCol">
				<div class="listHeader">Selected</div>
				<div class="listBox">
					{#if groupBy.length === 0}
						<div class="emptySel">No group-by columns</div>
					{:else}
						{#each [...groupBy].sort((a, b) => a.localeCompare(b)) as col}
							<button class="chipBtn" type="button" on:click={() => removeGroupByColumn(col)}>{col}</button>
						{/each}
					{/if}
				</div>
			</div>

			<div class="listCol">
				<div class="listHeader">Available Cols</div>
				<div class="listBox">
					{#if availableGroupByColumns.length === 0}
						<div class="emptySel">{knownColumns.length > 0 ? 'No more column names' : 'Schema unavailable (run upstream)'}</div>
					{:else}
						{#each availableGroupByColumns as col}
							<button class="chipBtn" type="button" on:click={() => addGroupByColumn(col)}>{col}</button>
						{/each}
					{/if}
				</div>
			</div>
		</div>
	</div>

	<div class="metricsHeader">
		<div class="groupTitle">Metrics</div>
		<button class="small" type="button" on:click={() => addMetric('count_rows')}>+ Metric</button>
	</div>

	<div class="metricsList">
		{#each metrics as metric, index}
			<div class="metricCard">
				<div class="metricRow metricTop">
					<Input
						value={metric.name}
						placeholder="metric name"
						onInput={(event) => updateMetric(index, { name: (event.currentTarget as HTMLInputElement).value })}
					/>
					<button class="small danger metricRemove" type="button" on:click={() => removeMetric(index)}>-</button>
				</div>
				<div class="metricRow metricBottom">
					<select
						value={metric.op}
						on:change={(event) =>
							updateMetric(index, {
								op: (event.currentTarget as HTMLSelectElement).value as AggregateOp,
								column: opNeedsColumn((event.currentTarget as HTMLSelectElement).value as AggregateOp)
									? String(metric.column ?? '')
									: null
							})}
					>
						{#each AGG_OPS as op}
							<option value={op}>{op}</option>
						{/each}
					</select>
					{#if opNeedsColumn(metric.op)}
						{#if hasKnownSchema}
							<select
								value={String(metric.column ?? '')}
								on:change={(event) =>
									updateMetric(index, { column: (event.currentTarget as HTMLSelectElement).value })}
							>
								<option value="">select column</option>
								{#each metricColumnOptions(metric) as col}
									<option value={col}>{col}</option>
								{/each}
							</select>
						{:else}
							<Input
								value={String(metric.column ?? '')}
								placeholder="column"
								onInput={(event) => updateMetric(index, { column: (event.currentTarget as HTMLInputElement).value })}
							/>
						{/if}
					{:else}
						<div class="noneCol">(none)</div>
					{/if}
				</div>
			</div>
		{/each}
	</div>

	{#if !validation.ok}
		<div class="warn">{validation.message}</div>
	{/if}
	{#if missingFromError.length > 0}
		<div class="warn">Runtime mismatch: {missingFromError.join(', ')}</div>
	{/if}
</Section>

<style>
	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}

	.groupByWrap {
		margin-top: 10px;
	}

	.groupTitle {
		font-size: 12px;
		font-weight: 700;
		opacity: 0.9;
	}

	.addRow {
		display: grid;
		grid-template-columns: minmax(0, 220px) auto;
		gap: 8px;
		align-items: center;
		margin-top: 8px;
	}

	.selectorGrid {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 10px;
		margin-top: 8px;
	}

	.listCol {
		min-width: 0;
	}

	.listHeader {
		font-size: 12px;
		font-weight: 700;
		margin-bottom: 6px;
		opacity: 0.9;
	}

	.listBox {
		min-height: 42px;
		max-height: 188px;
		overflow-y: auto;
		border: 1px solid rgba(255, 255, 255, 0.16);
		border-radius: 10px;
		padding: 8px;
		display: grid;
		grid-template-columns: 1fr;
		gap: 6px;
	}

	.emptySel {
		font-size: 12px;
		opacity: 0.75;
	}

	.chipBtn {
		padding: 4px 8px;
		font-size: 12px;
		border-radius: 8px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.04);
		color: inherit;
		cursor: pointer;
		text-align: left;
	}

	.metricsHeader {
		display: flex;
		justify-content: space-between;
		align-items: center;
		gap: 8px;
		margin-top: 12px;
	}

	.metricsList {
		display: grid;
		gap: 8px;
		margin-top: 8px;
	}

	.metricCard {
		border: 1px solid rgba(255, 255, 255, 0.14);
		border-radius: 10px;
		padding: 8px;
		display: grid;
		gap: 8px;
	}

	.metricRow {
		display: grid;
		gap: 8px;
		align-items: center;
	}

	.metricTop {
		grid-template-columns: minmax(0, 1fr) auto;
	}

	.metricBottom {
		grid-template-columns: minmax(110px, 0.8fr) minmax(110px, 1fr);
	}

	.metricRow :global(input),
	.metricRow :global(select),
	.metricRow :global(button) {
		font-size: 12px;
	}

	.metricRow :global(input),
	.metricRow :global(select) {
		min-height: 34px;
		padding: 6px 8px;
	}

	.metricRow :global(select) {
		background: var(--field-bg, rgba(0, 0, 0, 0.2));
		color: var(--field-fg, inherit);
		border: 1px solid var(--field-border, rgba(255, 255, 255, 0.16));
		border-radius: 10px;
	}

	.metricRow :global(select option) {
		background: var(--field-bg, #111827);
		color: var(--field-fg, #e5e7eb);
	}

	.noneCol {
		font-size: 12px;
		opacity: 0.75;
		padding: 0 4px;
		min-height: 34px;
		display: flex;
		align-items: center;
		border: 1px solid rgba(255, 255, 255, 0.12);
		border-radius: 10px;
	}

	.metricRemove {
		min-width: 30px;
		padding-left: 0;
		padding-right: 0;
	}

	.metricRow {
		gap: 8px;
		align-items: center;
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

	button.danger {
		border-color: rgba(239, 68, 68, 0.5);
		background: rgba(239, 68, 68, 0.14);
	}

	.warn {
		font-size: 12px;
		color: #fca5a5;
		margin-top: 8px;
	}

	@media (max-width: 760px) {
		.selectorGrid {
			grid-template-columns: 1fr;
		}

		.metricRow {
			grid-template-columns: 1fr;
		}
	}
</style>
