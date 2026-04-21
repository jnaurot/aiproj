<script lang="ts">
	import type { CheckpointRecord } from '$lib/flow/types/checkpoint';

	export type CheckpointPanelRow = {
		nodeId: string;
		nodeName: string;
		checkpoint: CheckpointRecord;
		depth?: number;
		isPromoted?: boolean;
		componentPath?: string;
		removable?: boolean;
	};

	export let rows: CheckpointPanelRow[] = [];
	export let onRemove: ((nodeId: string) => void) | undefined;
	export let onRemoveAll: (() => void) | undefined;
	export let onRemoveAllStale: (() => void) | undefined;
	export let onRename: ((nodeId: string, name: string) => void) | undefined;

	const staleStatuses = new Set(['stale', 'artifact_missing']);

	function badgeClass(staleness: string): string {
		if (staleness === 'valid') return 'badge-valid';
		if (staleness === 'stale') return 'badge-stale';
		if (staleness === 'artifact_missing') return 'badge-missing';
		return 'badge-unknown';
	}

	$: staleCount = rows.filter((row) => staleStatuses.has(String(row.checkpoint?.staleness ?? ''))).length;

	function handleRename(nodeId: string, value: string): void {
		const trimmed = String(value ?? '').trim();
		if (!trimmed || !onRename) return;
		onRename(nodeId, trimmed);
	}

	let lastComponentPath = '';
</script>

<section class="checkpoint-panel" aria-label="Checkpoint Registry">
	<div class="toolbar">
		<button type="button" on:click={() => onRemoveAllStale?.()} disabled={staleCount <= 0}>
			Remove all stale
		</button>
		<button type="button" on:click={() => onRemoveAll?.()} disabled={rows.length <= 0}>
			Remove all
		</button>
	</div>

	{#if rows.length <= 0}
		<div class="empty-state">No checkpoints. Run a node and save its output as a checkpoint.</div>
	{:else}
		<table>
			<thead>
				<tr>
					<th>Node Name</th>
					<th>Checkpoint Name</th>
					<th>Created</th>
					<th>Staleness</th>
					<th>Actions</th>
				</tr>
			</thead>
			<tbody>
				{#each rows as row (row.nodeId)}
					{#if row.isPromoted && row.componentPath && row.componentPath !== lastComponentPath}
						<tr class="component-group-header">
							<td colspan="5">{row.componentPath}</td>
						</tr>
						{@const lastComponentPath = row.componentPath}
					{/if}
					<tr class={row.isPromoted ? 'promoted-row' : ''}>
						<td style="padding-left: {8 + (row.depth ?? 0) * 16}px;">
							{#if row.isPromoted}
								<span class="promoted-label" title="Internal node of {row.componentPath ?? 'component'}">{row.nodeName}</span>
							{:else}
								{row.nodeName}
							{/if}
						</td>
						<td>
							{#if row.removable !== false}
								<input
									aria-label={`Checkpoint name ${row.nodeId}`}
									value={row.checkpoint.name}
									on:change={(event) =>
										handleRename(row.nodeId, (event.currentTarget as HTMLInputElement)?.value)}
								/>
							{:else}
								<span class="readonly-name">{row.checkpoint.name}</span>
							{/if}
						</td>
						<td>{row.checkpoint.createdAt}</td>
						<td>
							<span class={`badge ${badgeClass(row.checkpoint.staleness)}`}>{row.checkpoint.staleness}</span>
						</td>
						<td>
							{#if row.removable !== false}
								<button type="button" on:click={() => onRemove?.(row.nodeId)}>Remove</button>
							{:else}
								<span class="readonly-hint" title="Edit inside the component to modify this checkpoint">internal</span>
							{/if}
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
</section>

<style>
	.checkpoint-panel {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}
	.toolbar {
		display: flex;
		gap: 8px;
	}
	.empty-state {
		opacity: 0.9;
	}
	table {
		width: 100%;
		border-collapse: collapse;
	}
	th,
	td {
		text-align: left;
		padding: 4px 6px;
	}
	.badge {
		padding: 2px 6px;
		border-radius: 10px;
		font-size: 12px;
	}
	.badge-valid {
		background: rgba(34, 197, 94, 0.2);
	}
	.badge-stale {
		background: rgba(245, 158, 11, 0.2);
	}
	.badge-missing {
		background: rgba(239, 68, 68, 0.2);
	}
	.badge-unknown {
		background: rgba(148, 163, 184, 0.25);
	}
	.component-group-header td {
		font-weight: 600;
		font-size: 11px;
		color: rgba(148, 163, 184, 0.8);
		border-bottom: 1px solid rgba(148, 163, 184, 0.15);
		padding: 6px 6px 3px 6px;
		text-transform: uppercase;
		letter-spacing: 0.5px;
	}
	.promoted-row {
		opacity: 0.92;
	}
	.promoted-label {
		color: rgba(148, 163, 184, 0.9);
	}
	.readonly-name {
		color: rgba(148, 163, 184, 0.7);
		font-style: italic;
		font-size: 12px;
	}
	.readonly-hint {
		font-size: 10px;
		color: rgba(148, 163, 184, 0.6);
		font-style: italic;
	}
</style>