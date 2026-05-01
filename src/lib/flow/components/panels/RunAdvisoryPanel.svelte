<script lang="ts">
	import type { AdvisoryItem } from '$lib/flow/components/runAdvisor';

	export let items: AdvisoryItem[] = [];
	export let onNodeClick: ((nodeId: string) => void) | null = null;

	const expanded = new Set<string>();

	function toggle(id: string): void {
		if (expanded.has(id)) expanded.delete(id);
		else expanded.add(id);
		expanded = new Set(expanded);
	}

	function severityCount(severity: 'error' | 'warning' | 'info'): number {
		return items.filter((item) => item.severity === severity).length;
	}

	function handleNodeClick(nodeId: string): void {
		onNodeClick?.(nodeId);
	}
</script>

<div class="advisorSummary mono">
	errors={severityCount('error')} | warnings={severityCount('warning')} | info={severityCount('info')}
</div>

{#if items.length === 0}
	<div class="envProfileEmpty">No advisory items for current run snapshot.</div>
{:else}
	<div class="advisorList">
		{#each items as item (item.id)}
			<div class={`advisorItem sev-${item.severity}`}>
				<button type="button" class="advisorHead" on:click={() => toggle(item.id)}>
					<span class="advisorTitle">{item.title}</span>
					<span class="advisorMeta mono">{item.severity} | {item.confidence}</span>
				</button>
				{#if expanded.has(item.id)}
					<div class="advisorBody">
						{#if item.nodeIds.length > 0}
							<div class="advisorNodes">
								{#each item.nodeIds as nodeId (`${item.id}:${nodeId}`)}
									<button type="button" class="advisorNodeChip mono" on:click={() => handleNodeClick(nodeId)}>{nodeId}</button>
								{/each}
							</div>
						{/if}
						<p class="advisorExplanation">{item.explanation}</p>
						{#if item.actions.length > 0}
							<div class="advisorActions">
								<strong>Suggested action</strong>
								<ul>
									{#each item.actions as action (`${item.id}:${action}`)}
										<li>{action}</li>
									{/each}
								</ul>
							</div>
						{/if}
						{#if item.evidence.length > 0}
							<pre class="advisorEvidence mono">{item.evidence.join('\n')}</pre>
						{/if}
					</div>
				{/if}
			</div>
		{/each}
	</div>
{/if}

<style>
	.advisorSummary {
		font-size: 12px;
		opacity: 0.85;
		margin-bottom: 8px;
	}
	.advisorList {
		display: grid;
		gap: 8px;
	}
	.advisorItem {
		border: 1px solid #22304a;
		border-left-width: 3px;
		border-radius: 8px;
		background: rgba(10, 16, 28, 0.65);
	}
	.advisorItem.sev-error {
		border-left-color: #ff7b72;
	}
	.advisorItem.sev-warning {
		border-left-color: #f2cc60;
	}
	.advisorItem.sev-info {
		border-left-color: #79c0ff;
	}
	.advisorHead {
		width: 100%;
		background: transparent;
		border: 0;
		color: inherit;
		display: flex;
		justify-content: space-between;
		align-items: center;
		padding: 8px 10px;
		cursor: pointer;
	}
	.advisorTitle {
		font-weight: 600;
		text-align: left;
	}
	.advisorMeta {
		font-size: 11px;
		opacity: 0.8;
	}
	.advisorBody {
		padding: 0 10px 10px 10px;
		display: grid;
		gap: 8px;
	}
	.advisorNodes {
		display: flex;
		gap: 6px;
		flex-wrap: wrap;
	}
	.advisorNodeChip {
		border: 1px solid #2b3d5c;
		border-radius: 999px;
		padding: 2px 8px;
		background: rgba(28, 45, 71, 0.55);
		color: #d8e4ff;
		cursor: pointer;
	}
	.advisorExplanation {
		margin: 0;
		font-size: 12px;
		line-height: 1.4;
	}
	.advisorActions strong {
		display: block;
		margin-bottom: 4px;
	}
	.advisorActions ul {
		margin: 0;
		padding-left: 18px;
	}
	.advisorEvidence {
		margin: 0;
		font-size: 11px;
		white-space: pre-wrap;
		word-break: break-word;
		background: rgba(7, 12, 20, 0.7);
		border: 1px solid #1f2d44;
		border-radius: 6px;
		padding: 8px;
	}
</style>
