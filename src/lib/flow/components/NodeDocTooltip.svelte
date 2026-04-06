<script lang="ts">
	import type { NodeDocResolved } from './nodeDocsViewModel';

	export let doc: NodeDocResolved | null = null;
	export let open: boolean = false;
	export let expanded: boolean = false;

	$: safeTitle = String(doc?.title ?? 'Node documentation');
	$: safeSummary = String(doc?.summary ?? 'No documentation is available for this node yet.');
	$: dataSummary = String(doc?.planes?.data?.summary ?? 'No data-plane details.');
	$: controlSummary = String(doc?.planes?.control?.summary ?? 'No control-plane details.');
	$: paramSummary = String(doc?.planes?.param?.summary ?? 'No param-plane details.');
</script>

{#if open}
	<div class="nodeDocTooltip" role="tooltip" aria-live="polite">
		<div class="header">
			<div class="title">{safeTitle}</div>
			<div class="chips">
				<span class="chip">data</span>
				<span class="chip">control</span>
				<span class="chip">param</span>
			</div>
		</div>
		<div class="summary">{safeSummary}</div>
		{#if expanded}
			<div class="section">
				<div class="sectionTitle">Data Plane</div>
				<div class="sectionBody">{dataSummary}</div>
			</div>
			<div class="section">
				<div class="sectionTitle">Control Plane</div>
				<div class="sectionBody">{controlSummary}</div>
			</div>
			<div class="section">
				<div class="sectionTitle">Param Plane</div>
				<div class="sectionBody">{paramSummary}</div>
			</div>
		{/if}
	</div>
{/if}

<style>
	.nodeDocTooltip {
		position: absolute;
		left: 0;
		top: calc(100% + 6px);
		width: 300px;
		max-width: 340px;
		background: #0b1220;
		border: 1px solid #2a3651;
		border-radius: 10px;
		padding: 8px 10px;
		box-shadow: 0 10px 24px rgba(0, 0, 0, 0.45);
		color: #dce6ff;
		z-index: 60;
		pointer-events: none;
	}

	.header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
	}

	.title {
		font-size: 12px;
		font-weight: 700;
	}

	.chips {
		display: inline-flex;
		align-items: center;
		gap: 4px;
	}

	.chip {
		font-size: 10px;
		padding: 1px 6px;
		border-radius: 999px;
		border: 1px solid #334b77;
		color: #b7caef;
	}

	.summary {
		margin-top: 6px;
		font-size: 11px;
		line-height: 1.35;
		color: #cfdbf7;
	}

	.section {
		margin-top: 8px;
		padding-top: 6px;
		border-top: 1px dashed rgba(94, 127, 188, 0.45);
	}

	.sectionTitle {
		font-size: 11px;
		font-weight: 700;
		color: #c4d5f8;
	}

	.sectionBody {
		margin-top: 2px;
		font-size: 11px;
		line-height: 1.35;
		color: #c9d8f4;
	}
</style>
