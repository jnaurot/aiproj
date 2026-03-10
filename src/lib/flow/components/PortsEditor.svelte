<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';

	export let selectedNode: Node<PipelineNodeData> | null;
	$: currentInType = selectedNode?.data?.ports?.in ?? null;
	$: currentOutType = selectedNode?.data?.ports?.out ?? null;
</script>

{#if selectedNode}
	<div class="portsTheme">
	<div class="section">
		<div class="sectionTitle">Ports</div>

		<div class="group">
			<div class="field field-inline">
				<div class="k">inPort</div>
				<div class="readonlyField">{currentInType ?? '(none)'}</div>
			</div>

			<div class="field field-inline">
				<div class="k">outPort</div>
				<div class="readonlyField">{currentOutType ?? '(none)'}</div>
			</div>
		</div>
		<div class="muted">Port types are derived from node subtype and params.</div>
	</div>
	<hr class="divider" />
	</div>
{/if}

<style>
	.portsTheme {
		--pe-card: #ffffff;
		--pe-border: #d7deea;
		--pe-text: #1f2937;
		--pe-muted: #5b6677;
		--pe-control-bg: #ffffff;
		--pe-control-text: #1f2937;
		--pe-control-border: #b9c5da;
		--pe-divider: #d7deea;
		--pe-error-bg: #fee2e2;
		--pe-error-border: #fca5a5;
		--pe-error-text: #7f1d1d;
		color: var(--pe-text);
	}

	@media (prefers-color-scheme: dark) {
		.portsTheme {
			--pe-card: #0f1724;
			--pe-border: #253049;
			--pe-text: #e5e7eb;
			--pe-muted: #9aa3b2;
			--pe-control-bg: #0b1220;
			--pe-control-text: #e5e7eb;
			--pe-control-border: #2c3b59;
			--pe-divider: #253049;
			--pe-error-bg: rgba(239, 68, 68, 0.12);
			--pe-error-border: rgba(239, 68, 68, 0.45);
			--pe-error-text: #fecaca;
		}
	}

	:global(.portsTheme .section) {
		background: var(--pe-card);
		border: 1px solid var(--pe-border);
		border-radius: 10px;
		padding: 8px 10px;
	}

	:global(.portsTheme .sectionTitle) {
		color: var(--pe-text);
	}

	:global(.portsTheme .k) {
		color: var(--pe-muted);
	}

	:global(.portsTheme .field-inline) {
		display: flex;
		align-items: center;
		gap: 10px;
	}

	:global(.portsTheme .field-inline .k) {
		min-width: 52px;
	}

	:global(.portsTheme .field-inline .v) {
		flex: 1 1 auto;
		min-width: 0;
	}

	.readonlyField {
		background: var(--pe-control-bg);
		color: var(--pe-control-text);
		border: 1px solid var(--pe-control-border);
		border-radius: 8px;
		padding: 6px 8px;
		min-height: 32px;
		display: flex;
		align-items: center;
	}

	.muted {
		margin-top: 8px;
		color: var(--pe-muted);
		font-size: 12px;
	}

	.divider {
		border: 1px solid var(--pe-divider);
		margin: 6px 0 3px;
	}

</style>
