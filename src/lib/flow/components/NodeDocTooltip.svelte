<script lang="ts">
	import type { NodeDocResolved } from './nodeDocsViewModel';
	import type { NodeDocExplanationMode, NodeDocGeneratedExplanation } from '$lib/flow/schema/nodeDocs';
	import type { NodeDocLlmContext } from './nodeDocLlmContext';
	import { getOrGenerateNodeDocLlmExplanation } from './nodeDocLlmCache';

	export let doc: NodeDocResolved | null = null;
	export let open: boolean = false;
	export let expanded: boolean = false;
	export let mode: NodeDocExplanationMode = 'default';
	export let nodeId: string = '';
	export let llmContext: NodeDocLlmContext | null = null;
	export let llmSignature: string = '';

	let llmLoading = false;
	let llmFailure = false;
	let llmDoc: NodeDocGeneratedExplanation | null = null;
	let llmInFlightKey = '';

	$: shouldUseLlm = mode === 'llm';
	$: explanationSourceLabel = shouldUseLlm ? 'AI-generated explanation' : 'Default explanation';
	$: safeSummary = (() => {
		if (shouldUseLlm && llmDoc?.summary) return String(llmDoc.summary);
		return String(doc?.summary ?? 'No documentation is available for this node yet.');
	})();
	$: loadingLabel = llmLoading ? 'generating...' : '';
	$: failureLabel = llmFailure ? 'failed - showing default explanation' : '';

	$: safeTitle = String(doc?.title ?? 'Node documentation');
	$: dataSummary = String(doc?.planes?.data?.summary ?? 'No data-plane details.');
	$: controlSummary = String(doc?.planes?.control?.summary ?? 'No control-plane details.');
	$: paramSummary = String(doc?.planes?.param?.summary ?? 'No param-plane details.');

	$: {
		const key = `${String(mode)}::${String(nodeId)}::${String(llmSignature)}::${String(open)}::${String(expanded)}`;
		if (!open || !expanded || !shouldUseLlm || !llmContext || !llmSignature) {
			if (!shouldUseLlm) {
				llmLoading = false;
				llmFailure = false;
			}
		} else if (key !== llmInFlightKey) {
			llmInFlightKey = key;
			llmLoading = true;
			llmFailure = false;
			void getOrGenerateNodeDocLlmExplanation('llm', nodeId, llmContext, llmSignature)
				.then((result) => {
					if (llmInFlightKey !== key) return;
					llmDoc = result.explanation;
					llmFailure = !Boolean(result.explanation);
				})
				.catch(() => {
					if (llmInFlightKey !== key) return;
					llmDoc = null;
					llmFailure = true;
				})
				.finally(() => {
					if (llmInFlightKey === key) llmLoading = false;
				});
		}
	}
</script>

{#if open}
	<div class="nodeDocTooltip" role="tooltip" aria-live="polite">
		<div class="header">
			<div class="title">{safeTitle}</div>
			<div class="chips">
				<span class="chip source">{explanationSourceLabel}</span>
				<span class="chip">data</span>
				<span class="chip">control</span>
				<span class="chip">param</span>
			</div>
		</div>
		<div class="summary">{safeSummary}</div>
		{#if loadingLabel}
			<div class="meta">{loadingLabel}</div>
		{/if}
		{#if failureLabel}
			<div class="meta warn">{failureLabel}</div>
		{/if}
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

	.chip.source {
		border-color: #46639a;
		color: #d8e6ff;
	}

	.summary {
		margin-top: 6px;
		font-size: 11px;
		line-height: 1.35;
		color: #cfdbf7;
	}

	.meta {
		margin-top: 4px;
		font-size: 10px;
		color: #9fb8e8;
	}

	.meta.warn {
		color: #f0b686;
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
