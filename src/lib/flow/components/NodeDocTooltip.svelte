<script lang="ts">
	import type { NodeDocResolved } from './nodeDocsViewModel';
	import type {
		NodeDocExplanationMode,
		NodeDocGeneratedExplanation,
		NodeDocTrainingMode
	} from '$lib/flow/schema/nodeDocs';
	import type { NodeDocLlmContext } from './nodeDocLlmContext';
	import { clearNodeDocLlmCacheEntry, getOrGenerateNodeDocLlmExplanation } from './nodeDocLlmCache';
	import { submitNodeDocLlmFeedback } from './nodeDocLlmService';

	export let doc: NodeDocResolved | null = null;
export let open: boolean = false;
export let expanded: boolean = false;
export let planesExpansionEnabled: boolean = true;
export let mode: NodeDocExplanationMode = 'default';
export let trainingMode: NodeDocTrainingMode = 'off';
export let llmModel: string = 'glm-4.7-flash:latest';
export let llmTemperature: number = 0.2;
export let llmTopP: number = 1.0;
export let llmMaxTokens: number = 512;
export let nodeId: string = '';
	export let llmContext: NodeDocLlmContext | null = null;
	export let llmSignature: string = '';
	export let onPersistGenerated: ((explanation: NodeDocGeneratedExplanation) => void) | null = null;
	export let onClearPersistedGenerated: (() => void) | null = null;

	let llmLoading = false;
	let llmFailure = false;
	let llmDoc: NodeDocGeneratedExplanation | null = null;
	let llmInFlightKey = '';
	let feedbackBusy = false;
	let feedbackVerdict: '' | 'good' | 'bad' = '';
	let feedbackStatus = '';
	let feedbackCorrection = '';
	let feedbackShowCorrection = false;
	let persistedInvalidationKey = '';

	$: shouldUseLlm = mode === 'llm';
	$: trainingEnabled = trainingMode === 'on';
	$: explanationSourceLabel = shouldUseLlm ? 'AI' : 'Default';
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
			const persisted = doc?.generated ?? null;
			if (persisted && String(persisted.signature_key ?? '') === String(llmSignature ?? '')) {
				llmDoc = persisted;
				llmLoading = false;
				llmFailure = false;
				llmInFlightKey = key;
			} else {
				if (persisted && String(persisted.signature_key ?? '').trim()) {
					const invalidationKey = `${String(nodeId ?? '')}::${String(llmSignature ?? '')}`;
					if (invalidationKey !== persistedInvalidationKey) {
						persistedInvalidationKey = invalidationKey;
						onClearPersistedGenerated?.();
					}
				}
				llmInFlightKey = key;
				llmLoading = true;
				llmFailure = false;
					void getOrGenerateNodeDocLlmExplanation('llm', nodeId, llmContext, llmSignature, {
						provider: 'ollama',
						model: llmModel,
						temperature: llmTemperature,
						topP: llmTopP,
						maxTokens: llmMaxTokens
					})
					.then((result) => {
						if (llmInFlightKey !== key) return;
						llmDoc = result.explanation;
						llmFailure = !Boolean(result.explanation);
						if (result.explanation && String(result.explanation.signature_key ?? '') === String(llmSignature ?? '')) {
							onPersistGenerated?.(result.explanation);
						}
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
	}

	$: if (!open) {
		feedbackShowCorrection = false;
		feedbackCorrection = '';
		feedbackStatus = '';
		feedbackVerdict = '';
	}

	async function submitFeedback(verdict: 'good' | 'bad'): Promise<void> {
		if (!trainingEnabled || !shouldUseLlm || !llmContext || !llmSignature) return;
		if (feedbackBusy) return;
		if (verdict === 'bad' && !String(feedbackCorrection ?? '').trim()) {
			feedbackStatus = 'Please enter a better explanation before submitting.';
			return;
		}
		feedbackBusy = true;
		feedbackStatus = '';
		try {
			const generatedSummary = String(llmDoc?.summary ?? safeSummary ?? '').trim();
			const result = await submitNodeDocLlmFeedback({
				context: llmContext,
				signatureKey: llmSignature,
				generatedSummary,
				verdict,
				correctedSummary: feedbackCorrection
			});
			if (!result?.ok) {
				feedbackStatus = 'Feedback submit failed.';
				return;
			}
			feedbackVerdict = verdict;
			if (verdict === 'good') {
				feedbackStatus = 'Saved as good feedback.';
				feedbackShowCorrection = false;
			} else {
				const fieldsText = result.suggested_fields.length > 0 ? result.suggested_fields.join(', ') : 'none';
				feedbackStatus = `Saved bad feedback. Suggested fields: ${fieldsText}.`;
				if (llmSignature && nodeId) {
					// Force next tooltip explanation to regenerate after corrective feedback.
					clearNodeDocLlmCacheEntry('llm', nodeId, llmSignature);
				}
				onClearPersistedGenerated?.();
				llmDoc = null;
				llmFailure = false;
				llmInFlightKey = '';
			}
		} catch {
			feedbackStatus = 'Feedback submit failed.';
		} finally {
			feedbackBusy = false;
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
		{#if shouldUseLlm && trainingEnabled}
			<div class="feedbackRow">
				<button
					type="button"
					class={`feedbackBtn ${feedbackVerdict === 'good' ? 'active' : ''}`}
					on:click={() => void submitFeedback('good')}
					disabled={feedbackBusy}
				>
					Good
				</button>
				<button
					type="button"
					class={`feedbackBtn ${feedbackVerdict === 'bad' ? 'active' : ''}`}
					on:click={() => {
						feedbackShowCorrection = true;
						feedbackVerdict = 'bad';
					}}
					disabled={feedbackBusy}
				>
					Bad
				</button>
			</div>
			{#if feedbackShowCorrection}
				<div class="feedbackEdit">
					<textarea
						class="feedbackTextarea"
						rows="3"
						bind:value={feedbackCorrection}
						placeholder="What should this explanation say instead?"
					></textarea>
					<div class="feedbackActions">
						<button
							type="button"
							class="feedbackBtn"
							on:click={() => void submitFeedback('bad')}
							disabled={feedbackBusy}
						>
							{feedbackBusy ? 'Submitting...' : 'Submit correction'}
						</button>
					</div>
				</div>
			{/if}
			{#if feedbackStatus}
				<div class="meta">{feedbackStatus}</div>
			{/if}
		{/if}
		{#if expanded && planesExpansionEnabled}
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
		pointer-events: auto;
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

	.feedbackRow {
		display: flex;
		gap: 6px;
		margin-top: 8px;
	}

	.feedbackBtn {
		font-size: 10px;
		padding: 2px 8px;
		border-radius: 999px;
		border: 1px solid #3a4f7b;
		background: #101a2c;
		color: #dbe7ff;
		cursor: pointer;
	}

	.feedbackBtn.active {
		border-color: #6aa0ff;
		background: #163057;
	}

	.feedbackBtn:disabled {
		opacity: 0.55;
		cursor: default;
	}

	.feedbackEdit {
		margin-top: 6px;
		display: grid;
		gap: 6px;
	}

	.feedbackTextarea {
		width: 100%;
		box-sizing: border-box;
		font-size: 11px;
		border-radius: 8px;
		border: 1px solid #3b4f78;
		background: #0c1629;
		color: #dce6ff;
		padding: 6px;
		resize: vertical;
	}

	.feedbackActions {
		display: flex;
		justify-content: flex-end;
	}
</style>
