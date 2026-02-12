<script lang="ts">
	// lib/flow/components/NodeInspector.svelte
	import { SourceEditorByKind } from '$lib/flow/components/editors/SourceEditor/SourceEditor';
	import { LlmEditorByKind } from '$lib/flow/components/editors/LlmEditor/LlmEditor'; // <-- your new registry

	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import { graphStore } from '$lib/flow/store/graphStore';

	import PortsEditor from '$lib/flow/components/PortsEditor.svelte';
	import { selectedNode as selectedNodeStore } from '$lib/flow/store/graphStore';

	import type { SourceKind } from '$lib/flow/types/paramsMap';
	import type { LlmKind } from '$lib/flow/types/paramsMap'; // adjust path if yours differs

	$: selectedNode = $selectedNodeStore;

	// kind discriminators
	$: kind = selectedNode?.data?.kind as PipelineNodeData['kind'] | undefined;
	$: isSource = kind === 'source';
	$: isLlm = kind === 'llm';
	$: isTool = kind === 'tool';
	$: isTransform = kind === 'transform';

	// inspector draft params (single source of truth for editors)
	$: params = $graphStore.inspector?.draftParams ?? {};

	// sub-kinds / kinds
	$: sourceKind = (selectedNode?.data as any)?.sourceKind ?? 'file';

	// LLM kind: prefer params.kind (because your FE schema stores it there)
	// fallback if you later promote it to node.data.llmKind
	$: llmKind =
		((params as any)?.kind ??
			(selectedNode?.data as any)?.kind ??
			(selectedNode?.data as any)?.llmKind ??
			'ollama') as LlmKind;

	$: console.log('NodeInspector selectedNode kind:', kind);
	$: console.log('NodeInspector selectedNode sourceKind:', (selectedNode?.data as any)?.sourceKind);
	$: console.log('NodeInspector llmKind:', llmKind);

	function onApplyJson(parsed: unknown) {
		if (!selectedNode) return;
		graphStore.updateNodeConfig(selectedNode.id, { params: parsed });
	}

	function onDraft(patch: Record<string, any>) {
		graphStore.patchInspectorDraft(patch);
	}

	function onCommit(patch: Record<string, any>) {
		graphStore.commitInspectorImmediate(patch);
	}

	function setSourceKind(next: SourceKind) {
		if (!selectedNode) return;
		graphStore.setSourceKind(selectedNode.id, next);
	}

	/**
	 * For LLM: we keep kind inside params (matches your schema/llm.ts).
	 * This avoids needing a new node-level discriminator right now.
	 */
	function setLlmKind(next: LlmKind) {
		if (!selectedNode) return;
		onCommit({ kind: next }); // immediate commit so editor swaps cleanly
	}
</script>

{#if selectedNode}
	<PortsEditor {selectedNode} />

	{#if isSource}
		<!-- SOURCE -->
		<div class="section">
			<div class="sectionTitle">Source</div>
			<div class="field">
				<div class="k">source kind</div>
				<div class="v">
					<select
						value={sourceKind}
						on:change={(e) => {
							const nextKind = (e.currentTarget as HTMLSelectElement).value as SourceKind;
							graphStore.setSourceKind(selectedNode.id, nextKind);
						}}
					>
						<option value="file">file</option>
						<option value="database">database</option>
						<option value="api">api</option>
					</select>
				</div>
			</div>
		</div>

		<svelte:component
			this={SourceEditorByKind[sourceKind] ?? SourceEditorByKind.file}
			{selectedNode}
			{params}
			{onDraft}
			{onCommit}
		/>
	{:else if isLlm}
		<!-- LLM -->
		<div class="section">
			<div class="sectionTitle">LLM</div>

			<div class="field">
				<div class="k">llm kind</div>
				<div class="v">
					<select
						value={llmKind}
						on:change={(e) => {
							const nextKind = (e.currentTarget as HTMLSelectElement).value as LlmKind;
							graphStore.setLlmKind(selectedNode.id,nextKind);
						}}
					>
						<option selected value="ollama">ollama</option>
						<option value="openai_compat">openai_compat</option>
					</select>
				</div>
			</div>
		</div>

		<svelte:component
			this={LlmEditorByKind[llmKind] ?? LlmEditorByKind.ollama}
			{selectedNode}
			{params}
			{onDraft}
			{onCommit}
		/>
	{:else if isTool}
		<!-- TOOL (placeholder) -->
		<div class="section">
			<div class="sectionTitle">Tool</div>
			<div class="field">
				<div class="k">status</div>
				<div class="v">Tool editor not wired yet.</div>
			</div>
		</div>
	{:else if isTransform}
		<!-- TRANSFORM (placeholder) -->
		<div class="section">
			<div class="sectionTitle">Transform</div>
			<div class="field">
				<div class="k">status</div>
				<div class="v">Transform editor not wired yet.</div>
			</div>
		</div>
	{/if}
{/if}
