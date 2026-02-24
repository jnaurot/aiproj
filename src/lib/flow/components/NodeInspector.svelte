<script lang="ts">
	// lib/flow/components/NodeInspector.svelte
	import { SourceEditorByKind } from '$lib/flow/components/editors/SourceEditor/SourceEditor';
	import { LlmEditorByKind } from '$lib/flow/components/editors/LlmEditor/LlmEditor'; // <-- your new registry
	import { TransformEditorByKind } from '$lib/flow/components/editors/TransformEditor/TransformEditor';

	import type { PipelineNodeData } from '$lib/flow/types';
	import { graphStore } from '$lib/flow/store/graphStore';

	import { selectedNode as selectedNodeStore } from '$lib/flow/store/graphStore';

	import type { SourceKind, LlmKind, TransformKind } from '$lib/flow/types/paramsMap';
	// import type { LlmKind } from '$lib/flow/types/paramsMap'; // adjust path if yours differs
	// import type { TransformKind } from '$lib/flow/types/paramsMap';

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
	$: llmKind = ((params as any)?.kind ??
		(selectedNode?.data as any)?.kind ??
		(selectedNode?.data as any)?.llmKind ??
		'ollama') as LlmKind;

	$: transformKind = (selectedNode?.data as any)?.transformKind ?? 'select';

	function onDraft(patch: Record<string, any>) {
		graphStore.patchInspectorDraft(patch);
	}

	function onCommit(patch: Record<string, any>) {
		graphStore.commitInspectorImmediate(patch);
	}

</script>

{#if selectedNode}
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
							graphStore.setLlmKind(selectedNode.id, nextKind);
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
		<div class="section">
			<div class="field">
				<div class="k">transform op</div>
				<div class="v">
					<select
						value={transformKind}
						on:change={(e) => {
							const nextKind = (e.currentTarget as HTMLSelectElement).value as TransformKind;
							graphStore.setTransformKind(selectedNode.id, nextKind);
						}}
					>
						<option value="filter">filter</option>
						<option value="select">select</option>
						<option value="rename">rename</option>
						<option value="derive">derive</option>
						<option value="aggregate">aggregate</option>
						<option value="join">join</option>
						<option value="sort">sort</option>
						<option value="limit">limit</option>
						<option value="dedupe">dedupe</option>
						<option value="sql">sql</option>
						<option value="python">python</option>
						<option value="js">js</option>
					</select>
				</div>
			</div>
		</div>
		<svelte:component
			this={TransformEditorByKind[transformKind] ?? TransformEditorByKind.filter}
			{selectedNode}
			{params}
			{onDraft}
			{onCommit}
		/>
	{/if}
{/if}
