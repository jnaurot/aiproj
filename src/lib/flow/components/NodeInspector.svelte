<script lang="ts">
	// lib/flow/components/NodeInspector.svelte
	import { SourceEditorByKind } from '$lib/flow/components/editors/SourceEditor/SourceEditor';
	import { LlmEditorByKind } from '$lib/flow/components/editors/LlmEditor/LlmEditor'; // <-- your new registry
	import { TransformEditorByKind } from '$lib/flow/components/editors/TransformEditor/TransformEditor';
	import ToolEditor from '$lib/flow/components/editors/ToolEditor/ToolEditor.svelte';

	import type { PipelineNodeData } from '$lib/flow/types';
	import { graphStore } from '$lib/flow/store/graphStore';

	import { selectedNode as selectedNodeStore } from '$lib/flow/store/graphStore';

	import type { SourceKind, LlmKind, TransformKind, ToolProvider } from '$lib/flow/types/paramsMap';
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

	// LLM kind source of truth: node discriminator (optionally draft override), never node.data.kind.
	$: llmKind = (((params as any)?.llmKind ?? (selectedNode?.data as any)?.llmKind ?? 'ollama') as LlmKind);

	$: transformKind = (selectedNode?.data as any)?.transformKind ?? 'select';
	$: toolProvider = ((params as any)?.provider ??
		(selectedNode?.data as any)?.params?.provider ??
		'mcp') as ToolProvider;
	let configError: string | null = null;

	function onDraft(patch: Record<string, any>) {
		graphStore.patchInspectorDraft(patch);
	}

	function onCommit(patch: Record<string, any>) {
		graphStore.commitInspectorImmediate(patch);
	}
</script>

{#if selectedNode}
	<div class="nodeInspectorTheme">
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
		<!-- TOOL -->
		<div class="section">
			<div class="sectionTitle">Tool</div>
			<div class="field">
				<div class="k">provider</div>
				<div class="v">
					<select
						value={toolProvider}
						on:change={(e) => {
							const nextProvider = (e.currentTarget as HTMLSelectElement).value as ToolProvider;
							graphStore.setToolProvider(selectedNode.id, nextProvider);
						}}
					>
						<option value="mcp">mcp</option>
						<option value="http">http</option>
						<option value="function">function</option>
						<option value="python">python</option>
						<option value="js">js</option>
						<option value="shell">shell</option>
						<option value="db">db</option>
						<option value="builtin">builtin</option>
					</select>
				</div>
			</div>
		</div>
		<ToolEditor {selectedNode} {params} {onDraft} {onCommit} />
	{:else if isTransform}
		<div class="section">
			<div class="field">
				<div class="k">transform op</div>
				<div class="v">
					<select
						value={transformKind}
						on:change={(e) => {
							const nextKind = (e.currentTarget as HTMLSelectElement).value as TransformKind;
							const result = graphStore.setTransformKind(selectedNode.id, nextKind);
							if (!result.ok) {
								configError = result.error ?? 'Failed to update transform op';
							} else {
								configError = null;
							}
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

	{#if configError}
		<div class="configError">{configError}</div>
	{/if}
	</div>
{/if}

<style>
	.nodeInspectorTheme {
		--ni-bg: #f7f9fc;
		--ni-card: #ffffff;
		--ni-border: #d7deea;
		--ni-text: #1f2937;
		--ni-muted: #5b6677;
		--ni-control-bg: #ffffff;
		--ni-control-text: #1f2937;
		--ni-control-border: #b9c5da;
		--ni-error-bg: #fee2e2;
		--ni-error-border: #fca5a5;
		--ni-error-text: #7f1d1d;
		color: var(--ni-text);
	}

	@media (prefers-color-scheme: dark) {
		.nodeInspectorTheme {
			--ni-bg: #0b0f17;
			--ni-card: #0f1724;
			--ni-border: #253049;
			--ni-text: #e5e7eb;
			--ni-muted: #9aa3b2;
			--ni-control-bg: #0b1220;
			--ni-control-text: #e5e7eb;
			--ni-control-border: #2c3b59;
			--ni-error-bg: rgba(239, 68, 68, 0.12);
			--ni-error-border: rgba(239, 68, 68, 0.45);
			--ni-error-text: #fecaca;
		}
	}

	:global(.nodeInspectorTheme .section) {
		background: var(--ni-card);
		border: 1px solid var(--ni-border);
		border-radius: 10px;
		padding: 8px 10px;
	}

	:global(.nodeInspectorTheme .sectionTitle) {
		color: var(--ni-text);
	}

	:global(.nodeInspectorTheme .k) {
		color: var(--ni-muted);
	}

	:global(.nodeInspectorTheme .v input),
	:global(.nodeInspectorTheme .v select),
	:global(.nodeInspectorTheme .v textarea) {
		background: var(--ni-control-bg);
		color: var(--ni-control-text);
		border: 1px solid var(--ni-control-border);
	}

	:global(.nodeInspectorTheme .v select option) {
		background: var(--ni-control-bg);
		color: var(--ni-control-text);
	}

	.configError {
		margin-top: 8px;
		padding: 8px 10px;
		border-radius: 8px;
		border: 1px solid var(--ni-error-border);
		background: var(--ni-error-bg);
		color: var(--ni-error-text);
		font-size: 12px;
	}
</style>
