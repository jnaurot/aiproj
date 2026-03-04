<script lang="ts">
	// lib/flow/components/NodeInspector.svelte
	import { SourceEditorByKind } from '$lib/flow/components/editors/SourceEditor/SourceEditor';
	import { LlmEditorByKind } from '$lib/flow/components/editors/LlmEditor/LlmEditor'; // <-- your new registry
	import { TransformEditorByKind } from '$lib/flow/components/editors/TransformEditor/TransformEditor';
	import ToolEditor from '$lib/flow/components/editors/ToolEditor/ToolEditor.svelte';
	import { getArtifactMetaUrl } from '$lib/flow/client/runs';
	import { parseInputSchemaView, type InputSchemaView } from '$lib/flow/components/editors/TransformEditor/inputSchema';

	import type { PipelineNodeData } from '$lib/flow/types';
	import { graphStore } from '$lib/flow/store/graphStore';

	import { selectedNode as selectedNodeStore } from '$lib/flow/store/graphStore';

	import type { LlmKind, TransformKind, ToolProvider } from '$lib/flow/types/paramsMap';
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
	$: nodeError = selectedNode ? ($graphStore.nodeOutputs?.[selectedNode.id]?.lastError ?? null) : null;

	// sub-kinds / kinds
	$: sourceKind = (selectedNode?.data as any)?.sourceKind ?? 'file';

	// LLM kind source of truth: node discriminator (optionally draft override), never node.data.kind.
	$: llmKind = (((params as any)?.llmKind ?? (selectedNode?.data as any)?.llmKind ?? 'ollama') as LlmKind);

	$: transformKind = (selectedNode?.data as any)?.transformKind ?? 'select';
	$: toolProvider = ((params as any)?.provider ??
		(selectedNode?.data as any)?.params?.provider ??
		'mcp') as ToolProvider;
	$: splitInputColumns = Array.from(
		new Set(inputSchemas.flatMap((schema) => schema.columns.map((c) => String(c.name || ''))).filter(Boolean))
	);

	let inputSchemas: InputSchemaView[] = [];
	let inputSchemaReqSeq = 0;
	let lastInputSignature = '';

	function artifactIdFromBinding(binding: any): string {
		return String(
			binding?.current?.artifactId ??
				binding?.currentArtifactId ??
				binding?.last?.artifactId ??
				binding?.lastArtifactId ??
				''
		);
	}

	$: if (selectedNode?.id && isTransform) {
		const nodeId = selectedNode.id;
		const edges = $graphStore?.edges ?? [];
		const nodeBindings = $graphStore?.nodeBindings ?? {};
		const incoming = edges
			.filter((e) => e.target === nodeId)
			.map((e) => {
				const sourceBinding = nodeBindings[e.source];
				const artifactId = artifactIdFromBinding(sourceBinding);
				return `${String(e.id ?? '')}:${e.source}:${String(e.targetHandle ?? 'in')}:${artifactId}`;
			})
			.sort();
		const signature = `${String($graphStore?.graphId ?? '')}|${nodeId}|${incoming.join('|')}`;
		if (signature !== lastInputSignature) {
			lastInputSignature = signature;
			void refreshInputSchemas();
		}
	} else {
		lastInputSignature = '';
		inputSchemas = [];
	}

	async function refreshInputSchemas(): Promise<void> {
		const nodeId = selectedNode?.id;
		if (!nodeId) {
			inputSchemas = [];
			return;
		}
		const reqId = ++inputSchemaReqSeq;
		try {
			const edges = $graphStore?.edges ?? [];
			const nodeBindings = $graphStore?.nodeBindings ?? {};
			const nodesById = new Map(($graphStore?.nodes ?? []).map((n) => [n.id, n]));
			const graphId = String($graphStore?.graphId ?? '').trim();
			if (!graphId) {
				inputSchemas = [];
				return;
			}
				const incoming = edges
					.filter((e) => e.target === nodeId)
					.map((e) => {
						const sourceBinding = nodeBindings[e.source];
						const artifactId = artifactIdFromBinding(sourceBinding);
						return {
							sourceNodeId: e.source,
							inputHandle: String(e.targetHandle ?? 'in'),
							label: `${String(nodesById.get(e.source)?.data?.label ?? e.source)}.${String(e.targetHandle ?? 'in')}`,
							artifactId
						};
					})
				.filter((x) => x.artifactId.length > 0);
			if (incoming.length === 0) {
				inputSchemas = [];
				return;
			}
			const responses = await Promise.all(
				incoming.map(async (entry) => {
					const res = await fetch(getArtifactMetaUrl(entry.artifactId, graphId));
					if (!res.ok) throw new Error(`Failed to load schema for ${entry.artifactId}: ${res.status}`);
					const meta = await res.json();
						return parseInputSchemaView(
							entry.artifactId,
							entry.label,
							(meta?.schema ?? meta?.payloadSchema) as Record<string, unknown> | undefined,
							{
								sourceNodeId: entry.sourceNodeId,
								inputHandle: entry.inputHandle
							}
						);
					})
				);
			if (reqId !== inputSchemaReqSeq) return;
			inputSchemas = responses;
		} catch {
			if (reqId !== inputSchemaReqSeq) return;
			inputSchemas = [];
		}
	}

	function onDraft(patch: Record<string, any>) {
		graphStore.patchInspectorDraft(patch);
	}

	function onCommit(patch: Record<string, any>) {
		graphStore.commitInspectorImmediate(patch);
	}

	function toJoinPatch(patch: Record<string, any>): Record<string, any> {
		const next = patch && typeof patch === 'object' ? patch : {};
		if ('join' in next || 'op' in next) {
			return { op: 'join', ...next };
		}
		return { op: 'join', join: next };
	}

	function onJoinDraft(patch: Record<string, any>) {
		onDraft(toJoinPatch(patch));
	}

	function onJoinCommit(patch: Record<string, any>) {
		onCommit(toJoinPatch(patch));
	}
</script>

{#if selectedNode}
	<div class="nodeInspectorTheme">
	{#if isSource}
		<svelte:component
			this={SourceEditorByKind[sourceKind] ?? SourceEditorByKind.file}
			{selectedNode}
			{params}
			{onDraft}
			{onCommit}
		/>
	{:else if isLlm}
		<svelte:component
			this={LlmEditorByKind[llmKind] ?? LlmEditorByKind.ollama}
			{selectedNode}
			{params}
			{onDraft}
			{onCommit}
		/>
	{:else if isTool}
		<ToolEditor {selectedNode} {params} {onDraft} {onCommit} />
		{:else if isTransform}
			{#if transformKind === 'join'}
				<svelte:component
					this={TransformEditorByKind[transformKind] ?? TransformEditorByKind.filter}
					{selectedNode}
					{params}
					{nodeError}
					{inputSchemas}
					onDraft={onJoinDraft}
					onCommit={onJoinCommit}
				/>
			{:else}
				<svelte:component
					this={TransformEditorByKind[transformKind] ?? TransformEditorByKind.filter}
					{selectedNode}
					{params}
					{nodeError}
					inputColumns={splitInputColumns}
					{onDraft}
					{onCommit}
				/>
			{/if}
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
</style>
