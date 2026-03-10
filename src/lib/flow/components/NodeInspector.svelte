<script lang="ts">
	// lib/flow/components/NodeInspector.svelte
	import { SourceEditorByKind } from '$lib/flow/components/editors/SourceEditor/SourceEditor';
	import { LlmEditorByKind } from '$lib/flow/components/editors/LlmEditor/LlmEditor'; // <-- your new registry
	import { TransformEditorByKind } from '$lib/flow/components/editors/TransformEditor/TransformEditor';
	import { ToolEditorByProvider } from '$lib/flow/components/editors/ToolEditor/ToolEditor';
	import ToolEditor from '$lib/flow/components/editors/ToolEditor/ToolEditor.svelte';
	import ComponentEditor from '$lib/flow/components/editors/ComponentEditor/ComponentEditor.svelte';
	import { getArtifactMetaUrl } from '$lib/flow/client/runs';
	import { parseInputSchemaView, type InputSchemaView } from '$lib/flow/components/editors/TransformEditor/inputSchema';
	import { buildTransformSchemaProps } from '$lib/flow/components/editors/TransformEditor/schemaPropagation';

	import type { PipelineNodeData } from '$lib/flow/types';
	import {
		graphStore,
		__buildNodeSchemaContractSnapshotForTest,
		type NodeSchemaContractEdge
	} from '$lib/flow/store/graphStore';

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
	$: isComponent = kind === 'component';

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
	$: schemaProps = buildTransformSchemaProps(transformKind as TransformKind, inputSchemas);
	$: schemaContract = selectedNode
		? __buildNodeSchemaContractSnapshotForTest($graphStore as any, selectedNode.id)
		: { nodeId: '', status: 'clean', edges: [] as NodeSchemaContractEdge[] };

	let inputSchemas: InputSchemaView[] = [];
	let inputSchemaReqSeq = 0;
	let lastInputSignature = '';
	type SchemaAssistState = 'fresh' | 'partial' | 'stale' | 'unknown';
	type SchemaAssistSummary = {
		state: SchemaAssistState;
		source: string;
		hasSchema: boolean;
	};

	function artifactIdFromBinding(binding: any): string {
		return String(
			binding?.current?.artifactId ??
				binding?.currentArtifactId ??
				binding?.last?.artifactId ??
				binding?.lastArtifactId ??
				''
		);
	}

	function typedSchemaPayloadFromNode(node: Record<string, any> | undefined): Record<string, unknown> | null {
		const observation =
			(node?.data as any)?.schema?.observedSchema ??
			(node?.data as any)?.schema?.inferredSchema ??
			null;
		const typed = observation?.typedSchema;
		const typedType = String(typed?.type ?? '').trim().toLowerCase();
		const source = String(observation?.source ?? 'unknown');
		const state = String(observation?.state ?? 'unknown');
		if (typedType === 'table') {
			const fields = Array.isArray(typed?.fields) ? typed.fields : [];
			const columns = fields
				.map((field: Record<string, unknown>) => ({
					name: String(field?.name ?? '').trim(),
					type: String(field?.type ?? 'unknown').trim() || 'unknown'
				}))
				.filter((col: { name: string }) => col.name.length > 0);
			return {
				type: 'table',
				schema: {
					contract: 'TABLE_V1',
					source,
					state,
					table: { columns }
				}
			};
		}
		if (typedType === 'text') return { type: 'text', source, state };
		if (typedType === 'json') return { type: 'json', source, state };
		if (typedType === 'binary') return { type: 'binary', source, state };
		if (typedType === 'embeddings') return { type: 'embeddings', source, state };
		return null;
	}

	function schemaAssistStateRank(state: string): number {
		const normalized = String(state ?? 'unknown').toLowerCase();
		if (normalized === 'stale') return 3;
		if (normalized === 'partial') return 2;
		if (normalized === 'fresh') return 1;
		return 0;
	}

	function summarizeSchemaAssist(inputSchemasRaw: InputSchemaView[]): SchemaAssistSummary {
		const inputSchemas = Array.isArray(inputSchemasRaw) ? inputSchemasRaw : [];
		if (inputSchemas.length === 0) {
			return { state: 'unknown', source: 'unknown', hasSchema: false };
		}
		let topState: SchemaAssistState = 'unknown';
		let source = 'unknown';
		for (const view of inputSchemas) {
			const candidateState = String(view?.schemaState ?? 'unknown').toLowerCase();
			if (schemaAssistStateRank(candidateState) > schemaAssistStateRank(topState)) {
				topState = candidateState as SchemaAssistState;
			}
			if (source === 'unknown') {
				const candidateSource = String(view?.schemaSource ?? 'unknown').trim().toLowerCase();
				if (candidateSource && candidateSource !== 'unknown') source = candidateSource;
			}
		}
		return { state: topState, source, hasSchema: true };
	}

	$: schemaAssist = summarizeSchemaAssist(inputSchemas);
	let expectedSchemaDraft = '';
	let expectedSchemaError = '';
	let expectedSchemaNodeId = '';

	function normalizeExpectedSchemaDraft(node: any): string {
		const typed =
			node?.data?.schema?.expectedSchema?.typedSchema ??
			node?.data?.schema?.inferredSchema?.typedSchema ??
			{ type: 'unknown', fields: [] };
		return JSON.stringify(typed, null, 2);
	}

	$: if (selectedNode?.id && selectedNode.id !== expectedSchemaNodeId) {
		expectedSchemaNodeId = selectedNode.id;
		expectedSchemaDraft = normalizeExpectedSchemaDraft(selectedNode);
		expectedSchemaError = '';
	}

	function useInferredExpectedSchema(): void {
		if (!selectedNode) return;
		const typed = (selectedNode.data as any)?.schema?.inferredSchema?.typedSchema ?? { type: 'unknown', fields: [] };
		expectedSchemaDraft = JSON.stringify(typed, null, 2);
		expectedSchemaError = '';
	}

	function clearExpectedSchema(): void {
		if (!selectedNode?.id) return;
		const result = graphStore.setNodeExpectedSchema(selectedNode.id, null);
		if (!(result as any)?.ok) {
			expectedSchemaError = String((result as any)?.error ?? 'Failed to clear expected schema');
			return;
		}
		expectedSchemaDraft = JSON.stringify({ type: 'unknown', fields: [] }, null, 2);
		expectedSchemaError = '';
	}

	function saveExpectedSchema(): void {
		if (!selectedNode?.id) return;
		try {
			const parsed = JSON.parse(expectedSchemaDraft || '{}');
			const result = graphStore.setNodeExpectedSchema(selectedNode.id, parsed);
			if (!(result as any)?.ok) {
				expectedSchemaError = String((result as any)?.error ?? 'Failed to save expected schema');
				return;
			}
			expectedSchemaError = '';
		} catch (error) {
			expectedSchemaError = String((error as Error)?.message ?? 'Expected schema must be valid JSON.');
		}
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
					const sourceNode = nodesById.get(e.source) as Record<string, any> | undefined;
					return {
						sourceNodeId: e.source,
						inputHandle: String(e.targetHandle ?? 'in'),
						label: `${String(sourceNode?.data?.label ?? e.source)}.${String(e.targetHandle ?? 'in')}`,
						artifactId,
						sourceNode
					};
				});
			if (incoming.length === 0) {
				inputSchemas = [];
				return;
			}
			const responses = await Promise.all(
				incoming.map(async (entry) => {
					const context = {
						sourceNodeId: entry.sourceNodeId,
						inputHandle: entry.inputHandle
					};
					if (entry.artifactId.length > 0) {
						try {
							const res = await fetch(getArtifactMetaUrl(entry.artifactId, graphId));
							if (!res.ok) throw new Error(`Failed to load schema for ${entry.artifactId}: ${res.status}`);
							const meta = await res.json();
							return parseInputSchemaView(
								entry.artifactId,
								entry.label,
								(meta?.schema ?? meta?.payloadSchema) as Record<string, unknown> | undefined,
								context
							);
						} catch {
							// Fallback to authoring-time inferred/observed schema below.
						}
					}
					const fallbackPayload = typedSchemaPayloadFromNode(entry.sourceNode);
					if (!fallbackPayload) return null;
					return parseInputSchemaView(
						entry.artifactId || `schema:${entry.sourceNodeId}`,
						entry.label,
						fallbackPayload,
						context
					);
				})
			);
			if (reqId !== inputSchemaReqSeq) return;
			inputSchemas = responses.filter(Boolean) as InputSchemaView[];
		} catch {
			if (reqId !== inputSchemaReqSeq) return;
			inputSchemas = [];
		}
	}

	function onDraft(
		patch: Record<string, any>,
		opts?: { intent?: 'user_edit' | 'system_canonicalize'; notice?: string | null }
	) {
		graphStore.patchInspectorDraft(patch, opts);
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

	function schemaTypeLabel(schema: Record<string, any> | undefined): string {
		return String(schema?.type ?? 'unknown');
	}

	function schemaFieldSummary(schema: Record<string, any> | undefined, key: 'fields' | 'required_fields'): string {
		const fields = Array.isArray(schema?.[key]) ? (schema?.[key] as Array<Record<string, unknown>>) : [];
		if (fields.length === 0) return '-';
		return fields
			.map((field) => {
				const name = String(field?.name ?? '').trim();
				const type = String(field?.type ?? 'unknown').trim();
				return name.length > 0 ? `${name}:${type}` : '';
			})
			.filter((value) => value.length > 0)
			.join(', ');
	}

	function applySchemaSuggestion(edge: NodeSchemaContractEdge): void {
		if (!edge?.adapterKind) return;
		graphStore.deleteEdge(edge.edgeId);
		graphStore.insertSchemaAdapterForEdgeConnection({
			source: edge.sourceNodeId,
			target: edge.targetNodeId,
			sourceHandle: edge.sourceHandle,
			targetHandle: edge.targetHandle,
			adapterKind: edge.adapterKind
		});
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
		<svelte:component
			this={ToolEditorByProvider[toolProvider] ?? ToolEditorByProvider.mcp}
			{params}
			{onDraft}
			{onCommit}
		/>
	{:else if isComponent}
		<ComponentEditor {selectedNode} {params} {onDraft} />
		{:else if isTransform}
			<div class={`schemaAssist schemaAssist-${schemaAssist.state}`}>
				<div class="schemaAssistHead">
					<span>Schema Assist</span>
					<span class="schemaAssistBadge">{schemaAssist.source}/{schemaAssist.state}</span>
				</div>
				{#if !schemaAssist.hasSchema}
					<div class="schemaAssistHint">
						No inferred input schema yet. Run upstream once or declare expected schema to unlock field-aware controls.
					</div>
				{/if}
			</div>
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
					inputColumns={schemaProps.inputColumns}
					inputSchemaColumns={schemaProps.inputSchemaColumns}
					inputSchemas={schemaProps.inputSchemas}
					{onDraft}
					{onCommit}
				/>
			{/if}
		{/if}
		{#if !isComponent}
			<div class="expectedSchemaEditor">
				<div class="expectedSchemaHead">Expected Output Schema</div>
				<textarea
					class="expectedSchemaTextarea"
					rows="7"
					bind:value={expectedSchemaDraft}
					spellcheck="false"
				/>
				<div class="expectedSchemaActions">
					<button type="button" on:click={saveExpectedSchema}>Save expected</button>
					<button type="button" on:click={useInferredExpectedSchema}>Use inferred</button>
					<button type="button" on:click={clearExpectedSchema}>Clear</button>
				</div>
				{#if expectedSchemaError}
					<div class="expectedSchemaError">{expectedSchemaError}</div>
				{/if}
			</div>
		{/if}
		<div class={`schemaContract schemaContract-${schemaContract.status}`}>
			<div class="schemaHead">Schema Contract</div>
			<div class="schemaStatus">Status: {schemaContract.status}</div>
			{#if schemaContract.edges.length === 0}
				<div class="schemaEmpty">No connected edges.</div>
			{:else}
				{#each schemaContract.edges as edge (edge.edgeId)}
					<div class={`schemaEdge schemaEdge-${edge.severity}`}>
						<div class="schemaEdgeHead">
							<span>{edge.direction === 'incoming' ? 'in' : 'out'}: {edge.edgeId}</span>
							<span>{edge.severity}</span>
						</div>
						<div class="schemaRow">
							<span class="schemaLabel">provided</span>
							<span>{schemaTypeLabel(edge.providedSchema)} [{schemaFieldSummary(edge.providedSchema, 'fields')}]</span>
						</div>
						<div class="schemaRow">
							<span class="schemaLabel">required</span>
							<span>{schemaTypeLabel(edge.requiredSchema)} [{schemaFieldSummary(edge.requiredSchema, 'required_fields')}]</span>
						</div>
						{#if edge.suggestions.length > 0}
							<div class="schemaSuggestions">{edge.suggestions.join(' ')}</div>
						{/if}
						{#if edge.adapterKind}
							<button type="button" class="schemaApplyBtn" on:click={() => applySchemaSuggestion(edge)}>
								Apply {edge.adapterKind} adapter
							</button>
						{/if}
					</div>
				{/each}
			{/if}
		</div>
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

	.schemaAssist {
		margin-bottom: 8px;
		border: 1px solid var(--ni-border);
		border-radius: 10px;
		padding: 8px;
		background: var(--ni-card);
		display: grid;
		gap: 4px;
	}

	.schemaAssist-fresh {
		border-color: #22c55e;
	}

	.schemaAssist-partial {
		border-color: #f59e0b;
	}

	.schemaAssist-stale {
		border-color: #ef4444;
	}

	.schemaAssistHead {
		display: flex;
		align-items: center;
		justify-content: space-between;
		font-size: 12px;
		font-weight: 600;
	}

	.schemaAssistBadge {
		font-size: 11px;
		padding: 2px 7px;
		border: 1px solid var(--ni-control-border);
		border-radius: 999px;
		color: var(--ni-muted);
	}

	.schemaAssistHint {
		font-size: 11px;
		color: var(--ni-muted);
	}

	.expectedSchemaEditor {
		margin-top: 8px;
		border: 1px solid var(--ni-border);
		border-radius: 10px;
		padding: 8px;
		background: var(--ni-card);
		display: grid;
		gap: 6px;
	}

	.expectedSchemaHead {
		font-size: 12px;
		font-weight: 600;
	}

	.expectedSchemaTextarea {
		width: 100%;
		min-height: 112px;
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New',
			monospace;
	}

	.expectedSchemaActions {
		display: flex;
		gap: 8px;
	}

	.expectedSchemaActions button {
		font-size: 11px;
		padding: 4px 8px;
	}

	.expectedSchemaError {
		font-size: 11px;
		color: var(--ni-error-text);
	}

	.schemaContract {
		margin-top: 10px;
		border: 1px solid var(--ni-border);
		border-radius: 10px;
		padding: 8px;
		background: var(--ni-card);
		display: grid;
		gap: 6px;
	}

	.schemaContract-warning {
		border-color: #f59e0b;
	}

	.schemaContract-error {
		border-color: #ef4444;
	}

	.schemaHead {
		font-size: 12px;
		font-weight: 700;
	}

	.schemaStatus,
	.schemaEmpty,
	.schemaSuggestions {
		font-size: 11px;
		opacity: 0.86;
	}

	.schemaEdge {
		border: 1px solid var(--ni-border);
		border-radius: 8px;
		padding: 6px;
		display: grid;
		gap: 4px;
	}

	.schemaEdge-warning {
		border-color: #f59e0b;
	}

	.schemaEdge-error {
		border-color: #ef4444;
	}

	.schemaEdgeHead {
		display: flex;
		justify-content: space-between;
		font-size: 11px;
		font-weight: 600;
	}

	.schemaRow {
		display: grid;
		grid-template-columns: auto 1fr;
		gap: 6px;
		font-size: 11px;
	}

	.schemaLabel {
		color: var(--ni-muted);
	}

	.schemaApplyBtn {
		justify-self: start;
		font-size: 11px;
		padding: 4px 8px;
	}
</style>
