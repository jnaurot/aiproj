<script lang="ts">
	import { onDestroy, tick } from 'svelte';
	import { get } from 'svelte/store';
	import { SvelteFlow, Background, Controls, MarkerType, useSvelteFlow } from '@xyflow/svelte';
	import '@xyflow/svelte/dist/style.css';

	import type { Node, Edge, Connection } from '@xyflow/svelte';

	import { nodeTypes } from '$lib/flow/nodeTypes';
	import type { PipelineNodeData, PipelineEdgeData, NodeKind, PortType } from '$lib/flow/types'; //porttype actually in base
	import type { SourceKind, LlmKind, TransformKind, ToolProvider } from '$lib/flow/types/paramsMap';
	import { graphStore, selectedNode } from '$lib/flow/store/graphStore';
	import type { InputResolution } from '$lib/flow/store/graphStore';
	import NodeInspector from '$lib/flow/components/NodeInspector.svelte';
	import PortsEditor from '$lib/flow/components/PortsEditor.svelte';
	import OutputModal from '$lib/flow/components/OutputModal.svelte';
	import ArtifactViewer from './components/ArtifactViewer.svelte';
	import { getHeaderCachePill, getHeaderNodeStatus } from './components/inspectorCachePill';
	import { getArtifactMetaUrl } from '$lib/flow/client/runs';
	import { TransformEditorCommitModeByKind } from '$lib/flow/components/editors/TransformEditor/TransformEditor';

	const { screenToFlowPosition, setCenter, getViewport } = useSvelteFlow();

	let outputOpen = false;
	let outputNodeId: string | null = null;

	// local bind state (SvelteFlow requires bind)
	let nodes: Node<PipelineNodeData>[] = [];
	let edges: Edge<PipelineEdgeData>[] = [];

	let scrollElement: HTMLDivElement;
let inspectorPane: HTMLElement | null = null; // HTMLAsideElement type often isn't in TS DOM lib

	// Guard: when we apply store -> local, we don't want to sync right back.
	let applyingFromStore = false;

	let lastStoreNodes: Node<PipelineNodeData>[] | null = null;
	let lastStoreEdges: Edge<PipelineEdgeData>[] | null = null;
	let lastSelectedNodeId: string | null = null;

	//editing stuff
	let isEditingTitle = false;
	let titleDraft = '';
	let titleBeforeEdit = '';

	function beginEditTitle() {
		if (!$selectedNode) return;
		isEditingTitle = true;
		titleDraft = $selectedNode.data.label ?? '';
		titleBeforeEdit = titleDraft;
		tick().then(() => {
			const el = document.getElementById('node-title-input') as HTMLInputElement | null;
			el?.focus();
			el?.select();
		});
	}

	function commitEditTitle() {
		isEditingTitle = false;
		updateSelectedTitle(titleDraft);
	}

	function cancelEditTitle() {
		isEditingTitle = false;
		titleDraft = titleBeforeEdit;
		updateSelectedTitle(titleBeforeEdit);
	}
	//end editing stuff
	$: if ($graphStore.logs && scrollElement) {
		scrollToBottom();
	}

	$: displayEdges = edges.map((e) => ({
		...e,
		class: `edge edge-${e.data?.exec ?? 'idle'}`
	}));

	function applyCanvasSelection(
		seedNodes: Node<PipelineNodeData>[],
		selectedNodeId: string | null
	): Node<PipelineNodeData>[] {
		return seedNodes.map((n) => ({ ...n, selected: !!selectedNodeId && n.id === selectedNodeId }));
	}

	$: {
		const s = $graphStore;

		// Only apply store -> local when the STORE references change,
		// not when the CANVAS changes (like while dragging).
		const storeNodesChanged = s.nodes !== lastStoreNodes;
		const storeEdgesChanged = s.edges !== lastStoreEdges;
		const storeSelectionChanged = s.selectedNodeId !== lastSelectedNodeId;

		if (storeNodesChanged || storeEdgesChanged || storeSelectionChanged) {
			applyingFromStore = true;

			if (storeNodesChanged || storeSelectionChanged) {
				nodes = applyCanvasSelection(s.nodes, s.selectedNodeId);
				lastStoreNodes = s.nodes;
				lastSelectedNodeId = s.selectedNodeId;
			}

			if (storeEdgesChanged) {
				edges = s.edges;
				lastStoreEdges = s.edges;
			}

			tick().then(() => (applyingFromStore = false));
		}
	}

	// keep draft in sync when selection changes
	$: if ($selectedNode) {
		const next = JSON.stringify(
			{
				params: $selectedNode.data.params ?? {},
				ports: {
					in: $selectedNode.data.ports?.in ?? 'table',
					out: $selectedNode.data.ports?.out ?? 'table'
				}
			},
			null,
			2
		);
	}
	//ViewArtifact
	type InspectorMode = 'edit' | 'inputs' | 'output' | 'ports';
	let inspectorMode: InspectorMode = 'edit';
	let inspectorTopRatio = 0.5;
	let resizingInspector = false;
	let subtypeError: string | null = null;
	let subtypeErrorNodeId: string | null = null;
	let subtypeErrorTimer: ReturnType<typeof setTimeout> | null = null;

	$: selectedId = $selectedNode?.id;
	$: if (subtypeError && subtypeErrorNodeId && selectedId && subtypeErrorNodeId !== selectedId) {
		subtypeError = null;
		subtypeErrorNodeId = null;
		if (subtypeErrorTimer) {
			clearTimeout(subtypeErrorTimer);
			subtypeErrorTimer = null;
		}
	}
	$: inspectorParams = ($graphStore.inspector?.draftParams ?? {}) as Record<string, unknown>;
	$: selectedLlmKind = (((inspectorParams as any)?.llmKind ?? ($selectedNode?.data as any)?.llmKind ?? 'ollama') as LlmKind);
	// $: selectedSourceKind = (($selectedNode?.data as any)?.sourceKind ?? 'file') as SourceKind;
	// $: selectedTransformKind = ((($selectedNode?.data as any)?.transformKind ?? 'select') as TransformKind);
	$: selectedSourceKind = 
		(((inspectorParams as any)?.sourceKind ?? ($selectedNode?.data as any)?.sourceKind ?? 'file') as SourceKind);
	$: selectedTransformKind =
		(((inspectorParams as any)?.transformKind ?? ($selectedNode?.data as any)?.transformKind ?? 'select') as TransformKind);
		$: selectedToolProvider = (((inspectorParams as any)?.provider ??
		($selectedNode?.data as any)?.params?.provider ??
		'mcp') as ToolProvider);
	$: hideInspectorApplyRow =
		inspectorMode === 'edit' &&
		$selectedNode?.data?.kind === 'transform' &&
		TransformEditorCommitModeByKind[selectedTransformKind] === 'immediate';
	$: nodeBinding = selectedId ? $graphStore.nodeBindings?.[selectedId] : undefined;
	$: nodeOut = selectedId ? $graphStore.nodeOutputs?.[selectedId] : undefined;
	$: nodeError = (nodeOut as any)?.lastError ?? null;
	$: hasInputs = Boolean($selectedNode && $selectedNode.data?.ports?.in !== null && $selectedNode.data?.ports?.in !== undefined);
	$: inputResolutions = selectedId ? graphStore.resolveNodeInputs(selectedId) : [];
	$: if (inspectorMode === 'inputs' && !hasInputs) inspectorMode = 'edit';
	$: activeArtifactId =
		nodeBinding?.current?.artifactId ??
		nodeBinding?.currentArtifactId ??
		nodeBinding?.last?.artifactId ??
		nodeBinding?.lastArtifactId;
	$: hasOutput = !!activeArtifactId;
	$: displayNodeStatus = getHeaderNodeStatus(nodeBinding as any);
	$: headerCachePill = getHeaderCachePill(nodeOut, nodeBinding as any, displayNodeStatus);
	$: graphHeaderStatus =
		$graphStore.lastRunStatus === 'never_run'
			? 'Never run'
			: `${$graphStore.lastRunStatus === 'cancelled' ? 'Cancelled' : $graphStore.lastRunStatus.charAt(0).toUpperCase() + $graphStore.lastRunStatus.slice(1)}${$graphStore.freshness === 'stale' ? ` + Needs rerun${$graphStore.staleNodeCount > 0 ? ` (${$graphStore.staleNodeCount} stale)` : ''}` : ''}`;

	// auto-fallback if you select a node without output
	$: if (inspectorMode === 'output' && !hasOutput) inspectorMode = 'edit';
	$: if (selectedId) {
		inputMetaByArtifactId = {};
		inputPreviewArtifactId = null;
	}
	//ViewArtifact

	type InputArtifactMeta = {
		mimeType?: string;
		schemaFingerprint?: string | null;
		contract?: string;
	};

	let inputMetaByArtifactId: Record<string, InputArtifactMeta> = {};
	let inputPreviewArtifactId: string | null = null;

	function inputReasonCopy(
		reason: InputResolution['reason'] | undefined
	): string {
		if (reason === 'DISCONNECTED') return 'No upstream connection';
		if (reason === 'UPSTREAM_FAILED') return 'Upstream failed';
		if (reason === 'UPSTREAM_NO_ARTIFACT') return 'Upstream has no artifact yet';
		return 'Input unavailable';
	}

	function shortId(v: string | undefined | null, n = 8): string {
		const s = String(v ?? '');
		return s ? s.slice(0, n) : '';
	}

	function upstreamLabel(fromNodeId: string, fromPort: string): string {
		const node = $graphStore.nodes.find((n) => n.id === fromNodeId);
		const base = String(node?.data?.label ?? fromNodeId);
		return `${base}.${fromPort}`;
	}

	async function loadInputArtifactMetadata(
		graphId: string,
		resolutions: InputResolution[]
	): Promise<void> {
		const artifactIds = resolutions
			.filter((r) => r.status === 'resolved' && r.artifactId)
			.map((r) => String(r.artifactId));
		const uniqueIds = Array.from(new Set(artifactIds));
		const next: Record<string, InputArtifactMeta> = {};
		await Promise.all(
			uniqueIds.map(async (artifactId) => {
				try {
					const res = await fetch(getArtifactMetaUrl(graphId, artifactId));
					if (!res.ok) return;
					const meta = await res.json();
					next[artifactId] = {
						mimeType: meta?.mimeType,
						schemaFingerprint: meta?.schemaFingerprint ?? null,
						contract: meta?.schema?.contract ?? meta?.payloadSchema?.contract ?? meta?.portType
					};
				} catch {
					// best effort; leave metadata blank when fetch fails
				}
			})
		);
		const stillSameNode = selectedId && selectedId === $selectedNode?.id;
		if (!stillSameNode) return;
		inputMetaByArtifactId = next;
	}

	$: if (inspectorMode === 'inputs' && selectedId) {
		void loadInputArtifactMetadata($graphStore.graphId, inputResolutions);
	}

	async function scrollToBottom() {
		// Wait for Svelte to finish updating the DOM
		await tick();
		if (scrollElement) {
			scrollElement.scrollTo({
				top: scrollElement.scrollHeight,
				behavior: 'smooth' // Remove this for instant jumping
			});
		}
	}

	// ---------------------------
	// Canvas -> store sync helpers
	// ---------------------------
	function syncToStore() {
		if (applyingFromStore) return;
		graphStore.syncFromCanvas(nodes, edges);
	}

	// ---------------------------
	// UI handlers
	// ---------------------------
	// function onnodeclick({ node }: { node: Node<PipelineNodeData> }) {
	// 	graphStore.selectNode(node.id);
	// }
	// ---- dblclick detector ----
	let lastClickAt = 0;
	let lastClickNodeId: string | null = null;
	const DBL_MS = 350;
	function onnodeclick({ node }: { node: Node<PipelineNodeData> }) {
		const id = node.id;
		const now = performance.now();

		const isDbl = lastClickNodeId === id && now - lastClickAt < DBL_MS;

		lastClickAt = now;
		lastClickNodeId = id;

		// keep your current behavior
		graphStore.selectNode(id);

		// open output modal on “double click”
		if (isDbl) {
			outputNodeId = id;
			outputOpen = true;
		}
	}
	function onnodecontextmenu({ event, node }: { event: MouseEvent; node: Node<PipelineNodeData> }) {
		event.preventDefault();
		event.stopPropagation();
		if ($graphStore.runStatus === 'running') return;
		graphStore.deleteNode(node.id);
	}

	function onedgecontextmenu({ event, edge }: { event: MouseEvent; edge: Edge<PipelineEdgeData> }) {
		event.preventDefault();
		event.stopPropagation();
		if ($graphStore.runStatus === 'running') return;
		graphStore.deleteEdge(edge.id);
	}

	function addNode(kind: NodeKind) {
		const vp = getViewport();
		const centerScreen = { x: window.innerWidth * 0.35, y: window.innerHeight * 0.55 };
		const pos = screenToFlowPosition(centerScreen);

		const id = graphStore.addNode(kind, { x: pos.x, y: pos.y });
		graphStore.selectNode(id);
		setCenter(pos.x, pos.y, { zoom: vp.zoom, duration: 250 });
	}

	function coercePortType(t: any): PortType | null {
		// normalize anything that might exist in params (e.g., markdown)
		if (t === 'markdown') return 'text';
		if (t === 'table' || t === 'text' || t === 'json' || t === 'binary' || t === 'embeddings')
			return t;
		return null;
	}

	function getType(nodeId: string, whichPort: string): PortType | null {
		const n = nodes.find((x) => x.id === nodeId);
		if (!n) return null;

		// const hid = handleId ?? 'in';
		// const t = (n.data as any)?.ports?.[in] ?? (n.data as any)?.inputs?.in;
		return n.data.ports[whichPort];
	}

	function isValidConnection(conn: Connection) {
		if (!conn.source || !conn.target) return false;
		if (conn.source === conn.target) return false;

		// Basic cycle prevention (reuse your old DFS idea, but based on local edges)
		const seen = new Set<string>();
		function reaches(start: string, goal: string): boolean {
			if (start === goal) return true;
			for (const e of edges) {
				if (e.source === start && !seen.has(e.target)) {
					seen.add(e.target);
					if (reaches(e.target, goal)) return true;
				}
			}
			return false;
		}
		if (reaches(conn.target, conn.source)) return false;

		//port checking out === in
		const outPort = getType(conn.source, 'out');
		const inPort = getType(conn.target, 'in');

		// if you can't resolve types, fail closed (or choose fail open)
		if (!outPort || !inPort) return false;

		// strict match for Phase 1
		if (outPort !== inPort) {
			console.log('out: ' + outPort + ', in: ' + inPort);
			return false;
		}

		return true;
	}

	function onconnect(conn: Connection) {
		if (!isValidConnection(conn)) return;

		const outPort = getType(conn.source, 'out');
		const inPort = getType(conn.target, 'in');
		if (!outPort || !inPort) return;

		const e: Edge<PipelineEdgeData> = {
			id: `e_${crypto.randomUUID()}`,
			source: conn.source!,
			target: conn.target!,
			markerEnd: { type: MarkerType.ArrowClosed },
			data: {
				exec: 'idle',
				contract: { in: inPort, out: outPort } // ✅ persisted here
			}
		};

		// Delegate adding to the store (validates + persists); store update will sync back to canvas
		const r = graphStore.addEdge(e);
		if (!r.ok) {
			console.warn('Failed to add edge:', r.error);
			return;
		}
	}

	function decorateEdges(es: Edge<PipelineEdgeData>[]) {
		return es.map((e) => {
			const exec = e.data?.exec ?? 'idle';
			return {
				...e,
				class: `edge edge-${exec}`
			};
		});
	}

	function updateSelectedTitle(label: string) {
		if (!$selectedNode) return;
		graphStore.updateNodeTitle($selectedNode.id, label);
	}

	function jumpToNodeFromArtifact(nodeId: string) {
		if (!nodeId) return;
		const n = nodes.find((x) => x.id === nodeId);
		if (!n) return;
		graphStore.selectNode(nodeId);
		inspectorMode = 'output';
		const vp = getViewport();
		setCenter(n.position.x + 120, n.position.y + 40, { zoom: vp.zoom, duration: 250 });
	}

	function resetRunUi() {
		graphStore.resetRunUi();
	}

	function clearSubtypeError(): void {
		subtypeError = null;
		subtypeErrorNodeId = null;
		if (subtypeErrorTimer) {
			clearTimeout(subtypeErrorTimer);
			subtypeErrorTimer = null;
		}
	}

	function showSubtypeError(msg: string, nodeId?: string): void {
		subtypeError = msg;
		subtypeErrorNodeId = nodeId ?? null;
		if (subtypeErrorTimer) clearTimeout(subtypeErrorTimer);
		subtypeErrorTimer = setTimeout(() => {
			subtypeError = null;
			subtypeErrorNodeId = null;
			subtypeErrorTimer = null;
		}, 4000);
	}

	function setSelectedNodeSubtype(value: string): void {
		const n = get(selectedNode);
		if (!n) return;
		const nodeId = n.id;
		const kind = n.data.kind;
		if (kind === 'source') {
			graphStore.setSourceKind(nodeId, value as SourceKind);
			clearSubtypeError();
			return;
		}
		if (kind === 'llm') {
			graphStore.setLlmKind(nodeId, value as LlmKind);
			clearSubtypeError();
			return;
		}
		if (kind === 'transform') {
			const result = graphStore.setTransformKind(nodeId, value as TransformKind);
			if (!result.ok) {
				showSubtypeError(result.error ?? 'Failed to update transform op', nodeId);
			} else {
				clearSubtypeError();
			}
			return;
		}
		if (kind === 'tool') {
			graphStore.setToolProvider(nodeId, value as ToolProvider);
			clearSubtypeError();
		}
	}

	function newGraph() {
		graphStore.hardResetGraph();
	}

	function clampInspectorRatio(v: number): number {
		return Math.max(0.2, Math.min(0.8, v));
	}

	function updateInspectorSplit(clientY: number) {
		if (!inspectorPane) return;
		const rect = inspectorPane.getBoundingClientRect();
		if (rect.height <= 0) return;
		const ratio = (clientY - rect.top) / rect.height;
		inspectorTopRatio = clampInspectorRatio(ratio);
	}

	function onInspectorSplitDown(event: PointerEvent) {
		resizingInspector = true;
		updateInspectorSplit(event.clientY);
	}

	function onInspectorSplitMove(event: PointerEvent) {
		if (!resizingInspector) return;
		updateInspectorSplit(event.clientY);
	}

	function onInspectorSplitUp() {
		resizingInspector = false;
	}

	onDestroy(() => {
		if (subtypeErrorTimer) clearTimeout(subtypeErrorTimer);
	});
</script>

<svelte:window on:pointermove={onInspectorSplitMove} on:pointerup={onInspectorSplitUp} />

<div class="layout">
	<div class="flow">
		<div class="topbar">
			<div class="btnrow">
				<button on:click={newGraph}>New Graph</button>
				<button on:click={resetRunUi}>Reset</button>
				<button class="primary" on:click={() => graphStore.runRemote(null, 'from_start')}>
					Run from start
				</button>
				<button
					class="primary"
					disabled={!$selectedNode}
					on:click={() => graphStore.runRemote($selectedNode?.id ?? null, 'from_selected_onward')}
				>
					Run from selected
				</button>

				<span class="status">Graph: {graphHeaderStatus}</span>
			</div>

			<div class="btnrow">
				<button on:click={() => addNode('source')}>+ Source</button>
				<button on:click={() => addNode('transform')}>+ Transform</button>
				<button on:click={() => addNode('llm')}>+ LLM</button>
				<button on:click={() => addNode('tool')}>+ Tool</button>
			</div>
		</div>
		<OutputModal bind:open={outputOpen} nodeId={outputNodeId} />

		<SvelteFlow
			bind:nodes
			edges={displayEdges}
			{nodeTypes}
			{onnodeclick}
			{onnodecontextmenu}
			{onedgecontextmenu}
			{isValidConnection}
			{onconnect}
			onnodedragstop={() => {
				if (!applyingFromStore) graphStore.syncFromCanvas(nodes, edges);
			}}
			fitView
			defaultEdgeOptions={{ markerEnd: { type: MarkerType.ArrowClosed } }}
		>
			<Background />
			<Controls />
		</SvelteFlow>
	</div>

	<aside class="inspector" bind:this={inspectorPane}>
		<div class="inspectorTop" style={`flex: 0 0 ${Math.round(inspectorTopRatio * 100)}%;`}>
			<!-- <h3>Inspector</h3> -->

			{#if $selectedNode}
				<div class="card editorCard">
					<div class="head">
						<div style="min-width:0;display:flex;align-items:center;gap:8px;">
							{#if isEditingTitle}
								<input
									id="node-title-input"
									value={titleDraft}
									size={Math.max(1, titleDraft.length || 1)}
									on:input={(e) => {
										const next = (e.currentTarget as HTMLInputElement).value;
										titleDraft = next;
										updateSelectedTitle(next);
									}}
									on:blur={() => commitEditTitle()}
									on:keydown={(e) => {
										if (e.key === 'Enter') {
											e.preventDefault();
											commitEditTitle();
										} else if (e.key === 'Escape') {
											e.preventDefault();
											cancelEditTitle();
										}
									}}
									style="font-size:14px;font-weight:600;max-width:100%;width:auto;"
								/>
							{:else}
								<b
									class="title"
									role="button"
									tabindex="0"
									title="Click to edit title"
									style="cursor:text;display:inline-block;max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;"
									on:click={beginEditTitle}
									on:keydown={(e) => {
										if (e.key === 'Enter') beginEditTitle();
									}}
								>
									{$selectedNode.data.label}
								</b>
							{/if}

							<span class="pill">{$selectedNode.data.kind}</span>
							{#if headerCachePill}
								<span
									class={headerCachePill.className}
									title={headerCachePill.title}
								>
									{headerCachePill.label}
								</span>
							{/if}
						</div>

						<div class="headPills">
							<span class={`pill st-${displayNodeStatus ?? 'idle'}`}>
								{displayNodeStatus ?? 'idle'}
							</span>
						</div>
					</div>
					<div class="inspectorTabs">
						<button
							class="tabBtn"
							class:active={inspectorMode === 'edit'}
							on:click={() => (inspectorMode = 'edit')}
						>
							Edit
						</button>

						<button
							class="tabBtn"
							class:active={inspectorMode === 'inputs'}
							disabled={!hasInputs}
							on:click={() => (inspectorMode = 'inputs')}
						>
							Inputs
						</button>
						<button
							class="tabBtn"
							class:active={inspectorMode === 'output'}
							disabled={!hasOutput}
							on:click={() => (inspectorMode = 'output')}
						>
							Output
						</button>
						<button
							class="tabBtn"
							class:active={inspectorMode === 'ports'}
							on:click={() => (inspectorMode = 'ports')}
						>
							Ports
						</button>
						{#if inspectorMode === 'edit' && $selectedNode}
							<select
								class="nodeTypeSwitch"
								aria-label="Node subtype"
								value={
									$selectedNode.data.kind === 'source'
										? selectedSourceKind
										: $selectedNode.data.kind === 'llm'
											? selectedLlmKind
											: $selectedNode.data.kind === 'transform'
												? selectedTransformKind
												: selectedToolProvider
								}
								on:change={(e) => setSelectedNodeSubtype((e.currentTarget as HTMLSelectElement).value)}
							>
								{#if $selectedNode.data.kind === 'source'}
									<option value="file">file</option>
									<option value="database">database</option>
									<option value="api">api</option>
								{:else if $selectedNode.data.kind === 'llm'}
									<option value="ollama">ollama</option>
									<option value="openai_compat">openai_compat</option>
								{:else if $selectedNode.data.kind === 'transform'}
									<option value="filter">filter</option>
									<option value="select">select</option>
									<option value="rename">rename</option>
									<option value="derive">derive</option>
									<option value="aggregate">aggregate</option>
									<option value="join">join</option>
									<option value="sort">sort</option>
									<option value="limit">limit</option>
									<option value="dedupe">dedupe</option>
									<option value="split">split</option>
									<option value="sql">sql</option>
								{:else if $selectedNode.data.kind === 'tool'}
									<option value="mcp">mcp</option>
									<option value="http">http</option>
									<option value="function">function</option>
									<option value="python">python</option>
									<option value="js">js</option>
									<option value="shell">shell</option>
									<option value="db">db</option>
									<option value="builtin">builtin</option>
								{/if}
							</select>
						{/if}
						{#if subtypeError}
							<span class="subtypeError" aria-live="polite">{subtypeError}</span>
						{/if}
						
					</div>

					<div class="editorScroll">
						{#if inspectorMode === 'edit'}
							<NodeInspector />
						{:else if inspectorMode === 'inputs'}
							<div class="inputsView">
								{#if inputResolutions.length === 0}
									<div class="inputMissing">No input ports.</div>
								{:else}
									{#each inputResolutions as input (input.inPort)}
										<div class="inputCard">
											<div class="inputHead">
												<span class="inputPort">{input.inPort}</span>
												<span
													class={`pill ${input.status === 'resolved'
														? input.artifactSource === 'active_run'
															? 'st-running'
															: 'st-succeeded'
														: 'st-stale'}`}
												>
													{input.status === 'resolved'
														? input.artifactSource === 'active_run'
															? 'active'
															: 'bound'
														: 'missing'}
												</span>
											</div>
											<div class="inputUpstream">
												{#if input.edge}
													{upstreamLabel(input.edge.fromNodeId, input.edge.fromPort)}
												{:else}
													-
												{/if}
											</div>
											{#if input.status === 'missing'}
												<div class="inputMissing">{inputReasonCopy(input.reason)}</div>
											{:else}
												<div class="inputArtifact">
													<span class="mono">{shortId(input.artifactId)}</span>
													<button
														class="tabBtn"
														on:click={() => (inputPreviewArtifactId = input.artifactId ?? null)}
													>
														View
													</button>
												</div>
												{#if input.artifactId}
													<div class="inputMeta">
														<div>
															mime: {inputMetaByArtifactId[input.artifactId]?.mimeType ??
																input.artifactSummary?.mimeType ??
																'-'}
														</div>
														<div>
															contract: {inputMetaByArtifactId[input.artifactId]?.contract ??
																input.artifactSummary?.contract ??
																'-'}
														</div>
														<div>
															schema: {shortId(
																String(
																	inputMetaByArtifactId[input.artifactId]?.schemaFingerprint ??
																		input.artifactSummary?.schemaFingerprint ??
																		''
																),
																12
															) || '-'}
														</div>
													</div>
												{/if}
											{/if}
										</div>
									{/each}
									{#if inputPreviewArtifactId}
										<div class="inputPreview">
											<ArtifactViewer
												artifactId={inputPreviewArtifactId}
												graphId={$graphStore.graphId}
												onJumpToNode={jumpToNodeFromArtifact}
											/>
										</div>
									{/if}
								{/if}
							</div>
						{:else if inspectorMode === 'output'}
							<ArtifactViewer
								artifactId={activeArtifactId}
								graphId={$graphStore.graphId}
								mimeType={nodeOut.mimeType}
								portType={nodeOut.portType}
								cached={nodeOut.cached}
								cacheDecision={nodeOut.cacheDecision}
								preview={nodeOut.preview}
								onJumpToNode={jumpToNodeFromArtifact}
							/>
						{:else}
							<PortsEditor selectedNode={$selectedNode} />
						{/if}
					</div>

					{#if inspectorMode === 'edit' && !hideInspectorApplyRow}
						<!-- Apply row (applies to any draft-only fields in editors) -->
						<div class="inspectorActions">
							<button
								class="primary"
								disabled={!$graphStore.inspector.dirty}
								on:click={() => graphStore.applyInspectorDraft()}
							>
								Accept
							</button>

							<button
								disabled={!$graphStore.inspector.dirty}
								on:click={() => graphStore.revertInspectorDraft()}
							>
								Revert
							</button>
						</div>
					{/if}
				</div>
			{:else}
				<p>Click a node to edit it.</p>
			{/if}
		</div>
		<button
			type="button"
			class="inspectorSplitter"
			aria-label="Resize inspector panels"
			on:pointerdown={onInspectorSplitDown}
		></button>
		<div class="inspectorBottom">
			<h3>Run Logs</h3>
			<div class="logs" bind:this={scrollElement}>
				{#each $graphStore.logs as l (l.id)}
					<div class={`log ${l.level}`}>
						<span class="ts">{l.ts}</span>
						<span class="msg">
							{#if l.nodeId}
								<span class="nid">[{l.nodeId}]</span>
							{/if}
							{l.message}
						</span>
					</div>
				{/each}
			</div>
		</div>
	</aside>
</div>

<style>
	@import './styles/inspectorForm.css';

	:global(.edge path) {
		stroke: #2f3646;
		stroke-width: 2;
	}
	:global(.edge.edge-active path) {
		stroke-width: 3.5;
		stroke: #4b8cff;
		filter: drop-shadow(0 0 6px rgba(75, 140, 255, 0.6));
		stroke-dasharray: 8 6;
		animation: dashmove 0.8s linear infinite;
	}
	:global(.edge.edge-done path) {
		stroke: #7ee787;
		stroke-width: 3;
		filter: drop-shadow(0 0 4px rgba(126, 231, 135, 0.4));
	}
	@keyframes dashmove {
		to {
			stroke-dashoffset: -28;
		}
	}

	.layout {
		display: grid;
		grid-template-columns: 1fr calc(380px + 5ch);
		height: 100vh;
	}

	.flow {
		min-width: 0;
		display: flex;
		flex-direction: column;
	}

	.topbar {
		padding: 10px;
		display: flex;
		justify-content: space-between;
		align-items: center;
		border-bottom: 1px solid #1f2430;
		background: #0b0c10;
		color: #e6e6e6;
	}

	.btnrow {
		display: flex;
		gap: 8px;
		align-items: center;
	}

	.status {
		opacity: 0.85;
		font-size: 13px;
	}

	button {
		border: 1px solid #283044;
		background: #111522;
		color: #e6e6e6;
		padding: 8px 10px;
		border-radius: 10px;
		cursor: pointer;
		font-weight: 600;
	}
	button.primary {
		border-color: #4b8cff;
	}
	button:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}
	button:hover:not(:disabled) {
		filter: brightness(1.1);
	}

	.inspector {
		border-left: 1px solid #222;
		padding: 12px;
		background: #0b0c10;
		color: #e6e6e6;

		display: flex;
		flex-direction: column;
		height: 100vh;
		gap: 8px;
	}

	.inspectorTop {
		min-height: 0;
		overflow: hidden;
		padding-bottom: 20px;
	}

	.inspectorSplitter {
		display: block;
		width: 100%;
		height: 4px;
		padding: 0;
		border: 0;
		border-radius: 999px;
		background: #283044;
		cursor: row-resize;
		flex: 0 0 auto;
		touch-action: none;
		user-select: none;
	}

	.inspectorSplitter:hover,
	.inspectorSplitter:focus {
		background: #3c4d70;
		outline: none;
	}

	.inspectorBottom {
		flex: 1;
		min-height: 0;
		overflow: hidden;
		display: flex;
		flex-direction: column;
	}

	.card {
		border: 1px solid #1f2430;
		border-radius: 12px;
		padding: 12px;
		background: #0f1115;
	}

	.head {
		display: flex;
		align-items: center;
		justify-content: space-between;
	}

	.headPills {
		display: flex;
		align-items: center;
		gap: 6px;
		flex-wrap: wrap;
		justify-content: flex-end;
		min-width: 0;
	}

	.headPills .pill {
		margin-left: 0;
	}

	.pill {
		opacity: 0.85;
		font-size: 12px;
		margin-left: 8px;
		padding: 3px 8px;
		border: 1px solid #283044;
		border-radius: 999px;
		display: inline-flex;
		align-items: center;
		line-height: 1.2;
	}

	.pill-cache {
		background: rgba(95, 111, 137, 0.12);
	}

	.pill-cache-mismatch {
		border-color: #f2cc60;
		background: rgba(242, 204, 96, 0.14);
	}

	.st-idle {
		border-color: #283044;
	}
	.st-stale {
		border-color: #f2cc60;
	}
	.st-running {
		border-color: #8ab4ff;
	}
	.st-succeeded {
		border-color: #7ee787;
	}
	.st-failed {
		border-color: #ff7b72;
	}

	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 8px;
	}

	.logs {
		border: 1px solid #1f2430;
		border-radius: 12px;
		background: #0f1115;
		padding: 10px;
		flex: 1;
		min-height: 0;
		overflow: auto;
		margin-top: 10px;
	}

	.log {
		font-family: ui-monospace, Menlo, Consolas, monospace;
		font-size: 12px;
		padding: 6px 0;
		border-bottom: 1px solid #171b24;
	}
	.log:last-child {
		border-bottom: none;
	}

	.ts {
		opacity: 0.65;
		margin-right: 8px;
	}
	.nid {
		opacity: 0.75;
		margin-right: 6px;
	}
	.log.error {
		color: #ff7b72;
	}
	.log.warn {
		color: #f2cc60;
	}

	.editorCard {
		height: 100%;
		display: flex;
		flex-direction: column;
		min-height: 0;
	}

	.title {
		font-size: 14px;
	}

	.editorScroll {
		margin-top: 10px;
		min-height: 0;
		overflow-y: auto;
		overflow-x: hidden;
		padding-right: 30px;
		box-sizing: border-box;
	}

	.section {
		font-size: 12px;
		line-height: 1.25;
	}

	.sectionTitle {
		font-weight: 700;
		margin: 8px 0 6px;
		font-size: 12px;
		opacity: 0.9;
	}

	.group {
		margin-top: 10px;
		padding-left: 10px;
		border-left: 2px solid #1f2430;
	}

	.groupTitle {
		font-weight: 700;
		font-size: 12px;
		opacity: 0.85;
		margin-bottom: 6px;
	}

	/* safety: prevent horizontal overflow in editors */
	.editorScroll :global(input),
	.editorScroll :global(select),
	.editorScroll :global(textarea) {
		box-sizing: border-box;
		max-width: 100%;
	}

	.inspectorActions {
		font-size: 13px;
		display: flex;
		gap: 20px;
		margin: 5px;
	}

	.inspectorTabs {
		display: flex;
		gap: 6px;
		margin: 6px;
	}

	.tabBtn {
		font-size: 12px; /* ← same scale as Ports label */
		padding: 4px 10px; /* ← small like section controls */
		border-radius: 6px;
		border: 1px solid #2c3444;
		background: #111622;
		color: #9aa3b2;
		cursor: pointer;
		line-height: 1.2;
	}

	.inspectorTabs button {
		padding: 6px 10px;
		border-radius: 8px;
		border: 1px solid #283044;
		background: transparent;
		color: inherit;
		cursor: pointer;
	}

	.inspectorTabs button.active {
		background: #283044;
		font-weight: 700;
	}

	.nodeTypeSwitch {
		margin-left: auto;
		max-width: 200px;
		padding: 6px 10px;
		font-family: inherit;
		font-size: 12px;
		line-height: 1.2;
		border-radius: 8px;
		border: 1px solid #283044;
		background: #0b1220;
		color: #e5e7eb;
	}

	.nodeTypeSwitch option {
		background: #0b1220;
		color: #e5e7eb;
	}

	.nodeTypeSwitch:focus {
		outline: none;
		border-color: #3b82f6;
		box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2);
	}

	@media (prefers-color-scheme: light) {
		.nodeTypeSwitch {
			border: 1px solid #b9c5da;
			background: #ffffff;
			color: #1f2937;
		}

		.nodeTypeSwitch option {
			background: #ffffff;
			color: #1f2937;
		}
	}

	.subtypeError {
		margin-left: 8px;
		padding: 4px 8px;
		border-radius: 999px;
		border: 1px solid rgba(239, 68, 68, 0.55);
		background: rgba(239, 68, 68, 0.12);
		color: #fecaca;
		font-size: 11px;
		line-height: 1.2;
		max-width: 420px;
		white-space: normal;
	}

	.inspectorTabs button:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}

	.inputsView {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.inputCard {
		border: 1px solid #1f2430;
		border-radius: 10px;
		padding: 8px;
		background: #0f1115;
	}

	.inputHead {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
	}

	.inputPort {
		font-size: 12px;
		opacity: 0.9;
	}

	.inputUpstream {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 4px;
	}

	.inputArtifact {
		display: flex;
		align-items: center;
		gap: 8px;
		margin-top: 6px;
	}

	.inputMeta {
		font-size: 11px;
		opacity: 0.78;
		margin-top: 4px;
		display: grid;
		gap: 2px;
	}

	.inputMissing {
		font-size: 12px;
		opacity: 0.8;
		margin-top: 4px;
	}

	.inputPreview {
		border-top: 1px solid #1f2430;
		margin-top: 6px;
		padding-top: 8px;
	}

</style>
