<script lang="ts">
	import { tick } from 'svelte';
	import { SvelteFlow, Background, Controls, MarkerType, useSvelteFlow } from '@xyflow/svelte';
	import '@xyflow/svelte/dist/style.css';

	import type { Node, Edge, Connection } from '@xyflow/svelte';

	import { nodeTypes } from '$lib/flow/nodeTypes';
	import type { PipelineNodeData, PipelineEdgeData, NodeKind, PortType } from '$lib/flow/types'; //porttype actually in base
	import { graphStore, selectedNode } from '$lib/flow/store/graphStore';
	import NodeInspector from '$lib/flow/components/NodeInspector.svelte';
	import OutputModal from '$lib/flow/components/OutputModal.svelte';
	import { PortTypeSchema } from './schema';
	import { get } from 'svelte/store';
	import ArtifactViewer from './components/ArtifactViewer.svelte';

	const { screenToFlowPosition, setCenter, getViewport } = useSvelteFlow();

	let outputOpen = false;
	let outputNodeId: string | null = null;

	// local bind state (SvelteFlow requires bind)
	let nodes: Node<PipelineNodeData>[] = [];
	let edges: Edge<PipelineEdgeData>[] = [];

	let scrollElement: HTMLDivElement;

	// Guard: when we apply store -> local, we don't want to sync right back.
	let applyingFromStore = false;

	let lastStoreNodes: Node<PipelineNodeData>[] | null = null;
	let lastStoreEdges: Edge<PipelineEdgeData>[] | null = null;

	$: if ($graphStore.logs && scrollElement) {
		scrollToBottom();
	}

	$: displayEdges = edges.map((e) => ({
		...e,
		class: `edge edge-${e.data?.exec ?? 'idle'}`
	}));

	$: {
		const s = $graphStore;

		// Only apply store -> local when the STORE references change,
		// not when the CANVAS changes (like while dragging).
		const storeNodesChanged = s.nodes !== lastStoreNodes;
		const storeEdgesChanged = s.edges !== lastStoreEdges;

		if (storeNodesChanged || storeEdgesChanged) {
			applyingFromStore = true;

			if (storeNodesChanged) {
				nodes = s.nodes;
				lastStoreNodes = s.nodes;
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
					in: $selectedNode.data.ports.in ?? 'table',
					out: $selectedNode.data.ports.out ?? 'table'
				}
			},
			null,
			2
		);
	}
	//ViewArtifact
	type InspectorMode = 'edit' | 'output';
	let inspectorMode: InspectorMode = 'edit';

	$: selectedId = $selectedNode?.id;
	$: nodeOut = selectedId ? $graphStore.nodeOutputs?.[selectedId] : undefined;
	$: hasOutput = !!nodeOut?.artifactId;

	// auto-fallback if you select a node without output
	$: if (inspectorMode === 'output' && !hasOutput) inspectorMode = 'edit';
	//ViewArtifact

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
		if (
			t === 'table' ||
			t === 'text' ||
			t === 'json' ||
			t === 'binary' ||
			t === 'chat' ||
			t === 'embeddings'
		)
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

	function resetRunUi() {
		graphStore.resetRunUi();
	}

	async function loadOutput(nodeId: string) {
		const s = get(graphStore);
		const info = s.nodeOutputs[nodeId];
		if (!info) return { kind: 'empty' as const };

		const res = await fetch(`/runs/artifacts/${info.artifactId}`);
		const mime = info.mimeType;

		if (!res.ok) return { kind: 'error' as const, status: res.status, text: await res.text() };

		if (mime === 'application/json') return { kind: 'json' as const, data: await res.json() };
		return { kind: 'text' as const, text: await res.text() };
	}
</script>

<div class="layout">
	<div class="flow">
		<div class="topbar">
			<div class="btnrow">
				<button on:click={resetRunUi}>Reset</button>
				<button class="primary" on:click={() => graphStore.runRemote(null)}>Run from start</button>
				<button
					class="primary"
					disabled={!$selectedNode}
					on:click={() => graphStore.runRemote($selectedNode?.id ?? null)}
				>
					Run from selected
				</button>

				<span class="status">Status: {$graphStore.runStatus}</span>
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

	<aside class="inspector">
		<div class="inspectorTop">
			<h3>Inspector</h3>

			{#if $selectedNode}
				<div class="card editorCard">
					<div class="head">
						<div>
							<b class="title">{$selectedNode.data.label}</b>
							<span class="pill">{$selectedNode.data.kind}</span>
						</div>
						<span class={`pill st-${$selectedNode.data.status ?? 'idle'}`}>
							{$selectedNode.data.status ?? 'idle'}
						</span>
					</div>

					<div class="field">
						<div class="k">Title</div>
						<div class="v">
							<input
								id="edit-title"
								value={$selectedNode.data.label}
								on:input={(e) => updateSelectedTitle((e.currentTarget as HTMLInputElement).value)}
							/>
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
							class:active={inspectorMode === 'output'}
							disabled={!hasOutput}
							on:click={() => (inspectorMode = 'output')}
						>
							Output
						</button>
					</div>

					<!-- Source editor lives here (and will be scrollable) -->
					<div class="editorScroll">
						{#if inspectorMode === 'edit'}
							<NodeInspector />
						{:else}
							<ArtifactViewer
								artifactId={nodeOut.artifactId}
								mimeType={nodeOut.mimeType}
								preview={nodeOut.preview}
							/>
						{/if}
					</div>

					{#if inspectorMode === 'edit'}
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
		<hr style="border:3px solid #283044;" />
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
		grid-template-columns: 1fr 380px;
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
		gap: 10px;
	}

	/* Top cannot exceed 50% */
	.inspectorTop {
		max-height: 50%;
		min-height: 0;
		overflow: hidden;
		padding-bottom: 20px;
	}

	.inspectorBottom {
		flex: 1;
		min-height: 0;
		overflow: hidden;
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

	.pill {
		opacity: 0.85;
		font-size: 12px;
		margin-left: 8px;
		padding: 3px 8px;
		border: 1px solid #283044;
		border-radius: 999px;
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
		height: 100%;
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

	/* ---------------------------
	Canonical inspector k/v form layout
   --------------------------- */
	:global(.inspector .field) {
		display: flex;
		flex-direction: row;
		align-items: center;
		gap: 12px;
		margin: 8px 0;
	}

	:global(.inspector .k) {
		flex: 0 0 140px;
		min-width: 140px;
		font-family: ui-monospace, Menlo, Consolas, monospace;
		font-size: 12.5px;
		opacity: 0.8;
		padding-left: 6px;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	:global(.inspector .v) {
		flex: 1;
		min-width: 0;
		padding-left: 0;
		margin-top: 0;
		font-size: 13px;
	}

	:global(.inspector select) {
		background: #0b0c10;
		color: #e6e6e6;
		border: 1px solid #283044;
	}

	/* options inside dropdown */
	:global(.inspector select option) {
		background-color: #0b0c10;
		color: #e6e6e6;
	}

	/* highlighted option */
	:global(.inspector select option:checked),
	:global(.inspector select option:hover) {
		background-color: #283044;
	}

	.inspector .v :global(input),
	.inspector .v :global(select),
	.inspector .v :global(textarea) {
		width: 100%;
		box-sizing: border-box;
		font-size: 13px;
		padding: 6px 8px;
		border-radius: 8px;
		background: #0b0c10;
		color: #e6e6e6;
		border: 1px solid #283044;
	}

	.inspector .v :global(*) {
		min-width: 0;
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
		margin-bottom: 8px;
	}

	.tabBtn {
	font-size: 12px;          /* ← same scale as Ports label */
	padding: 4px 10px;        /* ← small like section controls */
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

	.inspectorTabs button:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}
</style>
