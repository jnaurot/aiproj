<script lang="ts">
	import { onDestroy, onMount, tick } from 'svelte';
	import { get } from 'svelte/store';
	import { SvelteFlow, Background, Controls, MarkerType, useSvelteFlow } from '@xyflow/svelte';
	import '@xyflow/svelte/dist/style.css';

	import type { Node, Edge, Connection } from '@xyflow/svelte';

	import { nodeTypes } from '$lib/flow/nodeTypes';
	import type { PipelineNodeData, PipelineEdgeData, NodeKind, PortType } from '$lib/flow/types'; //porttype actually in base
	import type { SourceKind, LlmKind, TransformKind, ToolProvider, ComponentKind } from '$lib/flow/types/paramsMap';
	import { graphStore, selectedNode } from '$lib/flow/store/graphStore';
	import type { GraphState, InputResolution } from '$lib/flow/store/graphStore';
	import NodeInspector from '$lib/flow/components/NodeInspector.svelte';
	import PortsEditor from '$lib/flow/components/PortsEditor.svelte';
	import OutputModal from '$lib/flow/components/OutputModal.svelte';
	import ArtifactViewer from './components/ArtifactViewer.svelte';
	import ToolbarMenu from './components/ToolbarMenu.svelte';
	import {
		buildAddMenuItems,
		buildProjectMenuItems,
		dispatchAddMenuAction,
		dispatchProjectMenuAction,
		routePrimarySaveAction
	} from './components/flowToolbarModel';
	import { getHeaderCachePill, getHeaderNodeStatus } from './components/inspectorCachePill';
	import { buildHeaderContextLabels } from './components/headerContext';
	import { buildScopedStatus } from './components/statusScope';
	import { parseComponentExitDecision } from './components/componentExitGuard';
	import { getArtifactMetaUrl } from '$lib/flow/client/runs';
	import { getGlobalCacheConfig, setGlobalCacheConfig } from '$lib/flow/client/runs';
	import { listEnvProfiles, installEnvProfile, type EnvProfileStatus } from '$lib/flow/client/envProfiles';
	import {
		exportGraphPackage,
		importGraphPackage,
		type GraphRevisionSummary,
		type GraphCatalogItem
	} from '$lib/flow/client/graphs';
	import { getGraphDraftInfo } from '$lib/flow/store/persist';
	import {
		getComponentRevision,
		createComponentRevision,
		validateComponentRevision,
		type ComponentApiContract,
		type ComponentCatalogItem,
		type ComponentRevisionSummary
	} from '$lib/flow/client/components';
import {
	summarizeComponentPreflight,
	summarizeComponentPublishFailure
} from '$lib/flow/components/componentPublishPreflight';
	import { TransformEditorCommitModeByKind } from '$lib/flow/components/editors/TransformEditor/TransformEditor';
	import { nodePresetStore } from '$lib/flow/store/nodePresetStore';
	import type { NodePreset } from '$lib/flow/store/nodePresetStore';
	import type { ToolbarMenuItem } from './components/toolbarMenu';
	import { refreshPortCapabilitiesFromBackend } from '$lib/flow/portCapabilities';

	const { screenToFlowPosition, setCenter, getViewport, setViewport } = useSvelteFlow();

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
	let lastStoreNodeSignature = '';
	let lastStoreEdgeSignature = '';

	function nodeSignature(list: Node<PipelineNodeData>[]): string {
		return list
			.map((n) => `${String(n.id)}:${Number(n.position?.x ?? 0)}:${Number(n.position?.y ?? 0)}:${n.selected ? 1 : 0}`)
			.join('|');
	}

	function edgeSignature(list: Edge<PipelineEdgeData>[]): string {
		return list
			.map((e) =>
				[
					String(e.id ?? ''),
					String(e.source ?? ''),
					String(e.sourceHandle ?? ''),
					String(e.target ?? ''),
					String(e.targetHandle ?? ''),
					String((e.data as any)?.exec ?? '')
				].join(':')
			)
			.join('|');
	}

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
	$: if ($graphStore.logs && scrollElement && logAutoScrollEnabled) {
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
		const nextNodeSig = nodeSignature(s.nodes);
		const nextEdgeSig = edgeSignature(s.edges);

		// Only apply store -> local when the STORE references change,
		// not when the CANVAS changes (like while dragging).
		const storeNodesChanged = s.nodes !== lastStoreNodes || nextNodeSig !== lastStoreNodeSignature;
		const storeEdgesChanged = s.edges !== lastStoreEdges || nextEdgeSig !== lastStoreEdgeSignature;
		const storeSelectionChanged = s.selectedNodeId !== lastSelectedNodeId;

		if (storeNodesChanged || storeEdgesChanged || storeSelectionChanged) {
			applyingFromStore = true;

			if (storeNodesChanged || storeSelectionChanged) {
				nodes = applyCanvasSelection(s.nodes, s.selectedNodeId);
				lastStoreNodes = s.nodes;
				lastStoreNodeSignature = nextNodeSig;
				lastSelectedNodeId = s.selectedNodeId;
			}

			if (storeEdgesChanged) {
				edges = s.edges;
				lastStoreEdges = s.edges;
				lastStoreEdgeSignature = nextEdgeSig;
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
	let inspectorTopWeight = 2;
	let environmentWeight = 1;
	let runLogsWeight = 1;
	let inspectorTopPaneEl: HTMLElement | null = null;
	let environmentPaneEl: HTMLElement | null = null;
	let runLogsPaneEl: HTMLElement | null = null;
	type InspectorSplitPair = 'top_env' | 'env_logs';
	let activeInspectorSplit: InspectorSplitPair | null = null;
	let splitStartY = 0;
	let splitPaneAStartPx = 0;
	let splitPaneBStartPx = 0;
	let subtypeError: string | null = null;
	let subtypeErrorNodeId: string | null = null;
	let subtypeErrorTimer: ReturnType<typeof setTimeout> | null = null;
	type GlobalCacheMode = 'default_on' | 'force_off' | 'force_on';
	let globalCacheMode: GlobalCacheMode = 'default_on';
	let globalCachePending = false;
	let commandPaletteOpen = false;
	let commandFilter = '';
	let commandFilterInput: HTMLInputElement | null = null;
	let runLogFilter = '';
	let envProfiles: EnvProfileStatus[] = [];
	let envProfilesLoading = false;
	let envProfilesError: string | null = null;
	let envInstallPendingByProfile: Record<string, boolean> = {};
	let previousEditingContext: 'graph' | 'component' = 'graph';
	let logAutoScrollEnabled = true;
	let nodeInspectorCollapsed = false;
	let environmentCollapsed = false;
	let runLogsCollapsed = false;
	type GraphUiReturnSnapshot = {
		viewport: { x: number; y: number; zoom: number };
		inspectorMode: InspectorMode;
		runLogScrollTop: number;
		runLogFilter: string;
	};
	let graphUiReturnSnapshot: GraphUiReturnSnapshot | null = null;
	let toastMessage: string | null = null;
	let toastLevel: 'info' | 'warn' | 'error' = 'info';
	let toastTimer: ReturnType<typeof setTimeout> | null = null;
	let lastSavedGraphSnapshotKey: string | null = null;
	let currentGraphName = 'unnamed';
	const DRAFT_RECOVERY_PROMPT_SESSION_KEY = 'graph_draft_recovery_prompted_at';
	let importFileInput: HTMLInputElement | null = null;
	let componentEditEntrySnapshotKey: string | null = null;
	let componentEditEntrySessionKey: string | null = null;
	let currentComponentSessionKey = '';
	let componentInternalsDirty = false;
	type ComponentSaveApplyScope = 'none' | 'one' | 'all';
	type ComponentSaveApplyPromptState = {
		componentId: string;
		fromRevisionId: string;
		toRevisionId: string;
		matchingCount: number;
		entryMatchCount: number;
		allMatchCount: number;
	};
	let componentSaveApplyModalOpen = false;
	let componentSaveApplyPrompt: ComponentSaveApplyPromptState = {
		componentId: '',
		fromRevisionId: '',
		toRevisionId: '',
		matchingCount: 0,
		entryMatchCount: 0,
		allMatchCount: 0
	};
	let componentSaveApplyResolver: ((scope: ComponentSaveApplyScope) => void) | null = null;
	type CanonicalGraphSnapshot = {
		graphId: string;
		nodes: Array<{
			id: string;
			type: string;
			position: { x: number; y: number };
			data: {
				kind?: string;
				label?: string;
				sourceKind?: string;
				transformKind?: string;
				llmKind?: string;
				componentKind?: string;
				ports?: { in?: unknown; out?: unknown };
				params?: unknown;
			};
		}>;
		edges: Array<{
			id: string;
			source: string;
			target: string;
			sourceHandle: string | null;
			targetHandle: string | null;
		}>;
	};
	const GlobalCacheModeLabels: Record<GlobalCacheMode, string> = {
		default_on: 'Default on',
		force_off: 'Forced off',
		force_on: 'Forced on'
	};
	type CommandItem = {
		id: string;
		label: string;
		disabled?: boolean;
		run: () => void;
	};
	$: presets = [...$nodePresetStore];
	$: hasPresets = presets.length > 0;
	$: selectedPresetRefId =
		(($selectedNode?.data as any)?.meta?.presetRef?.id as string | undefined | null) ?? null;
	$: selectedPresetRefExists = Boolean(
		selectedPresetRefId && presets.some((preset) => preset.id === selectedPresetRefId)
	);
	$: envProfilesInstalledCount = envProfiles.filter((profile) => Boolean(profile.installed)).length;
	$: envProfilesMissingCount = envProfiles.filter((profile) => !Boolean(profile.installed)).length;

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
	$: inspectorAcceptValidation = graphStore.getInspectorDraftAcceptValidation($graphStore as GraphState);
	$: inspectorAcceptDisabled = !$graphStore.inspector.dirty || !inspectorAcceptValidation.ok;
	$: inspectorSystemNotice = String(($graphStore.inspector as any)?.systemNotice ?? '').trim();
	$: inspectorAcceptTooltip =
		$graphStore.inspector.dirty && !inspectorAcceptValidation.ok
			? String(inspectorAcceptValidation.errors?.[0] ?? 'Resolve draft validation errors before Accept.')
			: undefined;
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
	$: selectedComponentKind =
		(((inspectorParams as any)?.componentKind ??
			($selectedNode?.data as any)?.componentKind ??
			'graph_component') as ComponentKind);
	$: hideInspectorApplyRow =
		inspectorMode === 'edit' &&
		$selectedNode?.data?.kind === 'transform' &&
		TransformEditorCommitModeByKind[selectedTransformKind] === 'immediate';
	$: nodeBinding = selectedId ? $graphStore.nodeBindings?.[selectedId] : undefined;
	$: nodeOut = selectedId ? $graphStore.nodeOutputs?.[selectedId] : undefined;
	$: nodeError = (nodeOut as any)?.lastError ?? null;
	$: selectedComponentHasUpdate =
		$selectedNode?.data?.kind === 'component'
			? Boolean(($selectedNode.data.meta as any)?.componentHasUpdate)
			: false;
	$: selectedComponentLatestRevisionId =
		$selectedNode?.data?.kind === 'component'
			? String(($selectedNode.data.meta as any)?.componentLatestRevisionId ?? '').trim()
			: '';
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
	$: isComponentEditContext = $graphStore.editingContext === 'component';
	$: editingComponentName = String($graphStore.componentEditSession?.componentId ?? '').trim() || 'unknown';
	$: headerContextLabels = buildHeaderContextLabels({
		editingContext: $graphStore.editingContext,
		graphName: currentGraphName,
		componentName: editingComponentName
	});
	$: statusScopeLabel = headerContextLabels.scopeLabel;
	$: currentGraphSnapshotKey = JSON.stringify(canonicalGraphSnapshot($graphStore.graphId, nodes, edges));
	$: currentComponentSessionKey = isComponentEditContext
		? `${String($graphStore.componentEditSession?.componentId ?? '').trim()}@${String($graphStore.componentEditSession?.revisionId ?? '').trim()}`
		: '';
	$: if (!isComponentEditContext) {
		componentEditEntrySnapshotKey = null;
		componentEditEntrySessionKey = null;
	} else if (
		componentEditEntrySnapshotKey == null ||
		componentEditEntrySessionKey !== currentComponentSessionKey
	) {
		componentEditEntrySnapshotKey = currentGraphSnapshotKey;
		componentEditEntrySessionKey = currentComponentSessionKey;
	}
	$: componentInternalsDirty =
		isComponentEditContext &&
		(Boolean($graphStore.inspector.dirty) ||
			(componentEditEntrySnapshotKey != null && componentEditEntrySnapshotKey !== currentGraphSnapshotKey));
	$: filteredLogs = ($graphStore.logs ?? []).filter((entry) => {
		const q = runLogFilter.trim().toLowerCase();
		if (!q) return true;
		const parts = [
			String(entry.ts ?? ''),
			String(entry.message ?? ''),
			String(entry.nodeId ?? ''),
			Array.isArray(entry.componentPath) ? entry.componentPath.join(' > ') : ''
		];
		return parts.join(' ').toLowerCase().includes(q);
	});
	$: if (previousEditingContext !== $graphStore.editingContext) {
		if (previousEditingContext === 'graph' && $graphStore.editingContext === 'component') {
			const vp = getViewport();
			graphUiReturnSnapshot = {
				viewport: { x: Number(vp.x ?? 0), y: Number(vp.y ?? 0), zoom: Number(vp.zoom ?? 1) },
				inspectorMode,
				runLogScrollTop: Number(scrollElement?.scrollTop ?? 0),
				runLogFilter
			};
		}
		if (previousEditingContext === 'component' && $graphStore.editingContext === 'graph') {
			void restoreGraphUiReturnSnapshot();
		}
		previousEditingContext = $graphStore.editingContext;
	}
	$: if ((nodes?.length ?? 0) === 0 && currentGraphName !== 'unnamed') {
		currentGraphName = 'unnamed';
	}
	$: hasUnsavedGraphChanges =
		typeof lastSavedGraphSnapshotKey === 'string' &&
		lastSavedGraphSnapshotKey !== currentGraphSnapshotKey;
	$: graphScopedSnapshot = {
		runStatus:
			isComponentEditContext && $graphStore.componentEditSession
				? $graphStore.componentEditSession.snapshot.runStatus
				: $graphStore.runStatus,
		lastRunStatus:
			isComponentEditContext && $graphStore.componentEditSession
				? $graphStore.componentEditSession.snapshot.lastRunStatus
				: $graphStore.lastRunStatus,
		freshness:
			isComponentEditContext && $graphStore.componentEditSession
				? $graphStore.componentEditSession.snapshot.freshness
				: $graphStore.freshness,
		staleNodeCount:
			isComponentEditContext && $graphStore.componentEditSession
				? Number($graphStore.componentEditSession.snapshot.staleNodeCount ?? 0)
				: Number($graphStore.staleNodeCount ?? 0),
		unsaved: hasUnsavedGraphChanges
	};
	$: componentScopedSnapshot = {
		runStatus: $graphStore.runStatus,
		lastRunStatus: $graphStore.lastRunStatus,
		freshness: $graphStore.freshness,
		staleNodeCount: Number($graphStore.staleNodeCount ?? 0),
		unsaved: componentInternalsDirty
	};
	$: scopedHeaderStatus = buildScopedStatus({
		editingContext: $graphStore.editingContext,
		graph: graphScopedSnapshot,
		component: componentScopedSnapshot
	});
	$: graphHeaderStatus = scopedHeaderStatus.statusText;
	$: graphHeaderStatusTone = scopedHeaderStatus.tone;
	$: scopedFreshness =
		$graphStore.editingContext === 'component'
			? String(componentScopedSnapshot.freshness ?? '')
			: String(graphScopedSnapshot.freshness ?? '');
	$: graphHeaderStatusClass = `status graphStatus graphStatus-${graphHeaderStatusTone}${scopedFreshness === 'stale' ? ' graphStatus-stale' : ''}`;
	$: scopedUnsavedChanges = scopedHeaderStatus.unsaved;
	$: projectMenuItems = buildProjectMenuItems($graphStore.editingContext) satisfies ToolbarMenuItem[];
	$: addMenuItems = buildAddMenuItems(hasPresets) satisfies ToolbarMenuItem[];
	$: primarySaveCommandLabel = isComponentEditContext ? 'Save Component Revision' : 'Save Graph';
	$: saveAsComponentCommandLabel = isComponentEditContext ? 'Save as New Component' : 'Save as Component';
	$: commandItems = [
		{ id: 'cmd_new_graph', label: 'New Graph', run: () => void newGraph() },
		{
			id: 'cmd_save_graph',
			label: primarySaveCommandLabel,
			run: () =>
				routePrimarySaveAction($graphStore.editingContext, {
					saveGraph: () => void saveGraphAction(),
					saveComponentRevision: () => void saveComponentRevisionAction()
				})
		},
		{
			id: 'cmd_save_version',
			label: 'Save Version',
			disabled: isComponentEditContext,
			run: () => void saveGraphVersionAction()
		},
		{
			id: 'cmd_save_graph_as',
			label: 'Save Graph As',
			disabled: isComponentEditContext,
			run: () => void saveGraphAsAction()
		},
		{ id: 'cmd_load_graph', label: 'Load Graph', run: () => void loadGraphAction() },
		{ id: 'cmd_delete_graph', label: 'Delete Graph', run: () => void deleteGraphAction() },
		{
			id: 'cmd_save_component',
			label: saveAsComponentCommandLabel,
			disabled: !isComponentEditContext,
			run: () =>
				void saveGraphAsComponent({
					suggestedComponentId: isComponentEditContext ? '' : undefined
				})
		},
		{ id: 'cmd_run', label: 'Run', run: () => void runFromStart() },
		{ id: 'cmd_run_selected', label: 'Run from selected', disabled: !$selectedNode, run: () => void runFromSelected() },
		{ id: 'cmd_add_source', label: 'Add Source', run: () => void addNode('source') },
		{ id: 'cmd_add_transform', label: 'Add Transform', run: () => void addNode('transform') },
		{ id: 'cmd_add_llm', label: 'Add LLM', run: () => void addNode('llm') },
		{ id: 'cmd_add_tool', label: 'Add Tool', run: () => void addNode('tool') },
		{ id: 'cmd_add_component', label: 'Add Component', run: () => void addComponentNodeWithPicker() },
		{ id: 'cmd_import', label: 'Import', run: () => void triggerImportGraphPackageV2() },
		{ id: 'cmd_export', label: 'Export', run: () => void exportGraphPackageV2() }
	] satisfies CommandItem[];
	$: filteredCommandItems = commandItems.filter((item) => {
		const f = commandFilter.trim().toLowerCase();
		if (!f) return true;
		return item.label.toLowerCase().includes(f);
	});

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

	function stableCanonicalValue(value: unknown): unknown {
		if (Array.isArray(value)) return value.map((v) => stableCanonicalValue(v));
		if (value && typeof value === 'object') {
			const obj = value as Record<string, unknown>;
			const out: Record<string, unknown> = {};
			for (const key of Object.keys(obj).sort()) {
				out[key] = stableCanonicalValue(obj[key]);
			}
			return out;
		}
		return value;
	}

	function canonicalGraphSnapshot(
		graphId: string | null | undefined,
		nodeList: Node<PipelineNodeData>[],
		edgeList: Edge<PipelineEdgeData>[]
	): CanonicalGraphSnapshot {
		const nodesCanonical = [...(nodeList ?? [])]
			.map((node) => {
				const data = (node?.data ?? {}) as Record<string, unknown>;
				const ports = (data.ports ?? {}) as Record<string, unknown>;
				return {
					id: String(node?.id ?? ''),
					type: String(node?.type ?? ''),
					position: {
						x: Number(node?.position?.x ?? 0),
						y: Number(node?.position?.y ?? 0)
					},
					data: {
						kind: typeof data.kind === 'string' ? data.kind : undefined,
						label: typeof data.label === 'string' ? data.label : undefined,
						sourceKind: typeof data.sourceKind === 'string' ? data.sourceKind : undefined,
						transformKind: typeof data.transformKind === 'string' ? data.transformKind : undefined,
						llmKind: typeof data.llmKind === 'string' ? data.llmKind : undefined,
						componentKind: typeof data.componentKind === 'string' ? data.componentKind : undefined,
						ports: {
							in: ports.in ?? null,
							out: ports.out ?? null
						},
						params: stableCanonicalValue(data.params ?? {})
					}
				};
			})
			.sort((a, b) => a.id.localeCompare(b.id));

		const edgesCanonical = [...(edgeList ?? [])]
			.map((edge) => ({
				id: String(edge?.id ?? ''),
				source: String(edge?.source ?? ''),
				target: String(edge?.target ?? ''),
				sourceHandle: edge?.sourceHandle ? String(edge.sourceHandle) : null,
				targetHandle: edge?.targetHandle ? String(edge.targetHandle) : null
			}))
			.sort((a, b) => a.id.localeCompare(b.id));

		return {
			graphId: String(graphId ?? ''),
			nodes: nodesCanonical,
			edges: edgesCanonical
		};
	}

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

	async function restoreGraphUiReturnSnapshot(): Promise<void> {
		const snapshot = graphUiReturnSnapshot;
		if (!snapshot) return;
		graphUiReturnSnapshot = null;
		runLogFilter = snapshot.runLogFilter;
		inspectorMode = snapshot.inspectorMode;
		logAutoScrollEnabled = false;
		await tick();
		if (scrollElement) {
			scrollElement.scrollTop = Math.max(0, Number(snapshot.runLogScrollTop ?? 0));
		}
		await setViewport(snapshot.viewport, { duration: 0 });
		await tick();
		logAutoScrollEnabled = true;
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

	function addNode(kind: NodeKind): string {
		const vp = getViewport();
		const centerScreen = { x: window.innerWidth * 0.35, y: window.innerHeight * 0.55 };
		const pos = screenToFlowPosition(centerScreen);

		const id = graphStore.addNode(kind, { x: pos.x, y: pos.y });
		graphStore.selectNode(id);
		setCenter(pos.x, pos.y, { zoom: vp.zoom, duration: 250 });
		return id;
	}

	function formatComponentCatalogLine(index: number, component: ComponentCatalogItem): string {
		const latest = String(component.latestRevisionId ?? '').trim();
		const updated = String(component.updatedAt ?? '').replace('T', ' ').slice(0, 19);
		return `${index + 1}. ${component.componentId}${latest ? `  latest:${latest.slice(0, 12)}` : ''}${updated ? `  updated:${updated}` : ''}`;
	}

	function formatComponentRevisionLine(index: number, revision: ComponentRevisionSummary): string {
		const stamp = String(revision.createdAt ?? '').replace('T', ' ').slice(0, 19);
		const msg = String(revision.message ?? '').trim();
		return `${index + 1}. ${revision.revisionId.slice(0, 14)}${stamp ? `  ${stamp}` : ''}${msg ? `  ${msg}` : ''}`;
	}

	async function pickComponentAndRevision(): Promise<{ componentId: string; revisionId: string } | null> {
		const catalogResult = await graphStore.listComponentCatalog(200, 0);
		if (!(catalogResult as any)?.ok) {
			showToast(
				`Component catalog failed: ${(catalogResult as any)?.error ?? (catalogResult as any)?.reason ?? 'unknown'}`,
				'error'
			);
			return null;
		}
		const components = (((catalogResult as any)?.components ?? []) as ComponentCatalogItem[]).filter(
			(component) => String(component.componentId ?? '').trim().length > 0
		);
		if (components.length === 0) {
			showToast('No components available. Save one from Project -> Save as Component first.', 'warn');
			return null;
		}
		const componentLines = components.map((component, i) => formatComponentCatalogLine(i, component)).join('\n');
		const componentRaw = window.prompt(
			`Add Component:\n${componentLines}\n\nEnter component number (1-${components.length})`,
			'1'
		);
		if (!componentRaw) return null;
		const componentPick = Number(componentRaw);
		if (!Number.isInteger(componentPick) || componentPick < 1 || componentPick > components.length) {
			showToast('Invalid component selection.', 'warn');
			return null;
		}
		const pickedComponent = components[componentPick - 1];
		const componentId = String(pickedComponent.componentId ?? '').trim();
		if (!componentId) {
			showToast('Invalid component id.', 'error');
			return null;
		}
		const revisionsResult = await graphStore.listComponentRevisionHistory(componentId, 50, 0);
		if (!(revisionsResult as any)?.ok) {
			showToast(
				`Component revisions failed: ${(revisionsResult as any)?.error ?? (revisionsResult as any)?.reason ?? 'unknown'}`,
				'error'
			);
			return null;
		}
		const revisions = (((revisionsResult as any)?.revisions ?? []) as ComponentRevisionSummary[]).filter(
			(revision) => String(revision.revisionId ?? '').trim().length > 0
		);
		const fallbackRevisionId = String(pickedComponent.latestRevisionId ?? '').trim();
		if (revisions.length === 0) {
			if (!fallbackRevisionId) {
				showToast(`No revisions found for component ${componentId}.`, 'warn');
				return null;
			}
			return { componentId, revisionId: fallbackRevisionId };
		}
		const revisionLines = revisions.map((revision, i) => formatComponentRevisionLine(i, revision)).join('\n');
		const revisionRaw = window.prompt(
			`Select revision for ${componentId}:\n${revisionLines}\n\nEnter revision number (1-${revisions.length})`,
			'1'
		);
		if (!revisionRaw) return null;
		const revisionPick = Number(revisionRaw);
		if (!Number.isInteger(revisionPick) || revisionPick < 1 || revisionPick > revisions.length) {
			showToast('Invalid revision selection.', 'warn');
			return null;
		}
		const pickedRevision = revisions[revisionPick - 1];
		const revisionId = String(pickedRevision.revisionId ?? '').trim();
		if (!revisionId) {
			showToast('Invalid revision id.', 'error');
			return null;
		}
		return { componentId, revisionId };
	}

	async function addComponentNodeWithPicker(): Promise<void> {
		const picked = await pickComponentAndRevision();
		if (!picked) return;
		const nodeId = addNode('component');
		const applied = await graphStore.applyComponentRevisionToNode(
			nodeId,
			picked.componentId,
			picked.revisionId
		);
		if (!(applied as any)?.ok) {
			graphStore.deleteNode(nodeId);
			showToast(
				`Add Component failed: ${(applied as any)?.error ?? (applied as any)?.reason ?? 'unknown'}`,
				'error'
			);
			return;
		}
		graphStore.selectNode(nodeId);
	}

	function saveSelectedNodeAsPreset(): void {
		const node = $selectedNode;
		if (!node) return;
		const linkedPresetRef = (node.data.meta as any)?.presetRef as
			| { id?: string; name?: string; subtype?: string }
			| undefined;
		const linkedPresetId = String(linkedPresetRef?.id ?? '').trim();
		const linkedPreset = linkedPresetId ? nodePresetStore.getById(linkedPresetId) : null;
		const suggested =
			String(linkedPreset?.name ?? linkedPresetRef?.name ?? '').trim() ||
			String(node.data.label ?? '').trim() ||
			`${node.data.kind} preset`;
		const promptText = linkedPreset
			? `Preset name (overwriting "${linkedPreset.name}" by default):`
			: 'Preset name';
		const name = window.prompt(promptText, suggested)?.trim() ?? '';
		if (!name) return;
		const shouldOverwriteLinked =
			Boolean(linkedPreset) &&
			name.trim().toLowerCase() === String(linkedPreset?.name ?? '').trim().toLowerCase();
		const result = nodePresetStore.upsertFromNodeData(node.data, name, {
			overwritePresetId: shouldOverwriteLinked ? linkedPresetId : null
		});
		if (!result.ok) {
			const err = 'error' in result ? result.error : null;
			if (err === 'identical_preset_exists') {
				showToast('Preset not saved: identical preset already exists.', 'warn');
				return;
			}
			if (err === 'duplicate_name_in_scope') {
				showToast(
					'Preset not saved: that name already exists for this node kind/subtype.',
					'warn'
				);
				return;
			}
			showToast('Could not save preset. Try again.', 'error');
			return;
		}
		const preset = result.preset;
		graphStore.setNodeMeta(node.id, {
			presetRef: {
				id: preset.id,
				name: preset.name,
				subtype: String(preset.subtype),
				appliedAt: new Date().toISOString(),
				appliedParams: structuredClone((node.data.params ?? {}) as Record<string, unknown>),
				appliedPorts: {
					in: node.data.ports?.in ?? null,
					out: node.data.ports?.out ?? null
				}
			}
		});
		if (result.mode === 'updated') {
			showToast(`Preset "${preset.name}" overwritten.`, 'info');
		}
	}

	function showToast(message: string, level: 'info' | 'warn' | 'error' = 'info'): void {
		toastMessage = message;
		toastLevel = level;
		if (toastTimer) clearTimeout(toastTimer);
		toastTimer = setTimeout(() => {
			toastMessage = null;
			toastTimer = null;
		}, 2600);
	}

	function computeComponentSaveApplyCounts(
		state: GraphState,
		componentId: string,
		fromRevisionId: string
	): { matchingCount: number; entryMatchCount: number; allMatchCount: number } {
		const session = state.componentEditSession;
		const cid = String(componentId ?? '').trim();
		const fromRid = String(fromRevisionId ?? '').trim();
		if (!session || !cid || !fromRid) {
			return { matchingCount: 0, entryMatchCount: 0, allMatchCount: 0 };
		}
		const snapshotNodes = Array.isArray(session.snapshot?.nodes) ? session.snapshot.nodes : [];
		const matching = snapshotNodes.filter((n) => {
			if (n.data?.kind !== 'component') return false;
			const ref = (((n.data as any)?.params ?? {}) as any)?.componentRef ?? {};
			const nodeComponentId = String(ref?.componentId ?? '').trim();
			const nodeRevisionId = String(ref?.revisionId ?? '').trim();
			return nodeComponentId === cid && nodeRevisionId === fromRid;
		});
		const entryNodeId = String(session.entryNodeId ?? '').trim();
		const entryMatchCount = entryNodeId && matching.some((n) => String(n.id) === entryNodeId) ? 1 : 0;
		return {
			matchingCount: matching.length,
			entryMatchCount,
			allMatchCount: matching.length
		};
	}

	function chooseComponentSaveApplyScope(scope: ComponentSaveApplyScope): void {
		const resolver = componentSaveApplyResolver;
		componentSaveApplyResolver = null;
		componentSaveApplyModalOpen = false;
		if (resolver) resolver(scope);
	}

	function openComponentSaveApplyModal(
		prompt: ComponentSaveApplyPromptState
	): Promise<ComponentSaveApplyScope> {
		componentSaveApplyPrompt = prompt;
		componentSaveApplyModalOpen = true;
		return new Promise<ComponentSaveApplyScope>((resolve) => {
			componentSaveApplyResolver = resolve;
		});
	}

	function syncPresetBaselineFromNode(
		nodeId: string,
		fallbackRef?: { id: string; name: string; subtype?: string }
	): void {
		const state = get(graphStore) as GraphState;
		const node = (state?.nodes ?? []).find((n: Node<PipelineNodeData>) => n.id === nodeId);
		if (!node) return;
		const currentRef = ((node.data as any)?.meta?.presetRef ??
			fallbackRef ??
			null) as
			| { id?: string; name?: string; subtype?: string }
			| null;
		if (!currentRef?.id || !currentRef?.name) return;
		graphStore.setNodeMeta(nodeId, {
			presetRef: {
				id: String(currentRef.id),
				name: String(currentRef.name),
				subtype: currentRef.subtype ? String(currentRef.subtype) : undefined,
				appliedAt: new Date().toISOString(),
				appliedParams: structuredClone((node.data.params ?? {}) as Record<string, unknown>),
				appliedPorts: {
					in: node.data.ports?.in ?? null,
					out: node.data.ports?.out ?? null
				}
			}
		});
	}

	function applyPresetToNode(nodeId: string, preset: NodePreset): void {
		if (preset.kind === 'source') {
			graphStore.setSourceKind(nodeId, preset.subtype as SourceKind);
		} else if (preset.kind === 'transform') {
			graphStore.setTransformKind(nodeId, preset.subtype as TransformKind);
		} else if (preset.kind === 'llm') {
			graphStore.setLlmKind(nodeId, preset.subtype as LlmKind);
		} else {
			graphStore.setToolProvider(nodeId, preset.subtype as ToolProvider);
		}
		graphStore.updateNodeConfig(nodeId, {
			params: structuredClone(preset.params),
			ports: {
				in: preset.ports?.in ?? null,
				out: preset.ports?.out ?? null
			}
		});
		syncPresetBaselineFromNode(nodeId, {
			id: preset.id,
			name: preset.name,
			subtype: String(preset.subtype)
		});
	}

	function addNodeFromPresetId(presetId: string): void {
		const preset = nodePresetStore.getById(presetId);
		if (!preset) return;
		const vp = getViewport();
		const centerScreen = { x: window.innerWidth * 0.35, y: window.innerHeight * 0.55 };
		const pos = screenToFlowPosition(centerScreen);
		const nodeId = graphStore.addNode(preset.kind, { x: pos.x, y: pos.y });
		applyPresetToNode(nodeId, preset);
		graphStore.selectNode(nodeId);
		nodePresetStore.markUsed(presetId);
		setCenter(pos.x, pos.y, { zoom: vp.zoom, duration: 250 });
	}

	function coercePortType(t: any): PortType | null {
		// normalize anything that might exist in params (e.g., markdown)
		if (t === 'markdown') return 'text';
		if (t === 'table' || t === 'text' || t === 'json' || t === 'binary' || t === 'embeddings')
			return t;
		return null;
	}

	function getComponentOutputType(nodeId: string, sourceHandle?: string | null): PortType | null {
		const n = nodes.find((x) => x.id === nodeId);
		if (!n || n.data.kind !== 'component') return null;
		const handle = String(sourceHandle ?? 'out').trim() || 'out';
		if (handle === 'out') return n.data.ports?.out ?? null;
		const outputs = Array.isArray((n.data as any)?.params?.api?.outputs)
			? ((n.data as any).params.api.outputs as any[])
			: [];
		const decl = outputs.find((o) => String((o as any)?.name ?? '').trim() === handle);
		const pt = String((decl as any)?.portType ?? '').trim().toLowerCase();
		return coercePortType(pt);
	}

	function componentOutputCount(nodeId: string): number {
		const n = nodes.find((x) => x.id === nodeId);
		if (!n || n.data.kind !== 'component') return 0;
		const outputs = Array.isArray((n.data as any)?.params?.api?.outputs)
			? ((n.data as any).params.api.outputs as any[])
			: [];
		return outputs
			.map((o) => String((o as any)?.name ?? '').trim())
			.filter((name) => name.length > 0).length;
	}

	function getType(nodeId: string, whichPort: string, handleId?: string | null): PortType | null {
		const n = nodes.find((x) => x.id === nodeId);
		if (!n) return null;
		if (whichPort === 'out' && n.data.kind === 'component') {
			return getComponentOutputType(nodeId, handleId);
		}

		// const hid = handleId ?? 'in';
		// const t = (n.data as any)?.ports?.[in] ?? (n.data as any)?.inputs?.in;
		return n.data.ports[whichPort];
	}

	function isValidConnection(conn: Connection) {
		if (!conn.source || !conn.target) return false;
		if (conn.source === conn.target) return false;
		if (
			componentOutputCount(conn.source) > 1 &&
			String(conn.sourceHandle ?? 'out').trim() === 'out'
		) {
			return false;
		}

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
		const outPort = getType(conn.source, 'out', conn.sourceHandle);
		const inPort = getType(conn.target, 'in', conn.targetHandle);

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

		const outPort = getType(conn.source, 'out', conn.sourceHandle);
		const inPort = getType(conn.target, 'in', conn.targetHandle);
		if (!outPort || !inPort) return;

		const e: Edge<PipelineEdgeData> = {
			id: `e_${crypto.randomUUID()}`,
			source: conn.source!,
			target: conn.target!,
			sourceHandle: conn.sourceHandle ?? undefined,
			targetHandle: conn.targetHandle ?? undefined,
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

	function runFromStart() {
		void graphStore.runRemote(null, 'from_start', globalCacheMode);
	}

	function runFromSelected() {
		void graphStore.runRemote($selectedNode?.id ?? null, 'from_selected_onward', globalCacheMode);
	}

	async function returnFromComponentEditMode() {
		if (componentInternalsDirty) {
			const raw = window.prompt(
				'Unsaved component edits detected.\n\n1. Save component\n2. Discard changes\n3. Cancel\n\nEnter 1, 2, or 3:',
				'1'
			);
			const decision = parseComponentExitDecision(raw);
			if (decision === 'cancel') return;
			if (decision === 'save') {
				const saved = await saveComponentRevisionAction();
				if (!saved) return;
			}
		}
		const res = graphStore.returnFromComponentEditSession();
		if (!(res as any)?.ok) {
			showToast(`Return failed: ${String((res as any)?.reason ?? 'unknown')}`, 'error');
		}
	}

	function onProjectMenuSelect(actionId: string) {
		dispatchProjectMenuAction(actionId, $graphStore.editingContext, {
			newGraph,
			saveGraph: () => void saveGraphAction(),
			saveComponentRevision: () => void saveComponentRevisionAction(),
			saveVersion: () => void saveGraphVersionAction(),
			saveGraphAs: () => void saveGraphAsAction(),
			loadGraph: () => void loadGraphAction(),
			saveAsComponent: () =>
				void saveGraphAsComponent({
					suggestedComponentId: isComponentEditContext ? '' : undefined
				}),
			importGraph: triggerImportGraphPackageV2,
			exportGraph: () => void exportGraphPackageV2(),
			deleteGraph: () => void deleteGraphAction(),
			reset: resetRunUi
		});
	}

	function onAddMenuSelect(actionId: string) {
		dispatchAddMenuAction(actionId, {
			addSource: () => addNode('source'),
			addTransform: () => addNode('transform'),
			addLlm: () => addNode('llm'),
			addTool: () => addNode('tool'),
			addComponent: () => void addComponentNodeWithPicker(),
			addFromPreset: openAddFromPresetPicker
		});
	}

	function openAddFromPresetPicker() {
		if (!hasPresets) {
			showToast('No presets available.', 'warn');
			return;
		}
		const lines = presets.map((preset, i) => `${i + 1}. ${preset.kind} / ${preset.name} (${preset.subtype})`).join('\n');
		const raw = window.prompt(`Add from preset:\n${lines}\n\nEnter number (1-${presets.length})`, '1');
		if (!raw) return;
		const pick = Number(raw);
		if (!Number.isInteger(pick) || pick < 1 || pick > presets.length) {
			showToast('Invalid preset selection.', 'warn');
			return;
		}
		const pickedPreset = presets[pick - 1];
		addNodeFromPresetId(pickedPreset.id);
	}

	function toggleCommandPalette() {
		commandPaletteOpen = !commandPaletteOpen;
		if (commandPaletteOpen) {
			commandFilter = '';
			queueMicrotask(() => commandFilterInput?.focus());
		}
	}

	function closeCommandPalette() {
		commandPaletteOpen = false;
		commandFilter = '';
	}

	function runCommand(command: CommandItem) {
		if (command.disabled) return;
		command.run();
		closeCommandPalette();
	}

	function onWindowKeyDown(event: KeyboardEvent) {
		const isCtrlK = (event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k';
		if (isCtrlK) {
			event.preventDefault();
			toggleCommandPalette();
			return;
		}
		if (commandPaletteOpen && event.key === 'Escape') {
			event.preventDefault();
			closeCommandPalette();
		}
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
			return;
		}
		if (kind === 'component') {
			clearSubtypeError();
		}
	}

	function newGraph() {
		graphStore.hardResetGraph();
		lastSavedGraphSnapshotKey = null;
		currentGraphName = 'unnamed';
	}

	function formatRevisionLine(index: number, revision: GraphRevisionSummary): string {
		const stamp = String(revision.createdAt ?? '').replace('T', ' ').slice(0, 19);
		const msg = String(revision.message ?? '').trim();
		return `${index + 1}. ${revision.revisionId.slice(0, 10)}  ${stamp}${msg ? `  ${msg}` : ''}`;
	}

	function deriveDefaultComponentApi(
		graphNodes: Node<PipelineNodeData>[],
		graphEdges: Edge<PipelineEdgeData>[]
	): ComponentApiContract {
		const nodeIds = new Set(graphNodes.map((n) => String(n.id)));
		const inDegree = new Map<string, number>();
		const outDegree = new Map<string, number>();
		for (const id of nodeIds) {
			inDegree.set(id, 0);
			outDegree.set(id, 0);
		}
		for (const e of graphEdges) {
			const source = String((e as any)?.source ?? '');
			const target = String((e as any)?.target ?? '');
			if (nodeIds.has(source)) outDegree.set(source, (outDegree.get(source) ?? 0) + 1);
			if (nodeIds.has(target)) inDegree.set(target, (inDegree.get(target) ?? 0) + 1);
		}
		const roots = graphNodes.filter((n) => (inDegree.get(String(n.id)) ?? 0) === 0);
		const leaves = graphNodes.filter((n) => (outDegree.get(String(n.id)) ?? 0) === 0);
		const primaryRoot = roots[0];
		const primaryLeaf = leaves[0];
		const inputPortType = (primaryRoot?.data?.ports?.in ?? null) as PortType | null;
		const outputPortType = (primaryLeaf?.data?.ports?.out ?? null) as PortType | null;
		const inputs: ComponentApiContract['inputs'] =
			inputPortType == null
				? []
				: [
						{
							name: 'in_data',
							portType: inputPortType,
							required: true,
							typedSchema: {
								type:
									inputPortType as ComponentApiContract['inputs'][number]['typedSchema']['type'],
								fields: []
							}
						}
					];
		const outputs: ComponentApiContract['outputs'] =
			outputPortType == null
				? []
				: [
						{
							name: 'out_data',
							portType: outputPortType,
							required: true,
							typedSchema: {
								type:
									outputPortType as ComponentApiContract['outputs'][number]['typedSchema']['type'],
								fields: []
							}
						}
					];
		return { inputs, outputs };
	}

	async function saveGraphAsComponent(options?: { suggestedComponentId?: string }) {
		const current = get(graphStore) as GraphState;
		const currentNodes = (current?.nodes ?? []) as Node<PipelineNodeData>[];
		const currentEdges = (current?.edges ?? []) as Edge<PipelineEdgeData>[];
		const graphPreflight = graphStore.getSavePreflight(current);
		if (!graphPreflight.ok) {
			const detail = (graphPreflight.diagnostics ?? [])
				.filter((d: any) => String(d?.severity ?? 'error').toLowerCase() === 'error')
				.map(
					(d: any, i: number) =>
						`${i + 1}. [${String(d?.code ?? 'VALIDATION')}] (${String(d?.path ?? 'graph')}) ${String(d?.message ?? '')}`
				)
				.slice(0, 8)
				.join('\n');
			window.alert(`Save as Component blocked by graph preflight.\n\n${detail || 'Preflight failed.'}`);
			showToast('Save as Component blocked by graph preflight.', 'error');
			return;
		}
		if (currentNodes.length === 0) {
			showToast('Save as Component failed: graph is empty.', 'warn');
			return;
		}
		if (currentNodes.some((n) => n.data?.kind === 'component')) {
			showToast('Save as Component failed: nested components are not supported in v1.', 'warn');
			return;
		}

		const requestedSuggestion = String(options?.suggestedComponentId ?? '').trim();
		const suggestedId =
			requestedSuggestion ||
			`cmp_${String(current.graphId ?? '').replace(/^graph_/, '')}`.slice(0, 64);
		const componentId = (window.prompt('New Component ID', suggestedId) ?? '').trim();
		if (!componentId) return;
		const revisionIdInput = (window.prompt('Revision ID (optional)', '') ?? '').trim();
		const note = window.prompt('Revision message (optional)', 'save_as_component') ?? '';
		const api = deriveDefaultComponentApi(currentNodes, currentEdges);
		if (api.inputs.length === 0 || api.outputs.length === 0) {
			const proceed = window.confirm(
				`Derived API is incomplete (inputs=${api.inputs.length}, outputs=${api.outputs.length}). Continue anyway?`
			);
			if (!proceed) return;
		}
		try {
			const preflight = await validateComponentRevision({
				schemaVersion: 1,
				graph: {
					nodes: structuredClone(currentNodes) as unknown[],
					edges: structuredClone(currentEdges) as unknown[]
				},
				api,
				configSchema: {}
			});
			const summary = summarizeComponentPreflight(
				Boolean(preflight?.ok),
				preflight?.diagnostics ?? [],
				componentId,
				revisionIdInput || '(new)'
			);
			if (!summary.ok) {
				window.alert(`${summary.headline}\n\n${summary.detail}`);
				showToast('Save as Component blocked by preflight validation.', 'error');
				return;
			}
			if (summary.warningCount > 0) {
				const proceed = window.confirm(`${summary.headline}\n\n${summary.detail}\n\nPublish anyway?`);
				if (!proceed) return;
			}
			const created = await createComponentRevision({
				componentId,
				revisionId: revisionIdInput || undefined,
				message: note,
				schemaVersion: 1,
				graph: {
					nodes: structuredClone(currentNodes) as unknown[],
					edges: structuredClone(currentEdges) as unknown[]
				},
				api,
				configSchema: {}
			});
			showToast(`Saved component ${created.componentId}@${created.revisionId}`, 'info');
		} catch (error) {
			const failure = summarizeComponentPublishFailure(
				error,
				componentId,
				revisionIdInput || '(new)'
			);
			window.alert(`${failure.headline}\n\n${failure.detail}`);
			showToast('Save as Component failed.', 'error');
		}
	}

	async function saveComponentRevisionAction(): Promise<boolean> {
		const state = get(graphStore) as GraphState;
		const session = state.componentEditSession;
		if (!session) {
			await saveGraphAction();
			return true;
		}
		const componentId = String(session.componentId ?? '').trim();
		const baseRevisionId = String(session.revisionId ?? '').trim();
		if (!componentId || !baseRevisionId) {
			showToast('Save Component Revision failed: missing component context.', 'error');
			return false;
		}
		const note = window.prompt('Component revision message (optional)', '') ?? '';
		const currentNodes = (state?.nodes ?? []) as Node<PipelineNodeData>[];
		const currentEdges = (state?.edges ?? []) as Edge<PipelineEdgeData>[];
		try {
			const detail = await getComponentRevision(componentId, baseRevisionId);
			const api = ((detail?.definition?.api ?? { inputs: [], outputs: [] }) as ComponentApiContract);
			const configSchema = (detail?.definition?.configSchema ?? {}) as Record<string, unknown>;
			const preflight = await validateComponentRevision({
				schemaVersion: Number(detail?.schemaVersion ?? 1) || 1,
				graph: {
					nodes: structuredClone(currentNodes) as unknown[],
					edges: structuredClone(currentEdges) as unknown[]
				},
				api,
				configSchema
			});
			const summary = summarizeComponentPreflight(
				Boolean(preflight?.ok),
				preflight?.diagnostics ?? [],
				componentId,
				baseRevisionId
			);
			if (!summary.ok) {
				window.alert(`${summary.headline}\n\n${summary.detail}`);
				showToast('Save Component Revision blocked by preflight validation.', 'error');
				return false;
			}
			if (summary.warningCount > 0) {
				const proceed = window.confirm(`${summary.headline}\n\n${summary.detail}\n\nPublish anyway?`);
				if (!proceed) return false;
			}
			const created = await createComponentRevision({
				componentId,
				parentRevisionId: baseRevisionId,
				message: note,
				schemaVersion: Number(detail?.schemaVersion ?? 1) || 1,
				graph: {
					nodes: structuredClone(currentNodes) as unknown[],
					edges: structuredClone(currentEdges) as unknown[]
				},
				api,
				configSchema
			});
			const nextRevisionId = String(created.revisionId ?? '').trim();
			if (nextRevisionId) {
				const counts = computeComponentSaveApplyCounts(state, componentId, baseRevisionId);
				const scope = await openComponentSaveApplyModal({
					componentId,
					fromRevisionId: baseRevisionId,
					toRevisionId: nextRevisionId,
					matchingCount: counts.matchingCount,
					entryMatchCount: counts.entryMatchCount,
					allMatchCount: counts.allMatchCount
				});
				const applyResult = graphStore.applySavedComponentRevisionToReturnGraph(
					componentId,
					baseRevisionId,
					nextRevisionId,
					scope
				);
				if (!(applyResult as any)?.ok) {
					showToast(
						`Saved component but failed to apply revision scope: ${String((applyResult as any)?.reason ?? 'unknown')}`,
						'error'
					);
					return false;
				}
				const updatedCount = Number((applyResult as any)?.updatedCount ?? 0);
				const matchedCount = Number((applyResult as any)?.matchedCount ?? 0);
				const scopeLabel =
					scope === 'all' ? 'all matching instances' : scope === 'none' ? 'no instances' : 'this instance';
				showToast(
					`Saved ${componentId}@${nextRevisionId} and applied to ${scopeLabel} (${updatedCount}/${matchedCount}).`,
					'info'
				);
			} else {
				showToast('Saved component revision but did not receive a revision id.', 'warn');
			}
			componentEditEntrySnapshotKey = currentGraphSnapshotKey;
			return true;
		} catch (error) {
			const failure = summarizeComponentPublishFailure(error, componentId, baseRevisionId);
			window.alert(`${failure.headline}\n\n${failure.detail}`);
			showToast('Save Component Revision failed.', 'error');
			return false;
		}
	}

	async function acceptInspectorDraftAction(): Promise<void> {
		const validation = graphStore.getInspectorDraftAcceptValidation();
		if (!validation.ok) {
			showToast(`Accept blocked: ${String(validation.errors?.[0] ?? 'validation failed')}`, 'warn');
			return;
		}
		const result = await graphStore.applyInspectorDraft();
		if (!(result as any)?.ok) {
			showToast(
				`Accept blocked: ${String((result as any)?.error ?? (result as any)?.reason ?? 'validation failed')}`,
				'warn'
			);
		}
	}

	async function saveGraphAction() {
		const graphNameInput = window.prompt('Graph name (optional)', '') ?? '';
		const note = window.prompt('Save note (optional)', '') ?? '';
		const graphName = graphNameInput.trim() || undefined;
		const result = await graphStore.saveGraph(note, { graphName });
		if (!(result as any)?.ok) {
			if (String((result as any)?.reason ?? '') === 'preflight_failed') {
				window.alert(`Save Graph blocked by preflight.\n\n${String((result as any)?.error ?? 'Validation failed')}`);
			}
			showToast(`Save Graph failed: ${(result as any)?.error ?? (result as any)?.reason ?? 'unknown'}`, 'error');
			return;
		}
		lastSavedGraphSnapshotKey = currentGraphSnapshotKey;
		const resolvedName = String((result as any)?.graphName ?? graphName ?? '').trim();
		if (resolvedName) currentGraphName = resolvedName;
		showToast(`Saved graph revision ${(result as any).revisionId.slice(0, 10)}`, 'info');
	}

	async function saveGraphVersionAction() {
		const versionName = (window.prompt('Version name', '') ?? '').trim();
		if (!versionName) return;
		const note = window.prompt('Version note (optional)', '') ?? '';
		const result = await graphStore.saveGraphVersion(versionName, note);
		if (!(result as any)?.ok) {
			if (String((result as any)?.reason ?? '') === 'preflight_failed') {
				window.alert(`Save Version blocked by preflight.\n\n${String((result as any)?.error ?? 'Validation failed')}`);
			}
			showToast(`Save Version failed: ${(result as any)?.error ?? (result as any)?.reason ?? 'unknown'}`, 'error');
			return;
		}
		lastSavedGraphSnapshotKey = currentGraphSnapshotKey;
		showToast(`Saved version ${(result as any).versionName ?? versionName}`, 'info');
	}

	async function saveGraphAsAction() {
		const graphName = (window.prompt('New graph name', '') ?? '').trim();
		if (!graphName) return;
		const versionName = (window.prompt('Initial version name (optional)', '') ?? '').trim() || undefined;
		const note = window.prompt('Save note (optional)', '') ?? '';
		const result = await graphStore.saveGraphAs(graphName, note, versionName);
		if (!(result as any)?.ok) {
			if (String((result as any)?.reason ?? '') === 'preflight_failed') {
				window.alert(`Save Graph As blocked by preflight.\n\n${String((result as any)?.error ?? 'Validation failed')}`);
			}
			showToast(`Save Graph As failed: ${(result as any)?.error ?? (result as any)?.reason ?? 'unknown'}`, 'error');
			return;
		}
		lastSavedGraphSnapshotKey = currentGraphSnapshotKey;
		currentGraphName = graphName;
		showToast(`Saved new graph ${graphName}`, 'info');
	}

	function formatGraphLine(index: number, graph: GraphCatalogItem): string {
		const name = String(graph.graphName ?? '').trim() || '(unnamed)';
		const updated = String(graph.updatedAt ?? '').replace('T', ' ').slice(0, 19);
		return `${index + 1}. ${name}  [${graph.graphId.slice(0, 10)}]${updated ? `  ${updated}` : ''}`;
	}

	async function loadGraphAction() {
		const catalog = await graphStore.listGraphs(200, 0);
		if (!(catalog as any)?.ok) {
			showToast(`Load Graph failed: ${(catalog as any)?.error ?? (catalog as any)?.reason ?? 'unknown'}`, 'error');
			return;
		}
		const allGraphs = (((catalog as any)?.graphs ?? []) as GraphCatalogItem[]).filter((g) =>
			String(g.graphId ?? '').trim().length > 0
		);
		const seenGraphIds = new Set<string>();
		const uniqueGraphs = allGraphs.filter((g) => {
			const gid = String(g.graphId ?? '').trim();
			if (!gid || seenGraphIds.has(gid)) return false;
			seenGraphIds.add(gid);
			return true;
		});
		const namedGraphs = uniqueGraphs.filter((g) => String(g.graphName ?? '').trim().length > 0);
		const graphs = namedGraphs.length > 0 ? namedGraphs : uniqueGraphs;
		if (graphs.length === 0) {
			showToast('No saved graphs found.', 'warn');
			return;
		}
		const graphLines = graphs.map((g, i) => formatGraphLine(i, g)).join('\n');
		const graphRaw = window.prompt(`Load graph:\n${graphLines}\n\nEnter number (1-${graphs.length})`, '1');
		if (!graphRaw) return;
		const graphPick = Number(graphRaw);
		if (!Number.isInteger(graphPick) || graphPick < 1 || graphPick > graphs.length) {
			showToast('Invalid graph selection.', 'warn');
			return;
		}
		const pickedGraph = graphs[graphPick - 1];
		const revisionsResult = await graphStore.listGraphRevisionHistoryForGraph(String(pickedGraph.graphId), 50, 0);
		if (!(revisionsResult as any)?.ok) {
			showToast(
				`Load Graph failed: ${(revisionsResult as any)?.error ?? (revisionsResult as any)?.reason ?? 'unknown'}`,
				'error'
			);
			return;
		}
		const revisions = (((revisionsResult as any)?.revisions ?? []) as GraphRevisionSummary[]).filter((r) =>
			String(r.revisionId ?? '').trim().length > 0
		);
		if (revisions.length === 0) {
			showToast('No revisions found for selected graph.', 'warn');
			return;
		}
		const revisionLines = revisions.map((r, i) => formatRevisionLine(i, r)).join('\n');
		const revisionRaw = window.prompt(
			`Load revision:\n${revisionLines}\n\nEnter number (1-${revisions.length})`,
			'1'
		);
		if (!revisionRaw) return;
		const revisionPick = Number(revisionRaw);
		if (!Number.isInteger(revisionPick) || revisionPick < 1 || revisionPick > revisions.length) {
			showToast('Invalid revision selection.', 'warn');
			return;
		}
		const selectedRevision = revisions[revisionPick - 1];
		const loaded = await graphStore.loadGraphRevision(String(pickedGraph.graphId), String(selectedRevision.revisionId));
		if (!(loaded as any)?.ok) {
			showToast(`Load Graph failed: ${(loaded as any)?.error ?? (loaded as any)?.reason ?? 'unknown'}`, 'error');
			return;
		}
		lastSavedGraphSnapshotKey = currentGraphSnapshotKey;
		currentGraphName = String((loaded as any)?.graphName ?? pickedGraph.graphName ?? '').trim() || 'unnamed';
		showToast('Loaded graph revision.', 'info');
	}

	async function deleteGraphAction() {
		const graphId = String($graphStore.graphId ?? '').trim();
		if (!graphId) {
			showToast('Delete Graph failed: missing graph id.', 'error');
			return;
		}
		const deleteAll = window.confirm(
			'Delete entire graph and all revisions?\n\nSelect "OK" to delete all revisions.\nSelect "Cancel" to choose deleting only the latest revision.'
		);
		if (deleteAll) {
			const confirmAll = window.confirm(
				`Confirm delete graph ${graphId} and all its revisions. This cannot be undone.`
			);
			if (!confirmAll) return;
			const result = await graphStore.deleteGraph(graphId);
			if (!(result as any)?.ok) {
				showToast(
					`Delete Graph failed: ${(result as any)?.error ?? (result as any)?.reason ?? 'unknown'}`,
					'error'
				);
				return;
			}
			newGraph();
			showToast('Graph deleted (all revisions).', 'info');
			return;
		}

		const history = await graphStore.listGraphRevisionHistoryForGraph(graphId, 1, 0);
		if (!(history as any)?.ok) {
			showToast(
				`Delete revision failed: ${(history as any)?.error ?? (history as any)?.reason ?? 'unknown'}`,
				'error'
			);
			return;
		}
		const latest = (((history as any)?.revisions ?? []) as GraphRevisionSummary[])[0];
		if (!latest?.revisionId) {
			showToast('No revisions found to delete.', 'warn');
			return;
		}
		const confirmLatest = window.confirm(
			`Delete latest revision ${String(latest.revisionId).slice(0, 10)} for this graph?`
		);
		if (!confirmLatest) return;
		const result = await graphStore.deleteGraphRevision(graphId, String(latest.revisionId));
		if (!(result as any)?.ok) {
			showToast(
				`Delete revision failed: ${(result as any)?.error ?? (result as any)?.reason ?? 'unknown'}`,
				'error'
			);
			return;
		}
		if ((result as any)?.deleted?.graphDeleted) {
			newGraph();
			showToast('Latest revision deleted. Graph had no revisions left and was removed.', 'info');
			return;
		}
		const remaining = await graphStore.hydrateLatestGraphFromBackend();
		if (!(remaining as any)?.ok) {
			showToast('Revision deleted, but reload failed; start new graph or load manually.', 'warn');
			return;
		}
		lastSavedGraphSnapshotKey = currentGraphSnapshotKey;
		showToast('Latest revision deleted and graph reloaded to previous revision.', 'info');
	}

	async function exportGraphPackageV2() {
		try {
			const graphId = String($graphStore.graphId ?? '').trim();
			if (!graphId) {
				showToast('Export failed: missing graph id.', 'error');
				return;
			}
			const exported = await exportGraphPackage(graphId, {
				includeArtifacts: false,
				includeSchemas: true
			});
			const payload = JSON.stringify(exported.package, null, 2);
			const blob = new Blob([payload], { type: 'application/json' });
			const href = URL.createObjectURL(blob);
			const a = document.createElement('a');
			a.href = href;
			a.download = `${graphId}.aipgraph`;
			document.body.appendChild(a);
			a.click();
			a.remove();
			URL.revokeObjectURL(href);
			showToast('Exported .aipgraph package.', 'info');
		} catch (error) {
			showToast(`Export failed: ${String(error)}`, 'error');
		}
	}

	function triggerImportGraphPackageV2() {
		importFileInput?.click();
	}

	async function onImportGraphPackageV2(event: Event) {
		const input = event.currentTarget as HTMLInputElement | null;
		const file = input?.files?.[0] ?? null;
		if (!file) return;
		try {
			const text = await file.text();
			const parsed = JSON.parse(text);
			const suggestedId =
				String(parsed?.manifest?.source?.graphId ?? '').trim() ||
				String($graphStore.graphId ?? '').trim();
			const targetGraphId =
				(window.prompt('Import graph id (optional)', suggestedId) ?? '').trim() || undefined;
			const imported = await importGraphPackage({
				package: parsed,
				targetGraphId,
				message: `import:file:${file.name}`
			});
			const loaded = graphStore.loadGraphDocument(imported.graph, imported.graphId);
			if (!(loaded as any)?.ok) {
				showToast('Import failed: invalid graph payload.', 'error');
				return;
			}
			lastSavedGraphSnapshotKey = null;
			const warnings = imported?.migrationReport?.warnings ?? [];
			if (warnings.length > 0) {
				showToast(`Imported with warnings: ${warnings.join(' | ')}`, 'warn');
			} else {
				showToast(`Imported graph ${imported.graphId}`, 'info');
			}
		} catch (error) {
			showToast(`Import failed: ${String(error)}`, 'error');
		} finally {
			if (input) input.value = '';
		}
	}

	function deleteSelectedPresetRef(): void {
		const node = $selectedNode;
		const presetRef = (node?.data as any)?.meta?.presetRef as { id?: string; name?: string } | undefined;
		const presetId = String(presetRef?.id ?? '').trim();
		if (!node || !presetId) return;
		const name = String(presetRef?.name ?? presetId);
		const ok = window.confirm(`Delete preset "${name}"? This does not delete node parameters.`);
		if (!ok) return;
		nodePresetStore.delete(presetId);
		graphStore.setNodeMeta(node.id, { presetRef: undefined });
	}

	function beginInspectorSplit(pair: InspectorSplitPair, event: PointerEvent) {
		const paneA = pair === 'top_env' ? inspectorTopPaneEl : environmentPaneEl;
		const paneB = pair === 'top_env' ? environmentPaneEl : runLogsPaneEl;
		if (!paneA || !paneB) return;
		const aRect = paneA.getBoundingClientRect();
		const bRect = paneB.getBoundingClientRect();
		if (aRect.height <= 0 || bRect.height <= 0) return;
		activeInspectorSplit = pair;
		splitStartY = event.clientY;
		splitPaneAStartPx = aRect.height;
		splitPaneBStartPx = bRect.height;
	}

	function onInspectorSplitMove(event: PointerEvent) {
		if (!activeInspectorSplit) return;
		const minPanePx = 96;
		const pairStartPx = splitPaneAStartPx + splitPaneBStartPx;
		if (pairStartPx <= 0) return;
		const dy = event.clientY - splitStartY;
		const nextPaneA = Math.max(minPanePx, Math.min(pairStartPx - minPanePx, splitPaneAStartPx + dy));
		const nextPaneB = pairStartPx - nextPaneA;

		if (activeInspectorSplit === 'top_env') {
			const total = inspectorTopWeight + environmentWeight;
			if (total <= 0) return;
			inspectorTopWeight = (total * nextPaneA) / pairStartPx;
			environmentWeight = total - inspectorTopWeight;
		} else {
			const total = environmentWeight + runLogsWeight;
			if (total <= 0) return;
			environmentWeight = (total * nextPaneA) / pairStartPx;
			runLogsWeight = total - environmentWeight;
		}
	}

	function onInspectorSplitUp() {
		activeInspectorSplit = null;
	}

	async function refreshWorkspaceEnvironmentPanel(): Promise<void> {
		envProfilesLoading = true;
		envProfilesError = null;
		try {
			const payload = await listEnvProfiles();
			envProfiles = Array.isArray(payload.profiles) ? payload.profiles : [];
		} catch (error) {
			envProfilesError = String((error as Error)?.message ?? error ?? 'Failed to load environment profiles.');
			envProfiles = [];
		} finally {
			envProfilesLoading = false;
		}
	}

	async function installWorkspaceProfile(profileId: string): Promise<void> {
		const pid = String(profileId ?? '').trim();
		if (!pid) return;
		envInstallPendingByProfile = { ...envInstallPendingByProfile, [pid]: true };
		envProfilesError = null;
		try {
			await installEnvProfile(pid);
			showToast(`Profile '${pid}' install completed.`, 'info');
			await refreshWorkspaceEnvironmentPanel();
		} catch (error) {
			const message = String((error as Error)?.message ?? error ?? 'Profile install failed.');
			envProfilesError = message;
			showToast(message, 'error');
		} finally {
			const next = { ...envInstallPendingByProfile };
			delete next[pid];
			envInstallPendingByProfile = next;
		}
	}

	onDestroy(() => {
		if (subtypeErrorTimer) clearTimeout(subtypeErrorTimer);
		if (toastTimer) clearTimeout(toastTimer);
	});

	onMount(async () => {
		await refreshPortCapabilitiesFromBackend();
		await refreshWorkspaceEnvironmentPanel();
		try {
			const config = await getGlobalCacheConfig();
			globalCacheMode = (config.mode ??
				(Boolean(config.enabled) ? 'default_on' : 'force_off')) as GlobalCacheMode;
		} catch (error) {
			console.warn('Failed to load global cache config', error);
		}
		const draftInfo = getGraphDraftInfo();
		const hasDraft = Boolean(String(draftInfo.updatedAt ?? '').trim());
		const draftStamp = String(draftInfo.updatedAt ?? '').trim();
		const previouslyPromptedFor = sessionStorage.getItem(DRAFT_RECOVERY_PROMPT_SESSION_KEY) ?? '';
		const shouldPromptForDraft = hasDraft && draftStamp.length > 0 && previouslyPromptedFor !== draftStamp;
		const recoverDraft =
			shouldPromptForDraft &&
			window.confirm(
				`Recover local draft${draftInfo.updatedAt ? ` from ${draftInfo.updatedAt}` : ''}?\n\n` +
					`Select "Cancel" to load latest saved graph from backend instead.`
			);
		if (shouldPromptForDraft) {
			sessionStorage.setItem(DRAFT_RECOVERY_PROMPT_SESSION_KEY, draftStamp);
		}
		if (!recoverDraft) {
			try {
				const hydrated = await graphStore.hydrateLatestGraphFromBackend();
				if ((hydrated as any)?.ok) {
					graphStore.clearDraft();
					currentGraphName = String((hydrated as any)?.graphName ?? '').trim() || 'unnamed';
					console.log(
						`[graph-v2-read] hydrated graphId=${(hydrated as any).graphId} revisionId=${(hydrated as any).revisionId}`
					);
				}
			} catch (error) {
				console.warn('Failed to hydrate graph from backend revision store', error);
			}
		}
		await tick();
		lastSavedGraphSnapshotKey = currentGraphSnapshotKey;
	});
</script>

<svelte:window
	on:pointermove={onInspectorSplitMove}
	on:pointerup={onInspectorSplitUp}
	on:keydown={onWindowKeyDown}
/>

<div class="layout">
	<div class="flow">
		{#if toastMessage}
			<div class={`toast toast-${toastLevel}`} role="status" aria-live="polite">{toastMessage}</div>
		{/if}
		<div class="topbar" role="toolbar" aria-label="Graph toolbar">
			<div class="toolbarZone projectActions">
				<ToolbarMenu
					label="Project"
					items={projectMenuItems}
					onSelect={onProjectMenuSelect}
					menuAriaLabel="Project actions"
				/>
			</div>

			<div class="toolbarZone runActions">
				<button class="primary runBtn" on:click={runFromStart}>▶ Run</button>
				<button class="runSecondary" on:click={runFromSelected} disabled={!$selectedNode}>
					Run from selected
				</button>
			</div>

			<div class="toolbarZone statusIndicators">
				<span class={graphHeaderStatusClass}
					>{statusScopeLabel}: {graphHeaderStatus}{scopedUnsavedChanges ? ' + Unsaved changes' : ''}</span
				>
				{#if isComponentEditContext}
					<button class="runSecondary" on:click={returnFromComponentEditMode}>
						Return to graph
					</button>
				{/if}
				<label class="cacheToggle">
					<span>Cache:</span>
					<select
						aria-label="Cache mode"
						value={globalCacheMode}
						disabled={globalCachePending}
						on:change={async (event) => {
							const nextMode = (event.currentTarget as HTMLSelectElement).value as GlobalCacheMode;
							globalCachePending = true;
							try {
								const result = await setGlobalCacheConfig({ mode: nextMode });
								globalCacheMode = (result.mode ??
									(Boolean(result.enabled) ? 'default_on' : 'force_off')) as GlobalCacheMode;
							} catch (error) {
								(event.currentTarget as HTMLSelectElement).value = globalCacheMode;
								console.warn('Failed to update global cache config', error);
							} finally {
								globalCachePending = false;
							}
						}}
					>
						<option value="default_on">{GlobalCacheModeLabels.default_on}</option>
						<option value="force_off">{GlobalCacheModeLabels.force_off}</option>
						<option value="force_on">{GlobalCacheModeLabels.force_on}</option>
					</select>
				</label>
			</div>

			<div class="toolbarZone addActions">
				<button class="commandEntry" on:click={toggleCommandPalette} aria-label="Open command palette">
					Ctrl+K
				</button>
				<ToolbarMenu
					label="+ Add"
					items={addMenuItems}
					onSelect={onAddMenuSelect}
					align="right"
					menuAriaLabel="Add node actions"
				/>
			</div>
		</div>
		{#if commandPaletteOpen}
			<div class="commandPaletteBackdrop" role="dialog" aria-modal="true" aria-label="Command palette">
				<div class="commandPaletteCard">
					<div class="commandPaletteHead">
						<b>Command Palette</b>
						<button class="commandClose" on:click={closeCommandPalette} aria-label="Close command palette">
							✕
						</button>
					</div>
					<input
						bind:this={commandFilterInput}
						class="commandFilter"
						placeholder="Type a command..."
						bind:value={commandFilter}
					/>
					<div class="commandList">
						{#if filteredCommandItems.length === 0}
							<div class="commandEmpty">No commands</div>
						{:else}
							{#each filteredCommandItems as cmd (cmd.id)}
								<button
									type="button"
									class="commandItem"
									disabled={cmd.disabled}
									on:click={() => runCommand(cmd)}
								>
									{cmd.label}
								</button>
							{/each}
						{/if}
					</div>
				</div>
			</div>
		{/if}
		{#if componentSaveApplyModalOpen}
			<div class="commandPaletteBackdrop" role="dialog" aria-modal="true" aria-label="Apply component revision">
				<div class="componentSaveApplyModal">
					<div class="componentSaveApplyHead">
						<b>Apply saved component revision</b>
					</div>
					<div class="componentSaveApplyBody">
						<div>
							<span class="mono">{componentSaveApplyPrompt.componentId}@{componentSaveApplyPrompt.fromRevisionId}</span>
							{' -> '}
							<span class="mono">{componentSaveApplyPrompt.componentId}@{componentSaveApplyPrompt.toRevisionId}</span>
						</div>
						<div class="componentSaveApplyHint">Choose where to apply this new revision in the current graph.</div>
					</div>
					<div class="componentSaveApplyActions">
						<button type="button" class="runSecondary" on:click={() => chooseComponentSaveApplyScope('none')}>
							None (0)
						</button>
						<button type="button" class="primary" on:click={() => chooseComponentSaveApplyScope('one')}>
							This instance ({componentSaveApplyPrompt.entryMatchCount})
						</button>
						<button type="button" class="runSecondary" on:click={() => chooseComponentSaveApplyScope('all')}>
							All matching ({componentSaveApplyPrompt.allMatchCount})
						</button>
					</div>
				</div>
			</div>
		{/if}
		<input
			bind:this={importFileInput}
			type="file"
			accept=".aipgraph,application/json,.json"
			style="display:none"
			on:change={onImportGraphPackageV2}
		/>
		<OutputModal bind:open={outputOpen} nodeId={outputNodeId} />

		<SvelteFlow
			bind:nodes
			edges={displayEdges}
			{nodeTypes}
			deleteKey={['Delete']}
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
		<div
			class="inspectorPane inspectorTop"
			bind:this={inspectorTopPaneEl}
			style={nodeInspectorCollapsed ? 'flex: 0 0 auto;' : `flex: ${inspectorTopWeight} 1 0;`}
		>
			<!-- <h3>Inspector</h3> -->

			{#if $selectedNode}
				<div class="card editorCard">
						<div class="head">
							<div style="min-width:0;display:flex;align-items:center;gap:8px;">
								<button
									type="button"
									class="tabBtn sectionToggle"
									title={nodeInspectorCollapsed ? 'Expand Node Inspector' : 'Collapse Node Inspector'}
									on:click={() => (nodeInspectorCollapsed = !nodeInspectorCollapsed)}
								>
									<span class="sectionToggleIcon" aria-hidden="true">{nodeInspectorCollapsed ? '▸' : '▾'}</span>
								</button>
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
								{#if selectedComponentHasUpdate}
									<span class="pill pill-update" title={`Latest available revision: ${selectedComponentLatestRevisionId}`}>
										update {selectedComponentLatestRevisionId}
									</span>
								{/if}
							</div>
						</div>
						{#if !nodeInspectorCollapsed}
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
														: $selectedNode.data.kind === 'tool'
															? selectedToolProvider
															: selectedComponentKind
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
										{:else if $selectedNode.data.kind === 'component'}
											<option value="graph_component">graph_component</option>
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
												mimeType={inputMetaByArtifactId[inputPreviewArtifactId]?.mimeType}
												portType={inputMetaByArtifactId[inputPreviewArtifactId]?.contract}
												preview={undefined}
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
									<button on:click={saveSelectedNodeAsPreset} disabled={!$selectedNode}>
										Save Preset
									</button>
									<button
										on:click={deleteSelectedPresetRef}
										disabled={!selectedPresetRefExists}
										title={selectedPresetRefExists
											? 'Delete linked preset'
											: 'No linked preset to delete'}
									>
										Delete Preset
									</button>
									<button
										class="primary"
										disabled={inspectorAcceptDisabled}
										title={inspectorAcceptTooltip}
										on:click={() => void acceptInspectorDraftAction()}
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
								{#if inspectorSystemNotice}
									<div class="inspectorSystemNote" aria-live="polite">{inspectorSystemNotice}</div>
								{/if}
							{/if}
						{/if}
					</div>
			{:else}
				<p>Click a node to edit it.</p>
			{/if}
		</div>
		{#if !nodeInspectorCollapsed && !environmentCollapsed}
			<button
				type="button"
				class="inspectorSplitter"
				aria-label="Resize Node Inspector and Environment panels"
				on:pointerdown={(event) => beginInspectorSplit('top_env', event)}
			></button>
		{/if}
		<div
			class="inspectorPane inspectorEnv"
			bind:this={environmentPaneEl}
			style={environmentCollapsed ? 'flex: 0 0 auto;' : `flex: ${environmentWeight} 1 0;`}
		>
			<div class="envPanel">
					<div class="envPanelHead">
						<div class="sectionHeadTitle">
							<h3>Environment</h3>
							<button
								type="button"
								class="tabBtn sectionToggle"
								title={environmentCollapsed ? 'Expand Environment' : 'Collapse Environment'}
								on:click={() => (environmentCollapsed = !environmentCollapsed)}
							>
								<span class="sectionToggleIcon" aria-hidden="true">{environmentCollapsed ? '▸' : '▾'}</span>
							</button>
						</div>
						{#if !environmentCollapsed}
							<button
								class="tabBtn envRefreshBtn"
								on:click={() => void refreshWorkspaceEnvironmentPanel()}
								disabled={envProfilesLoading}
							>
								{envProfilesLoading ? 'Refreshing...' : 'Refresh'}
							</button>
						{/if}
					</div>
					{#if !environmentCollapsed}
						<div class="envPanelSummary">
							{envProfilesInstalledCount}/{envProfiles.length} installed
							{#if envProfilesMissingCount > 0}
								<span class="envMissing">({envProfilesMissingCount} missing)</span>
							{/if}
						</div>
						{#if envProfilesError}
							<div class="envPanelError">{envProfilesError}</div>
						{/if}
						<div class="envProfileList">
							{#if !envProfilesLoading && envProfiles.length === 0}
								<div class="envProfileEmpty">No profiles available.</div>
							{/if}
							{#each envProfiles as profile (profile.profileId)}
								<div class="envProfileRow">
									<div class="envProfileMeta">
										<div class="envProfileTitle">
											<span class="mono">{profile.profileId}</span>
											<span class={`pill ${profile.installed ? 'st-succeeded' : 'st-stale'}`}>
												{profile.installed ? 'installed' : 'missing'}
											</span>
										</div>
										{#if !profile.installed && profile.missingPackages.length > 0}
											<div class="envProfileMissing">
												missing: {profile.missingPackages.join(', ')}
											</div>
										{/if}
										{#if profile.platformNotes?.length}
											<div class="envProfileNotes">
												{profile.platformNotes.join(' ')}
											</div>
										{/if}
									</div>
									<button
										class="tabBtn envInstallBtn"
										disabled={Boolean(envInstallPendingByProfile[profile.profileId])}
										on:click={() => void installWorkspaceProfile(profile.profileId)}
									>
										{#if envInstallPendingByProfile[profile.profileId]}
											Installing...
										{:else if profile.installed}
											Reinstall
										{:else}
											Install
										{/if}
									</button>
								</div>
							{/each}
						</div>
					{/if}
			</div>
		</div>
		{#if !environmentCollapsed && !runLogsCollapsed}
			<button
				type="button"
				class="inspectorSplitter"
				aria-label="Resize Environment and Run Logs panels"
				on:pointerdown={(event) => beginInspectorSplit('env_logs', event)}
			></button>
		{/if}
		<div
			class="inspectorPane inspectorLogs"
			bind:this={runLogsPaneEl}
			style={runLogsCollapsed ? 'flex: 0 0 auto;' : `flex: ${runLogsWeight} 1 0;`}
		>
			<div class="sectionHeadTitle">
				<h3>Run Logs</h3>
				<button
					type="button"
					class="tabBtn sectionToggle"
					title={runLogsCollapsed ? 'Expand Run Logs' : 'Collapse Run Logs'}
					on:click={() => (runLogsCollapsed = !runLogsCollapsed)}
				>
					<span class="sectionToggleIcon" aria-hidden="true">{runLogsCollapsed ? '▸' : '▾'}</span>
				</button>
			</div>
			{#if !runLogsCollapsed}
				<input
					class="logFilterInput"
					placeholder="Filter logs..."
					aria-label="Filter run logs"
					bind:value={runLogFilter}
				/>
				<div class="logs" bind:this={scrollElement}>
					{#each filteredLogs as l (l.id)}
						<div class={`log ${l.level}`}>
							<span class="ts">{l.ts}</span>
							<span class="msg">
								{#if l.componentPath?.length}
									<span class="nid">[Component: {l.componentPath.join(' > ')}]</span>
								{/if}
								{#if l.nodeId}
									<span class="nid">[{l.nodeId}]</span>
								{/if}
								{l.message}
							</span>
						</div>
					{/each}
				</div>
			{/if}
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
		position: relative;
	}

	.toast {
		position: absolute;
		right: 14px;
		top: 12px;
		z-index: 6;
		padding: 8px 10px;
		border-radius: 10px;
		font-size: 12px;
		border: 1px solid #2a3550;
		background: #0f1626;
		color: #e6e6e6;
		box-shadow: 0 8px 20px rgba(0, 0, 0, 0.28);
	}

	.toast-info {
		border-color: #2a4b78;
	}

	.toast-warn {
		border-color: #7a5b1f;
		background: #1a150a;
	}

	.toast-error {
		border-color: #7a2a2a;
		background: #1f0f12;
	}

	.topbar {
		padding: 10px;
		display: flex;
		align-items: center;
		gap: 12px;
		border-bottom: 1px solid #1f2430;
		background: #0b0c10;
		color: #e6e6e6;
		flex-wrap: nowrap;
		overflow: visible;
		position: relative;
		z-index: 25;
	}

	.toolbarZone {
		display: flex;
		gap: 8px;
		align-items: center;
		flex: 0 0 auto;
	}

	.projectActions {
		padding-right: 10px;
		border-right: 1px solid #222c3f;
	}

	.runActions {
		padding-right: 10px;
		border-right: 1px solid #222c3f;
	}

	.statusIndicators {
		min-width: 0;
		flex: 1 1 auto;
		justify-content: flex-start;
	}

	.addActions {
		margin-left: auto;
	}

	.status {
		opacity: 0.68;
		font-size: 13px;
		white-space: nowrap;
	}

	.graphStatus-running {
		color: var(--color-status-info);
		opacity: 0.98;
	}

	.graphStatus-succeeded {
		color: var(--color-status-success);
		opacity: 0.98;
	}

	.graphStatus-failed {
		color: var(--color-status-danger);
		opacity: 0.98;
	}

	.graphStatus-cancelled {
		color: var(--color-status-warning);
		opacity: 0.98;
	}

	.graphStatus-never_run {
		color: var(--color-status-muted);
		opacity: 0.85;
	}

	.graphStatus-stale {
		color: var(--color-status-warning);
	}

	.cacheToggle {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		font-size: 13px;
		opacity: 0.75;
		white-space: nowrap;
	}

	.cacheToggle select {
		border: 1px solid var(--color-control-border);
		background: var(--color-control-bg);
		color: var(--color-control-text);
		padding: 4px 8px;
		border-radius: 8px;
		font-size: 12px;
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
		background: #14305f;
	}

	.runBtn {
		min-width: 110px;
	}

	.commandEntry {
		padding: 6px 8px;
		font-size: 12px;
		opacity: 0.8;
	}
	button:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}
	button:hover:not(:disabled) {
		filter: brightness(1.1);
	}

	.commandPaletteBackdrop {
		position: absolute;
		inset: 0;
		background: rgba(2, 5, 10, 0.58);
		z-index: 8;
		display: grid;
		place-items: start center;
		padding-top: 64px;
	}

	.commandPaletteCard {
		width: min(600px, calc(100% - 20px));
		border-radius: 12px;
		border: 1px solid #2a3550;
		background: #0f1626;
		box-shadow: 0 12px 35px rgba(0, 0, 0, 0.4);
		padding: 10px;
		display: grid;
		gap: 8px;
	}

	.componentSaveApplyModal {
		width: min(560px, calc(100% - 20px));
		border-radius: 12px;
		border: 1px solid #2a3550;
		background: #0f1626;
		box-shadow: 0 12px 35px rgba(0, 0, 0, 0.4);
		padding: 12px;
		display: grid;
		gap: 10px;
	}

	.componentSaveApplyHead {
		display: flex;
		align-items: center;
		justify-content: space-between;
		font-size: 13px;
	}

	.componentSaveApplyBody {
		display: grid;
		gap: 8px;
		font-size: 13px;
	}

	.componentSaveApplyHint {
		opacity: 0.8;
	}

	.componentSaveApplyActions {
		display: flex;
		align-items: center;
		justify-content: flex-end;
		gap: 8px;
	}

	.commandPaletteHead {
		display: flex;
		align-items: center;
		justify-content: space-between;
		font-size: 13px;
	}

	.commandClose {
		font-size: 12px;
		padding: 4px 7px;
	}

	.commandFilter {
		border: 1px solid #2a3550;
		background: #0c1220;
		color: #e6e6e6;
		padding: 8px 10px;
		border-radius: 8px;
		font-size: 13px;
	}

	.commandList {
		display: grid;
		gap: 6px;
		max-height: 300px;
		overflow: auto;
	}

	.commandItem {
		text-align: left;
		padding: 8px 10px;
	}

	.commandEmpty {
		padding: 10px;
		opacity: 0.7;
		font-size: 13px;
	}

	.inspectorSystemNote {
		margin-top: 6px;
		font-size: 12px;
		opacity: 0.82;
		color: #9db3da;
	}

	@media (max-width: 1260px) {
		.graphStatus {
			display: none;
		}
	}

	@media (max-width: 1080px) {
		.runActions :global(.menuRoot) {
			display: none;
		}
	}

	@media (max-width: 920px) {
		.cacheToggle {
			display: none;
		}
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

	.inspectorPane {
		min-height: 0;
		overflow: hidden;
		display: flex;
		flex-direction: column;
	}

	.inspectorTop {
		min-height: 0;
		overflow: hidden;
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

	.inspectorEnv,
	.inspectorLogs {
		min-height: 0;
	}

	.envPanel {
		border: 1px solid #1f2430;
		border-radius: 12px;
		background: #0f1115;
		padding: 10px;
		min-height: 0;
		display: flex;
		flex-direction: column;
	}

	.envPanelHead {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
	}

	.sectionHead,
	.sectionHeadTitle {
		display: flex;
		align-items: center;
		gap: 8px;
	}

	.sectionHead {
		justify-content: space-between;
	}

	.sectionToggle {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		padding: 4px 8px;
	}

	.sectionToggleIcon {
		font-size: 11px;
		opacity: 0.85;
	}

	.envPanelHead h3 {
		margin: 0;
	}

	.envRefreshBtn,
	.envInstallBtn {
		padding: 4px 8px;
		font-size: 12px;
	}

	.envPanelSummary {
		margin-top: 6px;
		font-size: 12px;
		opacity: 0.85;
	}

	.envMissing {
		color: #f2cc60;
	}

	.envPanelError {
		margin-top: 6px;
		font-size: 12px;
		color: #ff7b72;
	}

	.envProfileList {
		display: flex;
		flex-direction: column;
		gap: 8px;
		max-height: 180px;
		overflow: auto;
		margin-top: 8px;
		padding-right: 2px;
	}

	.envProfileRow {
		display: flex;
		align-items: flex-start;
		justify-content: space-between;
		gap: 8px;
		border: 1px solid #1c2335;
		border-radius: 8px;
		padding: 8px;
		background: #0c1220;
	}

	.envProfileMeta {
		min-width: 0;
		flex: 1;
	}

	.envProfileTitle {
		display: flex;
		align-items: center;
		gap: 6px;
	}

	.envProfileMissing,
	.envProfileNotes,
	.envProfileEmpty {
		margin-top: 4px;
		font-size: 12px;
		opacity: 0.8;
	}

	.envProfileMissing {
		color: #f2cc60;
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

	.pill-update {
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

	.logFilterInput {
		border: 1px solid #2a3550;
		background: #0c1220;
		color: #e6e6e6;
		padding: 6px 8px;
		border-radius: 8px;
		font-size: 12px;
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
		border: 1px solid var(--color-control-border);
		background: var(--color-control-bg);
		color: var(--color-control-text);
	}

	.nodeTypeSwitch option {
		background: var(--color-control-option-bg);
		color: var(--color-control-option-text);
	}

	.nodeTypeSwitch:focus {
		outline: none;
		border-color: #3b82f6;
		box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2);
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
