<script lang="ts">
	import { onDestroy, onMount, tick } from 'svelte';
	import { get } from 'svelte/store';
	import { SvelteFlow, Background, Controls, MarkerType, useSvelteFlow } from '@xyflow/svelte';
	import '@xyflow/svelte/dist/style.css';

	import type { Node, Edge, Connection } from '@xyflow/svelte';

	import { nodeTypes } from '$lib/flow/nodeTypes';
	import type { PipelineNodeData, PipelineEdgeData, NodeKind, PayloadType } from '$lib/flow/types';
	import type { SourceKind, LlmKind, TransformKind, ToolProvider, ComponentKind } from '$lib/flow/types/paramsMap';
	import {
		graphStore,
		selectedNode,
		edgeSchemaDiagnostics,
		deriveNodeIoForData
	} from '$lib/flow/store/graphStore';
	import type { GraphState, InputResolution, SaveConsistencyMismatch } from '$lib/flow/store/graphStore';
	import NodeInspector from '$lib/flow/components/NodeInspector.svelte';
	import ThemedSelect, { type ThemedSelectOption } from '$lib/flow/components/ui/ThemedSelect.svelte';
	import OutputModal from '$lib/flow/components/OutputModal.svelte';
	import ArtifactViewer from './components/ArtifactViewer.svelte';
	import ToolbarMenu from './components/ToolbarMenu.svelte';
	import {
		buildAddMenuItems,
		buildProjectMenuItems,
		dispatchAddMenuAction,
		dispatchProjectMenuAction,
		pauseResumeToolbarVisibility,
		routePrimarySaveAction
	} from './components/flowToolbarModel';
	import { getHeaderCachePill, getHeaderNodeStatus } from './components/inspectorCachePill';
	import { buildHeaderContextLabels } from './components/headerContext';
	import { buildScopedStatus } from './components/statusScope';
	import { parseComponentExitDecision } from './components/componentExitGuard';
	import { getArtifactMetaUrl } from '$lib/flow/client/runs';
	import type {
		ExperimentAdaptiveDecision,
		ExperimentBottleneckNode,
		ExperimentFailureTaxonomyItem,
		ExperimentNodeTrendPoint,
		ExperimentRunTrendPoint,
		ExperimentSlaBreach,
		RegressionAlert
	} from '$lib/flow/client/runs';
	import { getGlobalCacheConfig, setGlobalCacheConfig } from '$lib/flow/client/runs';
	import {
		getExperimentFailureTaxonomy,
		getExperimentAdaptiveDecisions,
		getExperimentBottlenecks,
		getExperimentNodeTrends,
		getExperimentRunSummary,
		getExperimentRunTrends,
		getExperimentRegressions,
		getRunTransitions,
		getExperimentSlaBreaches
	} from '$lib/flow/client/runs';
	import { listEnvProfiles, installEnvProfile, type EnvProfileStatus } from '$lib/flow/client/envProfiles';
	import {
		listRuntimeEnvVars,
		updateRuntimeEnvVars,
		type RuntimeEnvVar
	} from '$lib/flow/client/envVars';
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
	import {
		getLlmEditorCommitMode,
		getSourceEditorCommitMode,
		getToolEditorCommitMode,
		getTransformEditorCommitMode
	} from '$lib/flow/editorCommitPolicy';
	import { graphSemanticSnapshotKey, isGraphSemanticDirty } from '$lib/flow/store/graphSemanticSnapshot';
	import { nodePresetStore } from '$lib/flow/store/nodePresetStore';
	import type { NodePreset } from '$lib/flow/store/nodePresetStore';
	import type { ToolbarMenuItem } from './components/toolbarMenu';
	import {
		DSML_STARTER_TEMPLATES,
		getOperationPresetsForKind,
		getStarterTemplateById,
		recommendNextStep,
		type GuidedOperationPreset,
		type GuidedRecommendation
	} from './components/dsmlGuidedUx';
	import { refreshSchemaCapabilitiesFromBackend } from '$lib/flow/schemaCapabilities';
	import {
		buildAdaptiveComponentBreakdown,
		buildTrendSparkline,
		buildRunMonitorAdaptiveDecisionRows,
		buildRunMonitorEdgeRows,
		buildRunMonitorNodeRows,
		buildRunMonitorTransitionRows,
		filterRunMonitorAdaptiveDecisionRows,
		filterAndSortRunMonitorNodes,
		filterRunMonitorTransitionRows,
		pickRunMonitorRegressionPairFromHistory,
		preferredMonitorEdgeFocusNodeId,
		resolveRunMonitorRegressionPair,
		summarizeAdaptiveDecisionRows,
		type RunMonitorAdaptiveDecisionRow,
		type RunMonitorAdaptiveComponentBreakdownItem,
		type RunMonitorAdaptiveDecisionSummary,
		type RunMonitorAdaptiveModeFilter,
		type RunMonitorAdaptiveSeverityFilter,
		type RunMonitorFilter,
		type RunMonitorRegressionPair,
		type RunMonitorSort,
		type RunMonitorTransitionFilter,
		type RunMonitorTransitionRow,
		type RunMonitorTrendSparkline
	} from '$lib/flow/components/runMonitorModel';

	const { screenToFlowPosition, setCenter, getViewport, setViewport } = useSvelteFlow();

	let outputOpen = false;
	let outputNodeId: string | null = null;
	let saveConsistencyModalOpen = false;
	let saveConsistencyModalContext = '';
	let saveConsistencyModalError = '';
	let saveConsistencyModalData: SaveConsistencyMismatch | null = null;
	const PORT_TYPE_LEGEND_MINIMIZED_KEY = 'flow.portTypeLegend.minimized.v1';
	const PORT_TYPE_LEGEND_POS_X_KEY = 'flow.portTypeLegend.posX.v1';
	const PORT_TYPE_LEGEND_POS_Y_KEY = 'flow.portTypeLegend.posY.v1';
	const PORT_TYPE_LEGEND_DEFAULT_POS = { x: 18, y: 74 };
	let portTypeLegendMinimized = false;
	let portTypeLegendPos = { ...PORT_TYPE_LEGEND_DEFAULT_POS };
	let flowPaneEl: HTMLDivElement | null = null;
	let topbarEl: HTMLDivElement | null = null;
	let portTypeLegendEl: HTMLDivElement | null = null;
	let isDraggingPortTypeLegend = false;
	let portTypeLegendDragOffset = { x: 0, y: 0 };

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
	$: {
		const logCount = Array.isArray($graphStore.logs) ? $graphStore.logs.length : 0;
		if (scrollElement && logAutoScrollEnabled && logCount > lastObservedLogCount) {
			void scrollToBottom('auto');
		}
		lastObservedLogCount = logCount;
	}

	$: displayEdges = edges.map((e) => {
		const diag = ($edgeSchemaDiagnostics as Record<string, any> | undefined)?.[String(e.id ?? '')] ?? null;
		const schemaClass =
			diag?.severity === 'error'
				? 'edge-schema-error'
				: diag?.severity === 'warning'
					? 'edge-schema-warning'
					: '';
		const linkKindClass =
			String(((e.data as any)?.linkKind ?? (e.data as any)?.link_kind ?? 'data_link')).trim().toLowerCase() ===
			'control_link'
				? 'edge-link-control'
				: '';
		const title = diag
			? `${String(diag.message ?? '')}${Array.isArray(diag.suggestions) && diag.suggestions.length > 0 ? `\n${diag.suggestions.join('\n')}` : ''}`
			: undefined;
		return {
			...e,
			class: `edge edge-${e.data?.exec ?? 'idle'} ${schemaClass} ${linkKindClass}`.trim(),
			title
		};
	});

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
					params: $selectedNode.data.params ?? {}
				},
				null,
				2
			);
		}
	//ViewArtifact
	type InspectorMode = 'edit' | 'inputs' | 'output';
	let inspectorMode: InspectorMode = 'edit';
	let inspectorTopWeight = 2;
	let environmentWeight = 1;
	let runLogsWeight = 1;
	let inspectorCollapseRestore: { top: number; logs: number } | null = null;
	let inspectorTopPaneEl: HTMLElement | null = null;
	let environmentPaneEl: HTMLElement | null = null;
	let runLogsPaneEl: HTMLElement | null = null;
	type InspectorSplitPair = 'top_env' | 'env_logs';
	let activeInspectorSplit: InspectorSplitPair | null = null;
	let splitStartY = 0;
	let splitPaneAStartPx = 0;
	let splitPaneBStartPx = 0;
	let activeInspectorPaneAEl: HTMLElement | null = null;
	let activeInspectorPaneBEl: HTMLElement | null = null;
	let activeInspectorPaneANoScrollPx = Number.POSITIVE_INFINITY;
	let activeInspectorPaneBNoScrollPx = Number.POSITIVE_INFINITY;
	let splitEnvLogsBypassEnvironment = false;
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
	let canUndo = false;
	let canRedo = false;
	let envProfiles: EnvProfileStatus[] = [];
	let envProfilesLoading = false;
	let envProfilesError: string | null = null;
	let envInstallPendingByProfile: Record<string, boolean> = {};
	let runtimeEnvVars: RuntimeEnvVar[] = [];
	let runtimeEnvLoading = false;
	let runtimeEnvError: string | null = null;
	let runtimeEnvSaving: Record<string, boolean> = {};
	let runtimeEnvDraftByName: Record<string, string> = {};
	let runtimeEnvFilter = '';
	let runtimeEnvRevealSensitive = false;
	let previousEditingContext: 'graph' | 'component' = 'graph';
	let logAutoScrollEnabled = true;
	let lastObservedLogCount = 0;
	let programmaticLogScroll = false;
	let programmaticLogScrollUnlockHandle: ReturnType<typeof setTimeout> | null = null;
	let nodeInspectorCollapsed = false;
	let environmentCollapsed = false;
	let runLogsCollapsed = false;
	let runMonitorNodeFilter: RunMonitorFilter = 'all';
	let runMonitorNodeSort: RunMonitorSort = 'depth_desc';
	let runMonitorSlideoutOpen = true;
	let runMonitorSlideoutWidth = 380;
	let runMonitorResizeActive = false;
	let runMonitorResizeStartX = 0;
	let runMonitorResizeStartWidth = 380;
	let runMonitorMonitorWeight = 1;
	let runMonitorEnvWeight = 1;
	let runMonitorNodesWeight = 1;
	let runMonitorEdgesWeight = 1;
	let runMonitorPaneEl: HTMLElement | null = null;
	let runMonitorEnvPaneEl: HTMLElement | null = null;
	let runMonitorNodesPaneEl: HTMLElement | null = null;
	let runMonitorEdgesPaneEl: HTMLElement | null = null;
	let runMonitorAdaptiveDecisionRows: RunMonitorAdaptiveDecisionRow[] = [];
	let runMonitorAdaptiveDecisionRowsLive: RunMonitorAdaptiveDecisionRow[] = [];
	let runMonitorAdaptiveDecisionRowsHistory: RunMonitorAdaptiveDecisionRow[] = [];
	let runMonitorAdaptiveRowsVisible: RunMonitorAdaptiveDecisionRow[] = [];
	let runMonitorAdaptiveDecisionSelectedKey = '';
	let selectedAdaptiveDecision: RunMonitorAdaptiveDecisionRow | null = null;
	let selectedAdaptiveDecisionPrevious: RunMonitorAdaptiveDecisionRow | null = null;
	let selectedAdaptiveDecisionComponents: RunMonitorAdaptiveComponentBreakdownItem[] = [];
	let selectedAdaptiveCapRows: Array<{
		key: string;
		hard: string;
		min: string;
		proposed: string;
		effective: string;
		changed: string;
	}> = [];
	let runMonitorAdaptiveDecisionSummary: RunMonitorAdaptiveDecisionSummary = {
		total: 0,
		enforced: 0,
		byMode: {},
		bySeverity: { low: 0, medium: 0, high: 0 }
	};
	let runMonitorAdaptiveSparklineRows: RunMonitorAdaptiveDecisionRow[] = [];
	let runMonitorAdaptiveSparkline: RunMonitorTrendSparkline | null = null;
	let runMonitorAdaptiveHoverIndex = -1;
	let runMonitorAdaptiveHoverPoint:
		| { x: number; y: number; value: number; createdAt: string }
		| null = null;
	let runMonitorAdaptiveModeFilter: RunMonitorAdaptiveModeFilter = 'all';
	let runMonitorAdaptiveSeverityFilter: RunMonitorAdaptiveSeverityFilter = 'all';
	let runMonitorAdaptiveChangedOnly = false;
	let runMonitorAdaptiveMinScore = 0;
	let runMonitorAdaptiveDataSource: 'live' | 'history' = 'live';
	let runMonitorAdaptiveHistoryRowsRaw: ExperimentAdaptiveDecision[] = [];
	let runMonitorAdaptiveHistorySort: 'created_desc' | 'created_asc' | 'impact_desc' = 'created_desc';
	const runMonitorAdaptiveDataSourceOptions: ThemedSelectOption[] = [
		{ value: 'live', label: 'Live run' },
		{ value: 'history', label: 'History window' }
	];
	const runMonitorAdaptiveHistorySortOptions: ThemedSelectOption[] = [
		{ value: 'created_desc', label: 'Newest' },
		{ value: 'created_asc', label: 'Oldest' },
		{ value: 'impact_desc', label: 'Impact desc' }
	];
	let runMonitorTrendNodeOptions: Array<{ id: string; label: string }> = [];
	type RunMonitorSplitPair = 'monitor_env' | 'nodes_edges';
	let activeRunMonitorSplit: RunMonitorSplitPair | null = null;
	let runMonitorSplitStartY = 0;
	let runMonitorSplitPaneAStartPx = 0;
	let runMonitorSplitPaneBStartPx = 0;
	let activeRunMonitorPaneAEl: HTMLElement | null = null;
	let activeRunMonitorPaneBEl: HTMLElement | null = null;
	let activeRunMonitorPaneANoScrollPx = Number.POSITIVE_INFINITY;
	let activeRunMonitorPaneBNoScrollPx = Number.POSITIVE_INFINITY;
	let inspectorSidebarWidth = 460;
	let inspectorResizeActive = false;
	let inspectorResizeStartX = 0;
	let inspectorResizeStartWidth = 460;
	let runMonitorPrefsGraphId = '';
	let runMonitorSectionCollapsed = false;
	let runMonitorShowHistory = false;
	let runMonitorAdaptiveModeOverride: 'default' | 'off' | 'observe' | 'enforce' = 'default';
	let runMonitorAdaptiveEnvMode: 'off' | 'observe' | 'enforce' = 'off';
	let runMonitorAdaptiveEffectiveMode: 'off' | 'observe' | 'enforce' = 'off';
	let runMonitorRegressionAlerts: RegressionAlert[] = [];
	let runMonitorRegressionLoading = false;
	let runMonitorRegressionError: string | null = null;
	let runMonitorRegressionRunId = '';
	let runMonitorRegressionBaselineRunId = '';
	let runMonitorRegressionRefreshKey = '';
	let runMonitorRegressionPair: RunMonitorRegressionPair = {
		runId: '',
		baselineRunId: ''
	};
	let runMonitorRegressionRunOverride = '';
	let runMonitorRegressionBaselineOverride = '';
	let runMonitorRegressionAutoKey = '';
	let runMonitorRegressionTypeFilter: 'all' | 'latency' | 'failure' = 'all';
	let runMonitorRegressionSeverityFilter: 'all' | 'low' | 'medium' | 'high' = 'all';
	let runMonitorRegressionSort: 'default' | 'impact_desc' | 'impact_asc' = 'default';
	let runMonitorRegressionSelectedIndex = -1;
	let selectedRegressionAlert: RegressionAlert | null = null;
	let runMonitorRegressionSummaryRefreshKey = '';
	let runMonitorRegressionCurrentSummary: Record<string, unknown> | null = null;
	let runMonitorRegressionBaselineSummary: Record<string, unknown> | null = null;
	let runMonitorRegressionSummaryLoading = false;
	let runMonitorRegressionSummaryError: string | null = null;
	let runMonitorTrendMetric: 'p95Ms' | 'p50Ms' | 'avgMs' | 'maxMs' | 'count' = 'p95Ms';
	let runMonitorTrendNodeId = '';
	let runMonitorRunTrendPoints: ExperimentRunTrendPoint[] = [];
	let runMonitorSelectedRunTrendId = '';
	let runMonitorSelectedRunSummary: Record<string, unknown> | null = null;
	let runMonitorSelectedRunSummaryLoading = false;
	let runMonitorSelectedRunSummaryError: string | null = null;
	let runMonitorAnalyticsStartAt = '';
	let runMonitorAnalyticsEndAt = '';
	let runMonitorAnalyticsOffset = 0;
	let runMonitorRunTrendSort: 'created_asc' | 'created_desc' | 'runtime_desc' = 'created_asc';
	let runMonitorNodeTrendSort: 'created_asc' | 'created_desc' | 'value_desc' = 'created_asc';
	let runMonitorTrendPoints: ExperimentNodeTrendPoint[] = [];
	let runMonitorTrendSparkline: RunMonitorTrendSparkline | null = null;
	let runMonitorTrendHoverIndex = -1;
	let runMonitorTrendHoverPoint:
		| { x: number; y: number; value: number; createdAt: string }
		| null = null;
	let runMonitorTrendHoverCreatedAt = '';
	let runMonitorSlaThresholdMs = 2000;
	let runMonitorSlaBreaches: ExperimentSlaBreach[] = [];
	let runMonitorFailureTaxonomy: ExperimentFailureTaxonomyItem[] = [];
	let runMonitorBottleneckNodes: ExperimentBottleneckNode[] = [];
	let runMonitorBottleneckSort: 'score_desc' | 'score_asc' | 'p95_desc' = 'score_desc';
	let runMonitorTransitions: RunMonitorTransitionRow[] = [];
	let runMonitorTransitionsVisible: RunMonitorTransitionRow[] = [];
	let runMonitorTransitionsLoading = false;
	let runMonitorTransitionsError: string | null = null;
	let runMonitorTransitionRunId = '';
	let runMonitorTransitionsRefreshKey = '';
	let runMonitorTransitionsAutoKey = '';
	let runMonitorTransitionFilter: RunMonitorTransitionFilter = 'all';
	let runMonitorAnalyticsLoading = false;
	let runMonitorAnalyticsError: string | null = null;
	let runMonitorAnalyticsRefreshKey = '';
	let slideoutEnvironmentCollapsed = true;
	let guidedDsmlDismissed = true;
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
	let toastActionLabel: string | null = null;
	let toastAction: (() => void) | null = null;
	let lastSavedGraphSnapshotKey: string | null = null;
	let lastSavedGraphSemanticSnapshotKey: string | null = null;
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
							modelKind?: string;
							taskKind?: string;
							componentKind?: string;
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
	$: runtimeEnvRows = runtimeEnvVars.filter((row) => {
		const q = String(runtimeEnvFilter ?? '').trim().toLowerCase();
		if (!q) return true;
		return (
			String(row.name ?? '').toLowerCase().includes(q) ||
			String(row.category ?? '').toLowerCase().includes(q) ||
			String(row.description ?? '').toLowerCase().includes(q)
		);
	});
	$: {
		const envModeRaw = String(
			runtimeEnvVars.find((row) => String(row?.name ?? '').trim() === 'RUNNER_ADAPTIVE_MODE')?.value ??
				'off'
		)
			.trim()
			.toLowerCase();
		if (envModeRaw === 'observe' || envModeRaw === 'enforce') {
			runMonitorAdaptiveEnvMode = envModeRaw;
		} else {
			runMonitorAdaptiveEnvMode = 'off';
		}
		runMonitorAdaptiveEffectiveMode =
			runMonitorAdaptiveModeOverride === 'default'
				? runMonitorAdaptiveEnvMode
				: (runMonitorAdaptiveModeOverride as 'off' | 'observe' | 'enforce');
	}
	$: {
		const gid = String($graphStore.graphId ?? '').trim() || 'default';
		if (gid !== runMonitorPrefsGraphId) {
			runMonitorPrefsGraphId = gid;
			loadRunMonitorSlideoutPrefs(gid);
			loadInspectorSidebarPrefs(gid);
		}
	}

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
	let lastPinAutoClearNotice = '';
	$: {
		if (typeof window !== 'undefined' && inspectorSystemNotice.startsWith('[Pin cleared]')) {
			if (inspectorSystemNotice !== lastPinAutoClearNotice) {
				lastPinAutoClearNotice = inspectorSystemNotice;
				window.alert(inspectorSystemNotice);
			}
		}
	}
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
	$: selectedNodeKind = ($selectedNode?.data?.kind ?? null) as NodeKind | null;
	$: guidedNextStep = recommendNextStep(nodes, selectedNodeKind);
	$: guidedPresetsForSelectedKind = getOperationPresetsForKind(selectedNodeKind);
	$: guidedInlinePreset = guidedPresetsForSelectedKind[0] ?? null;
	$: selectedComponentKind =
		(((inspectorParams as any)?.componentKind ??
			($selectedNode?.data as any)?.componentKind ??
			'graph_component') as ComponentKind);
	$: hideInspectorApplyRow =
		inspectorMode === 'edit' &&
		(() => {
			const kind = $selectedNode?.data?.kind;
			if (kind === 'transform') return getTransformEditorCommitMode(selectedTransformKind) === 'immediate';
			if (kind === 'source') return getSourceEditorCommitMode(selectedSourceKind) === 'immediate';
			if (kind === 'llm' || kind === 'model') return getLlmEditorCommitMode(selectedLlmKind) === 'immediate';
			if (kind === 'tool') return getToolEditorCommitMode(selectedToolProvider) === 'immediate';
			return false;
		})();
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
	$: selectedFreezeMode = (() => {
		const freeze = (($selectedNode?.data as any)?.meta?.freeze ?? null) as any;
		if (!freeze || freeze.enabled !== true) return null;
		const mode = String(freeze.mode ?? '').trim().toLowerCase();
		return mode === 'per_run' || mode === 'sticky' ? mode : null;
	})();
	$: selectedKindPillClass =
		selectedFreezeMode === 'sticky'
			? 'pill pill-freeze-sticky'
			: selectedFreezeMode === 'per_run'
				? 'pill pill-freeze-per-run'
				: 'pill';
	$: selectedKindPillText =
		selectedFreezeMode === 'sticky'
			? `${$selectedNode?.data?.kind ?? ''} #`
			: `${$selectedNode?.data?.kind ?? ''}`;
	$: selectedPinPillClass =
		selectedFreezeMode === 'sticky'
			? 'pill pinBtn pinSticky active'
			: selectedFreezeMode === 'per_run'
				? 'pill pinBtn active'
				: 'pill pinBtn';
	$: selectedPinPillText = selectedFreezeMode === 'sticky' ? 'pin #' : 'pin';
		$: hasInputs = Boolean($selectedNode && deriveNodeIoForData($selectedNode.data).in != null);
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
	$: nodeLabelById = new Map(
		(nodes ?? []).map((node) => [
			String(node.id ?? ''),
			String((node.data as any)?.label ?? (node.data as any)?.kind ?? '').trim()
		])
	);
	$: edgeById = new Map((edges ?? []).map((edge) => [String(edge.id ?? ''), edge]));
	$: isComponentEditContext = $graphStore.editingContext === 'component';
	$: editingComponentName = String($graphStore.componentEditSession?.componentId ?? '').trim() || 'unknown';
	$: headerContextLabels = buildHeaderContextLabels({
		editingContext: $graphStore.editingContext,
		graphName: currentGraphName,
		componentName: editingComponentName
	});
	$: statusScopeLabel = headerContextLabels.scopeLabel;
	$: currentGraphSnapshotKey = JSON.stringify(canonicalGraphSnapshot($graphStore.graphId, nodes, edges));
	$: currentGraphSemanticSnapshotKey = graphSemanticSnapshotKey($graphStore.graphId, nodes, edges);
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
		const nodeName = String(nodeLabelById.get(String(entry.nodeId ?? '')) ?? '');
		const edgeTag = runLogEdgeTag(entry);
		const parts = [
			String(entry.ts ?? ''),
			String(entry.message ?? ''),
			String(entry.nodeId ?? ''),
			nodeName,
			edgeTag,
			Array.isArray(entry.componentPath) ? entry.componentPath.join(' > ') : ''
		];
		return parts.join(' ').toLowerCase().includes(q);
	});

	function nodeToken(input: string): string {
		const stripped = String(input ?? '').replace(/[^A-Za-z0-9]+/g, '').trim();
		if (stripped.length > 0) return stripped.slice(0, 5);
		const fallback = String(input ?? '').replace(/\s+/g, '').trim();
		return fallback.slice(0, 5);
	}

	function edgeIdFromMessage(message: string): string | null {
		const src = String(message ?? '');
		const direct = src.match(/\be_[A-Za-z0-9-]{6,}\b/);
		if (direct?.[0]) return direct[0];
		const quoted = src.match(/edge\s+['"]([^'"]+)['"]/i);
		if (quoted?.[1]) return quoted[1];
		return null;
	}

	function runLogNodeName(entry: { nodeId?: string }): string {
		const nodeId = String(entry.nodeId ?? '').trim();
		if (!nodeId) return '';
		return String(nodeLabelById.get(nodeId) ?? '').trim();
	}

	function runLogEdgeTag(entry: { edgeId?: string; message?: string }): string {
		const edgeId = String(entry.edgeId ?? '').trim() || String(edgeIdFromMessage(String(entry.message ?? '')) ?? '').trim();
		if (!edgeId) return '';
		const edge = edgeById.get(edgeId);
		if (!edge) return edgeId;
		const sourceId = String((edge as any).source ?? '').trim();
		const targetId = String((edge as any).target ?? '').trim();
		const sourceName = String(nodeLabelById.get(sourceId) ?? sourceId).trim();
		const targetName = String(nodeLabelById.get(targetId) ?? targetId).trim();
		const abbrev = `${nodeToken(sourceName)}-${nodeToken(targetName)}`;
		return `${edgeId} ${abbrev}`;
	}
	$: warningSummaryRows = Object.values(($graphStore.queueRuntime?.warningSummary ?? {}) as Record<string, any>)
		.filter((row) => Number((row as any)?.count ?? 0) > 1)
		.sort(
			(a, b) =>
				String((b as any)?.updatedAt ?? '').localeCompare(String((a as any)?.updatedAt ?? '')) ||
				String((a as any)?.warningKey ?? '').localeCompare(String((b as any)?.warningKey ?? ''))
		);
	$: warningSummaryTotalCount = warningSummaryRows.reduce(
		(total, row) => total + Math.max(0, Number((row as any)?.count ?? 0)),
		0
	);
	$: runMonitorGlobalStalled = Boolean(($graphStore.queueRuntime?.schedulerSnapshot as any)?.stalled ?? false);
	$: runMonitorNodeRows = buildRunMonitorNodeRows({
		nodes: ($graphStore.nodes ?? []) as any,
		edges: ($graphStore.edges ?? []) as any,
		nodeBindings: ($graphStore.nodeBindings ?? {}) as any,
		queueRuntime: ($graphStore.queueRuntime ?? {}) as any
	});
	$: runMonitorNodeRowsVisible = filterAndSortRunMonitorNodes(
		runMonitorNodeRows,
		runMonitorNodeFilter,
		runMonitorNodeSort,
		runMonitorGlobalStalled
	);
	$: runMonitorEdgeRows = buildRunMonitorEdgeRows({
		nodes: ($graphStore.nodes ?? []) as any,
		edges: ($graphStore.edges ?? []) as any,
		queueRuntime: ($graphStore.queueRuntime ?? {}) as any
	});
	$: runMonitorAdaptiveDecisionRowsLive = buildRunMonitorAdaptiveDecisionRows(
		($graphStore.queueRuntime ?? {}) as any
	);
	$: runMonitorAdaptiveDecisionRowsHistory = buildRunMonitorAdaptiveDecisionRows({
		adaptiveDecisions: runMonitorAdaptiveHistoryRowsRaw
	} as any);
	$: runMonitorAdaptiveDecisionRows =
		runMonitorAdaptiveDataSource === 'history'
			? runMonitorAdaptiveDecisionRowsHistory
			: runMonitorAdaptiveDecisionRowsLive;
	$: if (
		runMonitorAdaptiveDecisionRows.length > 0 &&
		!runMonitorAdaptiveDecisionRows.some(
			(row) => `${row.at}:${row.runId}` === runMonitorAdaptiveDecisionSelectedKey
		)
	) {
		runMonitorAdaptiveDecisionSelectedKey = `${runMonitorAdaptiveDecisionRows[0].at}:${runMonitorAdaptiveDecisionRows[0].runId}`;
	}
	$: selectedAdaptiveDecision =
		runMonitorAdaptiveDecisionRows.find(
			(row) => `${row.at}:${row.runId}` === runMonitorAdaptiveDecisionSelectedKey
		) ?? null;
	$: selectedAdaptiveDecisionPrevious = (() => {
		if (!selectedAdaptiveDecision) return null;
		const selectedKey = `${selectedAdaptiveDecision.at}:${selectedAdaptiveDecision.runId}`;
		const index = runMonitorAdaptiveDecisionRows.findIndex(
			(row) => `${row.at}:${row.runId}` === selectedKey
		);
		if (index < 0) return null;
		return runMonitorAdaptiveDecisionRows[index + 1] ?? null;
	})();
	$: selectedAdaptiveDecisionComponents = buildAdaptiveComponentBreakdown(
		selectedAdaptiveDecision?.explanation?.components ?? []
	);
	$: selectedAdaptiveCapRows = (() => {
		if (!selectedAdaptiveDecision) return [];
		const hardCaps = selectedAdaptiveDecision.hardCaps ?? {};
		const minCaps = selectedAdaptiveDecision.minCaps ?? {};
		const proposedCaps = selectedAdaptiveDecision.proposedCaps ?? {};
		const effectiveCaps = selectedAdaptiveDecision.effectiveCaps ?? {};
		const changedCaps = selectedAdaptiveDecision.changedCaps ?? {};
		const keys = Array.from(
			new Set([
				...Object.keys(hardCaps),
				...Object.keys(minCaps),
				...Object.keys(proposedCaps),
				...Object.keys(effectiveCaps)
			])
		).sort((a, b) => a.localeCompare(b));
		return keys.map((key) => {
			const hard = hardCaps[key];
			const min = minCaps[key];
			const proposed = proposedCaps[key];
			const effective = effectiveCaps[key];
			const changed = changedCaps[key];
			return {
				key,
				hard: Number.isFinite(Number(hard)) ? String(Number(hard)) : '-',
				min: Number.isFinite(Number(min)) ? String(Number(min)) : '-',
				proposed: Number.isFinite(Number(proposed)) ? String(Number(proposed)) : '-',
				effective: Number.isFinite(Number(effective)) ? String(Number(effective)) : '-',
				changed:
					changed && Number.isFinite(Number((changed as any).from)) && Number.isFinite(Number((changed as any).to))
						? `${Number((changed as any).from)}->${Number((changed as any).to)}`
						: '-'
			};
		});
	})();
	$: runMonitorAdaptiveMinScore = Math.max(
		0,
		Math.min(100, Number.isFinite(Number(runMonitorAdaptiveMinScore)) ? Number(runMonitorAdaptiveMinScore) : 0)
	);
	$: runMonitorAdaptiveRowsVisible = filterRunMonitorAdaptiveDecisionRows(
		runMonitorAdaptiveDecisionRows,
		runMonitorAdaptiveModeFilter,
		runMonitorAdaptiveSeverityFilter,
		runMonitorAdaptiveChangedOnly,
		runMonitorAdaptiveMinScore
	);
	$: runMonitorAdaptiveDecisionSummary = summarizeAdaptiveDecisionRows(
		runMonitorAdaptiveDecisionRows
	);
	$: runMonitorAdaptiveSparklineRows = runMonitorAdaptiveRowsVisible.slice(0, 60).slice().reverse();
	$: runMonitorAdaptiveSparkline = buildTrendSparkline(
		runMonitorAdaptiveSparklineRows.map((row) => ({
			createdAt: String(row.at ?? ''),
			value: Number(row.explanation.score ?? NaN)
		})),
		{ width: 520, height: 72 }
	);
	$: runMonitorAdaptiveHoverPoint =
		runMonitorAdaptiveSparkline &&
		runMonitorAdaptiveHoverIndex >= 0 &&
		runMonitorAdaptiveHoverIndex < runMonitorAdaptiveSparkline.points.length
			? runMonitorAdaptiveSparkline.points[runMonitorAdaptiveHoverIndex]
			: null;
	$: runMonitorBlockedCount = runMonitorNodeRows.filter((row) => row.isBlocked).length;
	$: runMonitorWaitingCount = runMonitorNodeRows.filter((row) => row.isWaiting).length;
	$: runMonitorHistoryRows = (
		Array.isArray(($graphStore.queueRuntime as any)?.runHistory)
			? ((($graphStore.queueRuntime as any)?.runHistory ?? []) as Array<Record<string, unknown>>)
			: []
	)
		.slice()
		.reverse()
		.slice(0, 20);
	$: runMonitorRegressionPair = resolveRunMonitorRegressionPair(runMonitorHistoryRows as any, {
		runId: runMonitorRegressionRunOverride,
		baselineRunId: runMonitorRegressionBaselineOverride
	});
	$: selectedRegressionAlert =
		runMonitorRegressionSelectedIndex >= 0 &&
		runMonitorRegressionSelectedIndex < runMonitorRegressionAlerts.length
			? runMonitorRegressionAlerts[runMonitorRegressionSelectedIndex]
			: null;
	$: if (!selectedRegressionAlert) {
		runMonitorRegressionSummaryRefreshKey = '';
		runMonitorRegressionCurrentSummary = null;
		runMonitorRegressionBaselineSummary = null;
		runMonitorRegressionSummaryError = null;
	}
	$: {
		const autoKey = [
			String(runMonitorRegressionRunId ?? '').trim(),
			String(runMonitorRegressionBaselineRunId ?? '').trim(),
			String(runMonitorRegressionSelectedIndex ?? -1),
			String(selectedRegressionAlert?.reasonCode ?? ''),
			String(selectedRegressionAlert?.nodeId ?? selectedRegressionAlert?.errorCode ?? '')
		].join('|');
		if (
			autoKey !== runMonitorRegressionSummaryRefreshKey &&
			selectedRegressionAlert &&
			String(runMonitorRegressionRunId ?? '').trim() &&
			String(runMonitorRegressionBaselineRunId ?? '').trim()
		) {
			runMonitorRegressionSummaryRefreshKey = autoKey;
			void refreshRegressionRunSummaries(
				String(runMonitorRegressionRunId ?? '').trim(),
				String(runMonitorRegressionBaselineRunId ?? '').trim()
			);
		}
	}
	$: runMonitorRegressionAutoKey = `${String($graphStore.graphId ?? '').trim()}|${runMonitorRegressionPair.runId}|${runMonitorRegressionPair.baselineRunId}|${runMonitorRegressionTypeFilter}|${runMonitorRegressionSeverityFilter}|${runMonitorRegressionSort}`;
	$: if (
		runMonitorRegressionAutoKey !== runMonitorRegressionRefreshKey &&
		runMonitorRegressionPair.runId &&
		runMonitorRegressionPair.baselineRunId
	) {
		runMonitorRegressionRefreshKey = runMonitorRegressionAutoKey;
		void refreshRunMonitorRegressions(
			runMonitorRegressionPair.runId,
			runMonitorRegressionPair.baselineRunId
		);
	}
	$: runMonitorTransitionRunId = String(runMonitorRegressionPair.runId ?? '').trim();
	$: runMonitorTransitionsAutoKey = `${String($graphStore.graphId ?? '').trim()}|${runMonitorTransitionRunId}|${runMonitorTransitionFilter}`;
	$: if (
		runMonitorTransitionsAutoKey !== runMonitorTransitionsRefreshKey &&
		runMonitorTransitionRunId
	) {
		runMonitorTransitionsRefreshKey = runMonitorTransitionsAutoKey;
		void refreshRunMonitorTransitions(runMonitorTransitionRunId);
	}
	$: if (!runMonitorTransitionRunId) {
		runMonitorTransitions = [];
		runMonitorTransitionsError = null;
	}
	$: runMonitorTransitionsVisible = filterRunMonitorTransitionRows(
		runMonitorTransitions,
		runMonitorTransitionFilter
	);
	$: runMonitorTrendNodeOptions = runMonitorNodeRows
		.map((row) => ({ id: String(row.nodeId ?? '').trim(), label: String(row.label ?? '').trim() }))
		.filter((row) => row.id.length > 0)
		.sort((a, b) => a.label.localeCompare(b.label));
	$: if (!runMonitorTrendNodeId && runMonitorTrendNodeOptions.length > 0) {
		runMonitorTrendNodeId = runMonitorTrendNodeOptions[0].id;
	}
	$: runMonitorAnalyticsAutoKey = `${String($graphStore.graphId ?? '').trim()}|${runMonitorTrendNodeId}|${runMonitorTrendMetric}|${runMonitorRunTrendSort}|${runMonitorNodeTrendSort}|${runMonitorSlaThresholdMs}|${runMonitorBottleneckSort}|${runMonitorAnalyticsStartAt}|${runMonitorAnalyticsEndAt}|${runMonitorAnalyticsOffset}|${runMonitorAdaptiveHistorySort}`;
	$: if (
		runMonitorAnalyticsAutoKey !== runMonitorAnalyticsRefreshKey &&
		String($graphStore.graphId ?? '').trim().length > 0
	) {
		runMonitorAnalyticsRefreshKey = runMonitorAnalyticsAutoKey;
		void refreshRunMonitorAnalytics();
	}
	$: runMonitorTrendSparkline = buildTrendSparkline(
		runMonitorTrendPoints.map((point) => ({
			createdAt: String(point.createdAt ?? ''),
			value: Number(point.value ?? NaN)
		})),
		{ width: 520, height: 88 }
	);
	$: if (
		runMonitorRunTrendPoints.length === 0 &&
		(runMonitorSelectedRunTrendId || runMonitorSelectedRunSummary || runMonitorSelectedRunSummaryError)
	) {
		runMonitorSelectedRunTrendId = '';
		runMonitorSelectedRunSummary = null;
		runMonitorSelectedRunSummaryError = null;
	}
	$: if (
		runMonitorRunTrendPoints.length > 0 &&
		!runMonitorRunTrendPoints.some(
			(point) => String(point.runId ?? '').trim() === runMonitorSelectedRunTrendId
		)
	) {
		runMonitorSelectedRunTrendId = String(runMonitorRunTrendPoints[0]?.runId ?? '').trim();
		if (runMonitorSelectedRunTrendId) {
			void refreshRunTrendSummary(runMonitorSelectedRunTrendId);
		}
	}
	$: runMonitorTrendHoverPoint =
		runMonitorTrendSparkline &&
		runMonitorTrendHoverIndex >= 0 &&
		runMonitorTrendHoverIndex < runMonitorTrendSparkline.points.length
			? runMonitorTrendSparkline.points[runMonitorTrendHoverIndex]
			: null;
	$: runMonitorTrendHoverCreatedAt = String(runMonitorTrendHoverPoint?.createdAt ?? '').trim();
	$: if (!runMonitorRegressionPair.runId || !runMonitorRegressionPair.baselineRunId) {
		runMonitorRegressionAlerts = [];
		runMonitorRegressionError = null;
		runMonitorRegressionRunId = runMonitorRegressionPair.runId;
		runMonitorRegressionBaselineRunId = runMonitorRegressionPair.baselineRunId;
		runMonitorRegressionSelectedIndex = -1;
	}
	$: canUndo = Boolean($graphStore) && graphStore.canUndo();
	$: canRedo = Boolean($graphStore) && graphStore.canRedo();
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
	$: hasUnsavedGraphChanges = isGraphSemanticDirty(
		lastSavedGraphSemanticSnapshotKey,
		currentGraphSemanticSnapshotKey
	);
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
	$: runToolbarControls = pauseResumeToolbarVisibility($graphStore.runStatus as any);
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
		{ id: 'cmd_add_model', label: 'Add Model', run: () => void addNode('model') },
		{ id: 'cmd_add_tool', label: 'Add Tool', run: () => void addNode('tool') },
		{ id: 'cmd_add_component', label: 'Add Component', run: () => void addComponentNodeWithPicker() },
		{ id: 'cmd_add_starter_template', label: 'Add Starter Template', run: () => void openStarterTemplatePicker() },
		{
			id: 'cmd_apply_operation_preset',
			label: 'Apply Operation Preset',
			disabled: !$selectedNode || guidedPresetsForSelectedKind.length === 0,
			run: () => void openOperationPresetPickerForSelectedNode()
		},
		{
			id: 'cmd_apply_inline_example',
			label: 'Apply Inline Example',
			disabled: !$selectedNode || !guidedInlinePreset,
			run: () => {
				if (!$selectedNode || !guidedInlinePreset) return;
				applyGuidedOperationPresetToNode($selectedNode.id, guidedInlinePreset);
			}
		},
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
							modelKind: typeof data.modelKind === 'string' ? data.modelKind : undefined,
							taskKind: typeof data.taskKind === 'string' ? data.taskKind : undefined,
							componentKind: typeof data.componentKind === 'string' ? data.componentKind : undefined,
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

	function graphCenterPoint(items: Node<PipelineNodeData>[]): { x: number; y: number } | null {
		if (!Array.isArray(items) || items.length === 0) return null;
		let minX = Number.POSITIVE_INFINITY;
		let minY = Number.POSITIVE_INFINITY;
		let maxX = Number.NEGATIVE_INFINITY;
		let maxY = Number.NEGATIVE_INFINITY;
		for (const node of items) {
			const x = Number(node?.position?.x ?? 0);
			const y = Number(node?.position?.y ?? 0);
			const w = Number((node as any)?.width ?? 240);
			const h = Number((node as any)?.height ?? 96);
			minX = Math.min(minX, x);
			minY = Math.min(minY, y);
			maxX = Math.max(maxX, x + (Number.isFinite(w) ? w : 240));
			maxY = Math.max(maxY, y + (Number.isFinite(h) ? h : 96));
		}
		if (!Number.isFinite(minX) || !Number.isFinite(minY) || !Number.isFinite(maxX) || !Number.isFinite(maxY)) {
			return null;
		}
		return { x: (minX + maxX) / 2, y: (minY + maxY) / 2 };
	}

	async function centerGraphAfterLoad(duration = 220): Promise<void> {
		await tick();
		const state = get(graphStore);
		const center = graphCenterPoint(state.nodes);
		if (!center) return;
		const vp = getViewport();
		setCenter(center.x, center.y, { zoom: Number(vp.zoom ?? 1), duration });
	}

	function upstreamLabel(fromNodeId: string, sourceHandle: string): string {
		const node = $graphStore.nodes.find((n) => n.id === fromNodeId);
		const base = String(node?.data?.label ?? fromNodeId);
		return `${base}.${sourceHandle}`;
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
						contract: meta?.schema?.contract ?? meta?.payloadSchema?.contract
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

	function releaseProgrammaticLogScrollGuard(): void {
		if (programmaticLogScrollUnlockHandle) {
			clearTimeout(programmaticLogScrollUnlockHandle);
			programmaticLogScrollUnlockHandle = null;
		}
		programmaticLogScroll = false;
	}

	async function scrollToBottom(behavior: ScrollBehavior = 'auto') {
		// Wait for Svelte to finish updating the DOM
		await tick();
		if (scrollElement) {
			programmaticLogScroll = true;
			if (programmaticLogScrollUnlockHandle) clearTimeout(programmaticLogScrollUnlockHandle);
			scrollElement.scrollTo({
				top: scrollElement.scrollHeight,
				behavior
			});
			programmaticLogScrollUnlockHandle = setTimeout(releaseProgrammaticLogScrollGuard, 160);
		}
	}

	function isRunLogNearBottom(el: HTMLElement, thresholdPx = 24): boolean {
		const distanceFromBottom = el.scrollHeight - (el.scrollTop + el.clientHeight);
		return distanceFromBottom <= thresholdPx;
	}

	function handleRunLogScroll(): void {
		if (!scrollElement) return;
		if (programmaticLogScroll) return;
		logAutoScrollEnabled = isRunLogNearBottom(scrollElement);
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
		releaseProgrammaticLogScrollGuard();
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
	const NODE_LONG_PRESS_MS = 500;
	const NODE_LONG_PRESS_MOVE_PX = 8;
	const NODE_DUPLICATE_OFFSET_X = 40;
	const NODE_DUPLICATE_OFFSET_Y = 30;
	let historyDragTransactionOpen = false;
	let longPressTimer: ReturnType<typeof setTimeout> | null = null;
	let longPressNodeId: string | null = null;
	let longPressPointerId: number | null = null;
	let longPressStartX = 0;
	let longPressStartY = 0;
	let suppressClickNodeId: string | null = null;
	let suppressClickUntil = 0;

	function clearLongPressState(): void {
		if (longPressTimer) clearTimeout(longPressTimer);
		longPressTimer = null;
		longPressNodeId = null;
		longPressPointerId = null;
	}

	function duplicateNodeExact(nodeId: string): string | null {
		const state = get(graphStore);
		const original = state.nodes.find((n) => n.id === nodeId);
		if (!original) return null;
		if (state.runStatus === 'running') return null;
		const kind = original.data.kind as NodeKind;
		const cloneId = graphStore.addNode(kind, {
			x: Number(original.position?.x ?? 0) + NODE_DUPLICATE_OFFSET_X,
			y: Number(original.position?.y ?? 0) + NODE_DUPLICATE_OFFSET_Y
		});
		if (kind === 'source') {
			graphStore.setSourceKind(cloneId, ((original.data as any)?.sourceKind ?? 'file') as SourceKind);
		}
		if (kind === 'llm' || kind === 'model') {
			graphStore.setLlmKind(cloneId, ((original.data as any)?.llmKind ?? 'ollama') as LlmKind);
		}
		if (kind === 'transform') {
			graphStore.setTransformKind(cloneId, ((original.data as any)?.transformKind ?? 'select') as TransformKind);
		}
		if (kind === 'tool') {
			const provider =
				(((original.data as any)?.params ?? {}) as Record<string, unknown>)?.provider ??
				(original.data as any)?.params?.provider ??
				'mcp';
			graphStore.setToolProvider(cloneId, String(provider) as ToolProvider);
		}
		const params = structuredClone((original.data.params ?? {}) as Record<string, unknown>);
		graphStore.updateNodeConfig(cloneId, { params });
		const expectedTypedSchema = ((original.data as any)?.schema?.expectedSchema?.typedSchema ?? null) as
			| Record<string, unknown>
			| null;
		if (expectedTypedSchema && typeof expectedTypedSchema === 'object') {
			graphStore.setNodeExpectedSchema(cloneId, structuredClone(expectedTypedSchema));
		}
		const label = String(original.data.label ?? '').trim();
		if (label) graphStore.updateNodeTitle(cloneId, label);
		const meta = (original.data as any)?.meta;
		if (meta && typeof meta === 'object') {
			graphStore.setNodeMeta(cloneId, structuredClone(meta as Record<string, unknown>));
		}
		graphStore.selectNode(cloneId);
		return cloneId;
	}

	function onFlowPointerDown(event: PointerEvent): void {
		if (event.button !== 0) return;
		const target = event.target as HTMLElement | null;
		const nodeEl = target?.closest?.('.svelte-flow__node') as HTMLElement | null;
		const nodeId = String(nodeEl?.dataset?.id ?? '').trim();
		if (!nodeId) return;
		if (!historyDragTransactionOpen) {
			graphStore.beginHistoryTransaction();
			historyDragTransactionOpen = true;
		}
		clearLongPressState();
		longPressNodeId = nodeId;
		longPressPointerId = event.pointerId;
		longPressStartX = event.clientX;
		longPressStartY = event.clientY;
		longPressTimer = setTimeout(() => {
			const id = longPressNodeId;
			clearLongPressState();
			if (!id) return;
			const duplicated = duplicateNodeExact(id);
			if (duplicated) {
				suppressClickNodeId = id;
				suppressClickUntil = performance.now() + 260;
				showToast('Node duplicated', 'info');
			}
		}, NODE_LONG_PRESS_MS);
	}

	function onFlowPointerMove(event: PointerEvent): void {
		if (!longPressNodeId || longPressPointerId !== event.pointerId) return;
		const dx = event.clientX - longPressStartX;
		const dy = event.clientY - longPressStartY;
		if (Math.hypot(dx, dy) > NODE_LONG_PRESS_MOVE_PX) {
			clearLongPressState();
		}
	}

	function onFlowPointerUp(event: PointerEvent): void {
		if (longPressPointerId === event.pointerId) {
			clearLongPressState();
		}
		if (historyDragTransactionOpen) {
			graphStore.endHistoryTransaction();
			historyDragTransactionOpen = false;
		}
	}

	function onnodeclick({ node }: { node: Node<PipelineNodeData> }) {
		const id = node.id;
		if (suppressClickNodeId === id && performance.now() < suppressClickUntil) return;
		if (suppressClickNodeId === id && performance.now() >= suppressClickUntil) {
			suppressClickNodeId = null;
			suppressClickUntil = 0;
		}
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
					appliedParams: structuredClone((node.data.params ?? {}) as Record<string, unknown>)
				}
			});
		if (result.mode === 'updated') {
			showToast(`Preset "${preset.name}" overwritten.`, 'info');
		}
	}

	function showToast(
		message: string,
		level: 'info' | 'warn' | 'error' = 'info',
		action?: { label: string; onClick: () => void }
	): void {
		toastMessage = message;
		toastLevel = level;
		toastActionLabel = action?.label ?? null;
		toastAction = action?.onClick ?? null;
		if (toastTimer) clearTimeout(toastTimer);
		toastTimer = setTimeout(() => {
			toastMessage = null;
			toastActionLabel = null;
			toastAction = null;
			toastTimer = null;
		}, action ? 6000 : 2600);
	}

	function runToastAction(): void {
		const action = toastAction;
		toastMessage = null;
		toastActionLabel = null;
		toastAction = null;
		if (toastTimer) {
			clearTimeout(toastTimer);
			toastTimer = null;
		}
		if (action) action();
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
					appliedParams: structuredClone((node.data.params ?? {}) as Record<string, unknown>)
				}
			});
	}

	function applyPresetToNode(nodeId: string, preset: NodePreset): void {
		if (preset.kind === 'source') {
			graphStore.setSourceKind(nodeId, preset.subtype as SourceKind);
		} else if (preset.kind === 'transform') {
			graphStore.setTransformKind(nodeId, preset.subtype as TransformKind);
		} else if (preset.kind === 'llm' || preset.kind === 'model') {
			graphStore.setLlmKind(nodeId, preset.subtype as LlmKind);
		} else {
			graphStore.setToolProvider(nodeId, preset.subtype as ToolProvider);
		}
			graphStore.updateNodeConfig(nodeId, {
				params: structuredClone(preset.params)
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

	function applyGuidedOperationPresetToNode(nodeId: string, preset: GuidedOperationPreset): void {
		const state = get(graphStore) as GraphState;
		const node = (state.nodes ?? []).find((n) => n.id === nodeId);
		if (!node) return;
		if (node.data.kind !== preset.kind) {
			showToast(`Preset "${preset.name}" requires a ${preset.kind} node.`, 'warn');
			return;
		}
		if (preset.kind === 'source' && preset.sourceKind) {
			graphStore.setSourceKind(nodeId, preset.sourceKind);
		} else if (preset.kind === 'transform' && preset.transformKind) {
			graphStore.setTransformKind(nodeId, preset.transformKind);
		} else if (preset.kind === 'tool' && preset.toolProvider) {
			graphStore.setToolProvider(nodeId, preset.toolProvider);
		}
			graphStore.updateNodeConfig(nodeId, {
				params: structuredClone(preset.params)
			});
		showToast(`Applied preset: ${preset.name}`, 'info');
	}

	function openOperationPresetPickerForSelectedNode(): void {
		const node = $selectedNode;
		if (!node) {
			showToast('Select a node first.', 'warn');
			return;
		}
		const options = getOperationPresetsForKind(node.data.kind);
		if (options.length === 0) {
			showToast(`No operation presets available for ${node.data.kind}.`, 'warn');
			return;
		}
		const lines = options.map((preset, index) => `${index + 1}. ${preset.name} - ${preset.description}`).join('\n');
		const raw = window.prompt(`Apply operation preset:\n${lines}\n\nEnter number (1-${options.length})`, '1');
		if (!raw) return;
		const pick = Number(raw);
		if (!Number.isInteger(pick) || pick < 1 || pick > options.length) {
			showToast('Invalid preset selection.', 'warn');
			return;
		}
		applyGuidedOperationPresetToNode(node.id, options[pick - 1]);
	}

	function applyStarterTemplate(templateId: string): void {
		const template = getStarterTemplateById(templateId);
		if (!template) {
			showToast('Starter template not found.', 'error');
			return;
		}
		const vp = getViewport();
		const centerScreen = { x: window.innerWidth * 0.3, y: window.innerHeight * 0.5 };
		const anchor = screenToFlowPosition(centerScreen);
		const nodeIdByTemplateId: Record<string, string> = {};
		for (const definition of template.nodes) {
			const nodeId = graphStore.addNode(definition.kind, {
				x: anchor.x + Number(definition.position?.x ?? 0),
				y: anchor.y + Number(definition.position?.y ?? 0)
			});
			nodeIdByTemplateId[definition.id] = nodeId;
			if (definition.kind === 'source' && definition.sourceKind) {
				graphStore.setSourceKind(nodeId, definition.sourceKind);
			} else if (definition.kind === 'transform' && definition.transformKind) {
				graphStore.setTransformKind(nodeId, definition.transformKind);
			} else if (definition.kind === 'tool' && definition.toolProvider) {
				graphStore.setToolProvider(nodeId, definition.toolProvider);
			}
			graphStore.updateNodeTitle(nodeId, definition.label);
				graphStore.updateNodeConfig(nodeId, {
					params: structuredClone(definition.params ?? {})
				});
		}
			for (const edge of template.edges) {
				const source = nodeIdByTemplateId[edge.source];
				const target = nodeIdByTemplateId[edge.target];
				if (!source || !target) continue;
				graphStore.addEdge({
				id: `e_${crypto.randomUUID()}`,
				source,
				target,
					markerEnd: { type: MarkerType.ArrowClosed },
					data: {
						exec: 'idle'
					}
				});
		}
		const focusId = nodeIdByTemplateId[template.nodes[template.nodes.length - 1]?.id ?? ''];
		if (focusId) graphStore.selectNode(focusId);
		setCenter(anchor.x + 320, anchor.y, { zoom: vp.zoom, duration: 260 });
		showToast(`Starter template added: ${template.name}`, 'info');
	}

	function openStarterTemplatePicker(): void {
		const lines = DSML_STARTER_TEMPLATES.map(
			(template, index) => `${index + 1}. ${template.name} - ${template.description}`
		).join('\n');
		const raw = window.prompt(
			`Add starter template:\n${lines}\n\nEnter number (1-${DSML_STARTER_TEMPLATES.length})`,
			'1'
		);
		if (!raw) return;
		const pick = Number(raw);
		if (!Number.isInteger(pick) || pick < 1 || pick > DSML_STARTER_TEMPLATES.length) {
			showToast('Invalid starter template selection.', 'warn');
			return;
		}
		applyStarterTemplate(DSML_STARTER_TEMPLATES[pick - 1].id);
	}

	function runGuidedRecommendation(recommendation: GuidedRecommendation): void {
		if (recommendation.action === 'add_node' && recommendation.nodeKind) {
			addNode(recommendation.nodeKind);
			return;
		}
		if (recommendation.action === 'open_template') {
			openStarterTemplatePicker();
			return;
		}
		if (recommendation.action === 'apply_preset') {
			openOperationPresetPickerForSelectedNode();
			return;
		}
		if (recommendation.action === 'run') {
			runFromStart();
		}
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

	function validateConnection(conn: Connection): {
		ok: boolean;
		error?: string;
		suggestion?: string | null;
		adapterKind?: string | null;
	} {
		if (!conn.source || !conn.target) return { ok: false, error: 'Missing source or target' };
		if (conn.source === conn.target) return { ok: false, error: 'Cannot connect node to itself' };
		if (
			componentOutputCount(conn.source) > 1 &&
			String(conn.sourceHandle ?? 'out').trim() === 'out'
		) {
			return {
				ok: false,
				error: 'Component output handle is required when component has multiple outputs'
			};
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
		if (reaches(conn.target, conn.source)) {
			return { ok: false, error: 'Connection would create a cycle' };
		}

		const preflight = graphStore.preflightConnection({
			source: conn.source,
			target: conn.target,
			sourceHandle: conn.sourceHandle ?? null,
			targetHandle: conn.targetHandle ?? null
		});
		if (!preflight.ok) {
			const details = (preflight as any).details ?? null;
			const detailText =
				details && typeof details === 'object'
					? ` [mode=${String(details.mode ?? 'work')} source=${String(details.sourceHandle ?? 'out')}(${String(details.sourceAffinity ?? 'work')}) target=${String(details.targetHandle ?? 'in')}(${String(details.targetAffinity ?? '?')})]`
					: '';
			return {
				ok: false,
				error: `${String((preflight as any).error ?? 'Connection preflight failed')}${detailText}`,
				suggestion: (preflight as any).suggestion ?? null,
				adapterKind: (preflight as any).adapterKind ?? null
			};
		}

		return { ok: true };
	}

	function isValidConnection(conn: Connection): boolean {
		return validateConnection(conn).ok;
	}

	function onconnect(conn: Connection) {
		const validation = validateConnection(conn);
		if (!validation.ok) {
			const msg = validation.suggestion
				? `${validation.error ?? 'Connection rejected'} ${validation.suggestion}`
				: validation.error ?? 'Connection rejected';
			if (validation.adapterKind) {
				showToast(msg, 'warn', {
					label: 'Insert adapter',
					onClick: () => {
						const inserted = graphStore.insertSchemaAdapterForEdgeConnection({
							source: conn.source!,
							target: conn.target!,
							sourceHandle: conn.sourceHandle ?? null,
							targetHandle: conn.targetHandle ?? null,
							adapterKind: validation.adapterKind as any
						});
						if (!inserted.ok) {
							showToast(String(inserted.error ?? 'Failed to insert adapter'), 'error');
							return;
						}
						showToast('Inserted schema adapter.', 'info');
					}
				});
			} else {
				showToast(msg, 'error');
			}
			return;
		}

		const e: Edge<PipelineEdgeData> = {
			id: `e_${crypto.randomUUID()}`,
			source: conn.source!,
			target: conn.target!,
			sourceHandle: conn.sourceHandle ?? undefined,
			targetHandle: conn.targetHandle ?? undefined,
			markerEnd: { type: MarkerType.ArrowClosed },
			data: {
				exec: 'idle',
				contract: {} // graphStore.addEdge computes schema contract
			}
		};

		// Delegate adding to the store (validates + persists); store update will sync back to canvas
		const r = graphStore.addEdge(e);
		if (!r.ok) {
			const msg = String(r.error ?? 'Failed to add edge');
			console.warn('Failed to add edge:', msg);
			if (r.adapterKind) {
				showToast(msg, 'warn', {
					label: 'Insert adapter',
					onClick: () => {
						const inserted = graphStore.insertSchemaAdapterForEdgeConnection({
							source: conn.source!,
							target: conn.target!,
							sourceHandle: conn.sourceHandle ?? null,
							targetHandle: conn.targetHandle ?? null,
							adapterKind: r.adapterKind ?? null
						});
						if (!inserted.ok) {
							showToast(String(inserted.error ?? 'Failed to insert adapter'), 'error');
							return;
						}
						showToast('Inserted schema adapter.', 'info');
					}
				});
				return;
			}
			showToast(msg, 'warn');
			return;
		}
		if (r.adapterKind && r.id) {
			showToast(
				'Connection added via coercion. Insert explicit adapter for deterministic schema flow?',
				'info',
				{
					label: 'Insert adapter',
					onClick: () => {
						graphStore.deleteEdge(r.id as string);
						const inserted = graphStore.insertSchemaAdapterForEdgeConnection({
							source: conn.source!,
							target: conn.target!,
							sourceHandle: conn.sourceHandle ?? null,
							targetHandle: conn.targetHandle ?? null,
							adapterKind: r.adapterKind ?? null
						});
						if (!inserted.ok) {
							showToast(String(inserted.error ?? 'Failed to insert adapter'), 'error');
							return;
						}
						showToast('Inserted schema adapter.', 'info');
					}
				}
			);
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

	function focusNodeFromMonitor(nodeId: string) {
		const resolvedNodeId = String(nodeId ?? '').trim();
		if (!resolvedNodeId) return;
		const n = nodes.find((candidate) => String(candidate.id ?? '').trim() === resolvedNodeId);
		if (!n) return;
		graphStore.selectNode(resolvedNodeId);
		inspectorMode = 'edit';
		const vp = getViewport();
		setCenter(Number(n.position?.x ?? 0) + 120, Number(n.position?.y ?? 0) + 40, {
			zoom: Number(vp.zoom ?? 1),
			duration: 220
		});
	}

	function focusEdgeFromMonitor(sourceNodeId: string, targetNodeId: string) {
		const fallback = preferredMonitorEdgeFocusNodeId(sourceNodeId, targetNodeId);
		if (!fallback) return;
		focusNodeFromMonitor(fallback);
	}

	function compactEdgeId(edgeId: string): string {
		const raw = String(edgeId ?? '').trim();
		if (!raw) return '';
		if (raw.length <= 8) return raw;
		return `${raw.slice(0, 8)}...`;
	}

	function adaptiveSeverityClass(severity: string): string {
		const normalized = String(severity ?? '').trim().toLowerCase();
		if (normalized === 'high') return 'adaptiveSeverity adaptiveSeverity-high';
		if (normalized === 'medium') return 'adaptiveSeverity adaptiveSeverity-medium';
		return 'adaptiveSeverity adaptiveSeverity-low';
	}

	function regressionSeverity(alert: RegressionAlert): 'low' | 'medium' | 'high' {
		const explicit = String(alert?.severity ?? '').trim().toLowerCase();
		if (explicit === 'high' || explicit === 'medium' || explicit === 'low') {
			return explicit;
		}
		const type = String(alert?.type ?? '').trim().toLowerCase();
		if (type.includes('latency')) {
			const pct = Math.abs(Number(alert?.driftPct ?? 0));
			if (pct >= 75) return 'high';
			if (pct >= 35) return 'medium';
			return 'low';
		}
		const delta = Math.abs(Number(alert?.delta ?? 0));
		if (delta >= 5) return 'high';
		if (delta >= 2) return 'medium';
		return 'low';
	}

	function selectTrendPointDrilldown(point: ExperimentNodeTrendPoint): void {
		const nodeId = String(point?.nodeId ?? '').trim();
		if (nodeId) {
			runMonitorTrendNodeId = nodeId;
			focusNodeFromMonitor(nodeId);
		}
		const metric = String(point?.metric ?? '').trim();
		if (
			metric === 'p95Ms' ||
			metric === 'p50Ms' ||
			metric === 'avgMs' ||
			metric === 'maxMs' ||
			metric === 'count'
		) {
			runMonitorTrendMetric = metric;
		}
	}

	function selectSlaBreachDrilldown(breach: ExperimentSlaBreach): void {
		const nodeId = String(breach?.nodeId ?? '').trim();
		if (nodeId) {
			runMonitorTrendNodeId = nodeId;
			runMonitorTrendMetric = 'p95Ms';
			focusNodeFromMonitor(nodeId);
		}
	}

	function selectRegressionAlertDrilldown(alert: RegressionAlert, index?: number): void {
		if (Number.isInteger(index) && Number(index) >= 0) {
			runMonitorRegressionSelectedIndex = Number(index);
		}
		const nodeId = String(alert?.nodeId ?? '').trim();
		if (!nodeId) return;
		runMonitorTrendNodeId = nodeId;
		const type = String(alert?.type ?? '').trim().toLowerCase();
		const reasonCode = String(alert?.reasonCode ?? '').trim().toLowerCase();
		if (type.includes('latency') || reasonCode.includes('latency')) {
			runMonitorTrendMetric = 'p95Ms';
		}
		focusNodeFromMonitor(nodeId);
	}

	function selectFailureTaxonomyDrilldown(item: ExperimentFailureTaxonomyItem): void {
		const errorCode = String(item?.errorCode ?? '').trim();
		if (!errorCode) return;
		runLogFilter = errorCode;
	}

	function selectBottleneckDrilldown(item: ExperimentBottleneckNode): void {
		const nodeId = String(item?.nodeId ?? '').trim();
		if (!nodeId) return;
		runMonitorTrendNodeId = nodeId;
		runMonitorTrendMetric = 'p95Ms';
		focusNodeFromMonitor(nodeId);
	}

	function selectRunTrendDrilldown(point: ExperimentRunTrendPoint): void {
		const runId = String(point?.runId ?? '').trim();
		if (!runId) return;
		runMonitorSelectedRunTrendId = runId;
		void refreshRunTrendSummary(runId);
		runLogFilter = runId;
	}

	async function refreshRunTrendSummary(runId: string): Promise<void> {
		const resolvedRunId = String(runId ?? '').trim();
		if (!resolvedRunId) {
			runMonitorSelectedRunSummary = null;
			runMonitorSelectedRunSummaryError = null;
			return;
		}
		runMonitorSelectedRunSummaryLoading = true;
		runMonitorSelectedRunSummaryError = null;
		try {
			const res = await getExperimentRunSummary(resolvedRunId);
			runMonitorSelectedRunSummary =
				res.experiment && typeof res.experiment === 'object'
					? (res.experiment as Record<string, unknown>)
					: null;
		} catch (error) {
			runMonitorSelectedRunSummary = null;
			runMonitorSelectedRunSummaryError = String(error ?? 'Failed to load run summary');
		} finally {
			runMonitorSelectedRunSummaryLoading = false;
		}
	}

	async function refreshRegressionRunSummaries(
		runId: string,
		baselineRunId: string
	): Promise<void> {
		const currentRunId = String(runId ?? '').trim();
		const baselineId = String(baselineRunId ?? '').trim();
		if (!currentRunId || !baselineId) {
			runMonitorRegressionCurrentSummary = null;
			runMonitorRegressionBaselineSummary = null;
			runMonitorRegressionSummaryError = null;
			return;
		}
		runMonitorRegressionSummaryLoading = true;
		runMonitorRegressionSummaryError = null;
		try {
			const [currentRes, baselineRes] = await Promise.all([
				getExperimentRunSummary(currentRunId),
				getExperimentRunSummary(baselineId)
			]);
			runMonitorRegressionCurrentSummary =
				currentRes.experiment && typeof currentRes.experiment === 'object'
					? (currentRes.experiment as Record<string, unknown>)
					: null;
			runMonitorRegressionBaselineSummary =
				baselineRes.experiment && typeof baselineRes.experiment === 'object'
					? (baselineRes.experiment as Record<string, unknown>)
					: null;
		} catch (error) {
			runMonitorRegressionCurrentSummary = null;
			runMonitorRegressionBaselineSummary = null;
			runMonitorRegressionSummaryError = String(
				error ?? 'Failed to load regression run summaries'
			);
		} finally {
			runMonitorRegressionSummaryLoading = false;
		}
	}

	function selectTransitionEventDrilldown(event: RunMonitorTransitionRow): void {
		const entity = String(event?.entity ?? '').trim().toLowerCase();
		const entityId = String(event?.entityId ?? '').trim();
		if (entity === 'node' && entityId) {
			focusNodeFromMonitor(entityId);
			return;
		}
		if (entity === 'run') {
			const runId = String(event?.runId ?? '').trim();
			if (runId) runLogFilter = runId;
		}
	}

	function selectAdaptiveDecisionDrilldown(row: RunMonitorAdaptiveDecisionRow): void {
		const key = `${row.at}:${row.runId}`;
		runMonitorAdaptiveDecisionSelectedKey = key;
		const runId = String(row.runId ?? '').trim();
		if (runId) runLogFilter = runId;
	}

	function selectRegressionHistoryPair(index: number): void {
		const pair = pickRunMonitorRegressionPairFromHistory(runMonitorHistoryRows as any, index);
		if (!pair.runId || !pair.baselineRunId) return;
		runMonitorRegressionRunOverride = pair.runId;
		runMonitorRegressionBaselineOverride = pair.baselineRunId;
	}

	function clearRegressionHistoryPairOverride(): void {
		runMonitorRegressionRunOverride = '';
		runMonitorRegressionBaselineOverride = '';
	}

	function onTrendSparklineMove(event: PointerEvent): void {
		if (!runMonitorTrendSparkline || runMonitorTrendSparkline.points.length === 0) {
			runMonitorTrendHoverIndex = -1;
			return;
		}
		const target = event.currentTarget as SVGSVGElement | null;
		if (!target) return;
		const rect = target.getBoundingClientRect();
		if (rect.width <= 0) return;
		const px = Math.max(0, Math.min(rect.width, Number(event.clientX || 0) - rect.left));
		const normalizedX = (px / rect.width) * runMonitorTrendSparkline.width;
		let bestIndex = 0;
		let bestDistance = Number.POSITIVE_INFINITY;
		for (let index = 0; index < runMonitorTrendSparkline.points.length; index += 1) {
			const distance = Math.abs(runMonitorTrendSparkline.points[index].x - normalizedX);
			if (distance < bestDistance) {
				bestDistance = distance;
				bestIndex = index;
			}
		}
		runMonitorTrendHoverIndex = bestIndex;
	}

	function onAdaptiveSparklineMove(event: PointerEvent): void {
		if (!runMonitorAdaptiveSparkline || runMonitorAdaptiveSparkline.points.length === 0) {
			runMonitorAdaptiveHoverIndex = -1;
			return;
		}
		const target = event.currentTarget as SVGSVGElement | null;
		if (!target) return;
		const rect = target.getBoundingClientRect();
		if (rect.width <= 0) return;
		const px = Math.max(0, Math.min(rect.width, Number(event.clientX || 0) - rect.left));
		const normalizedX = (px / rect.width) * runMonitorAdaptiveSparkline.width;
		let bestIndex = 0;
		let bestDistance = Number.POSITIVE_INFINITY;
		for (let index = 0; index < runMonitorAdaptiveSparkline.points.length; index += 1) {
			const distance = Math.abs(runMonitorAdaptiveSparkline.points[index].x - normalizedX);
			if (distance < bestDistance) {
				bestDistance = distance;
				bestIndex = index;
			}
		}
		runMonitorAdaptiveHoverIndex = bestIndex;
	}

	function focusAdaptiveSparklineHoverPoint(): void {
		const index = Number(runMonitorAdaptiveHoverIndex ?? -1);
		if (index < 0 || index >= runMonitorAdaptiveSparklineRows.length) return;
		const row = runMonitorAdaptiveSparklineRows[index];
		if (!row) return;
		selectAdaptiveDecisionDrilldown(row);
	}

	async function refreshRunMonitorRegressions(
		runId?: string | null,
		baselineRunId?: string | null
	): Promise<void> {
		if (typeof window === 'undefined') return;
		const resolvedRunId = String(runId ?? '').trim();
		const resolvedBaselineRunId = String(baselineRunId ?? '').trim();
		if (!resolvedRunId || !resolvedBaselineRunId) {
			runMonitorRegressionAlerts = [];
			runMonitorRegressionError = null;
			runMonitorRegressionRunId = resolvedRunId;
			runMonitorRegressionBaselineRunId = resolvedBaselineRunId;
			runMonitorRegressionSummaryRefreshKey = '';
			runMonitorRegressionCurrentSummary = null;
			runMonitorRegressionBaselineSummary = null;
			runMonitorRegressionSummaryError = null;
			return;
		}
		runMonitorRegressionLoading = true;
		runMonitorRegressionError = null;
		try {
			const res = await getExperimentRegressions({
				runId: resolvedRunId,
				baselineRunId: resolvedBaselineRunId,
				alertType: runMonitorRegressionTypeFilter,
				severity: runMonitorRegressionSeverityFilter,
				sort: runMonitorRegressionSort,
				limit: 50,
				offset: 0,
				latencyDriftPct: 25,
				failureDriftAbs: 1
			});
			runMonitorRegressionAlerts = Array.isArray(res.alerts) ? res.alerts : [];
			if (runMonitorRegressionAlerts.length === 0) {
				runMonitorRegressionSelectedIndex = -1;
				runMonitorRegressionSummaryRefreshKey = '';
				runMonitorRegressionCurrentSummary = null;
				runMonitorRegressionBaselineSummary = null;
				runMonitorRegressionSummaryError = null;
			} else if (
				runMonitorRegressionSelectedIndex < 0 ||
				runMonitorRegressionSelectedIndex >= runMonitorRegressionAlerts.length
			) {
				runMonitorRegressionSelectedIndex = 0;
			}
			runMonitorRegressionRunId = String(res.runId ?? resolvedRunId).trim();
			runMonitorRegressionBaselineRunId = String(
				res.baselineRunId ?? resolvedBaselineRunId
			).trim();
		} catch (error) {
			runMonitorRegressionAlerts = [];
			runMonitorRegressionSelectedIndex = -1;
			runMonitorRegressionRunId = resolvedRunId;
			runMonitorRegressionBaselineRunId = resolvedBaselineRunId;
			runMonitorRegressionSummaryRefreshKey = '';
			runMonitorRegressionCurrentSummary = null;
			runMonitorRegressionBaselineSummary = null;
			runMonitorRegressionSummaryError = null;
			runMonitorRegressionError = String(error ?? 'Failed to load regression alerts');
		} finally {
			runMonitorRegressionLoading = false;
		}
	}

	async function refreshRunMonitorTransitions(runId?: string | null): Promise<void> {
		if (typeof window === 'undefined') return;
		const resolvedRunId = String(runId ?? '').trim();
		if (!resolvedRunId) {
			runMonitorTransitions = [];
			runMonitorTransitionsError = null;
			return;
		}
		runMonitorTransitionsLoading = true;
		runMonitorTransitionsError = null;
		try {
			const transitionQuery: {
				entity?: 'run' | 'node' | null;
				includeViolations?: boolean;
				violationsOnly?: boolean;
			} = {};
			if (runMonitorTransitionFilter === 'run') transitionQuery.entity = 'run';
			else if (runMonitorTransitionFilter === 'node') transitionQuery.entity = 'node';
			else if (runMonitorTransitionFilter === 'violations') transitionQuery.violationsOnly = true;
			const res = await getRunTransitions({
				runId: resolvedRunId,
				afterId: 0,
				limit: 200,
				...transitionQuery
			});
			runMonitorTransitions = buildRunMonitorTransitionRows(
				Array.isArray(res.events) ? res.events.slice(-200) : []
			).slice(0, 50);
		} catch (error) {
			runMonitorTransitions = [];
			runMonitorTransitionsError = String(error ?? 'Failed to load transition history');
		} finally {
			runMonitorTransitionsLoading = false;
		}
	}

	async function refreshRunMonitorAnalytics(): Promise<void> {
		if (typeof window === 'undefined') return;
		const graphId = String($graphStore.graphId ?? '').trim();
		if (!graphId) {
			runMonitorRunTrendPoints = [];
			runMonitorSelectedRunTrendId = '';
			runMonitorSelectedRunSummary = null;
			runMonitorSelectedRunSummaryError = null;
			runMonitorTrendPoints = [];
			runMonitorSlaBreaches = [];
			runMonitorFailureTaxonomy = [];
			runMonitorBottleneckNodes = [];
			runMonitorAdaptiveHistoryRowsRaw = [];
			runMonitorAnalyticsError = null;
			return;
		}
		runMonitorAnalyticsLoading = true;
		runMonitorAnalyticsError = null;
		try {
			const [runTrendRes, trendRes, slaRes, failureRes, adaptiveRes, bottleneckRes] =
				await Promise.all([
				getExperimentRunTrends({
					graphId,
					startAt: runMonitorAnalyticsStartAt || undefined,
					endAt: runMonitorAnalyticsEndAt || undefined,
					sort: runMonitorRunTrendSort,
					limit: 20,
					offset: Math.max(0, Number(runMonitorAnalyticsOffset || 0))
				}),
				getExperimentNodeTrends({
					graphId,
					nodeId: runMonitorTrendNodeId || undefined,
					metric: runMonitorTrendMetric,
					startAt: runMonitorAnalyticsStartAt || undefined,
					endAt: runMonitorAnalyticsEndAt || undefined,
					sort: runMonitorNodeTrendSort,
					limit: 50,
					offset: 0
				}),
				getExperimentSlaBreaches({
					graphId,
					p95Ms: Number(runMonitorSlaThresholdMs || 2000),
					startAt: runMonitorAnalyticsStartAt || undefined,
					endAt: runMonitorAnalyticsEndAt || undefined,
					limit: 30,
					offset: 0
				}),
				getExperimentFailureTaxonomy({
					graphId,
					startAt: runMonitorAnalyticsStartAt || undefined,
					endAt: runMonitorAnalyticsEndAt || undefined,
					limit: 30,
					offset: 0
				}),
				getExperimentAdaptiveDecisions({
					graphId,
					startAt: runMonitorAnalyticsStartAt || undefined,
					endAt: runMonitorAnalyticsEndAt || undefined,
					sort: runMonitorAdaptiveHistorySort,
					limit: 100,
					offset: 0
				}),
				getExperimentBottlenecks({
					graphId,
					startAt: runMonitorAnalyticsStartAt || undefined,
					endAt: runMonitorAnalyticsEndAt || undefined,
					sort: runMonitorBottleneckSort,
					limit: 30,
					offset: 0
				})
			]);
			runMonitorRunTrendPoints = Array.isArray(runTrendRes.points)
				? runTrendRes.points.slice(-10)
				: [];
			runMonitorTrendPoints = Array.isArray(trendRes.points) ? trendRes.points.slice(-20) : [];
			runMonitorTrendHoverIndex = -1;
			runMonitorSlaBreaches = Array.isArray(slaRes.breaches) ? slaRes.breaches.slice(0, 20) : [];
			runMonitorFailureTaxonomy = Array.isArray(failureRes.taxonomy)
				? failureRes.taxonomy.slice(0, 20)
				: [];
			runMonitorAdaptiveHistoryRowsRaw = Array.isArray(adaptiveRes.decisions)
				? adaptiveRes.decisions.slice(0, 100)
				: [];
			runMonitorBottleneckNodes = Array.isArray(bottleneckRes.nodes)
				? bottleneckRes.nodes.slice(0, 20)
				: [];
		} catch (error) {
			runMonitorRunTrendPoints = [];
			runMonitorTrendPoints = [];
			runMonitorSlaBreaches = [];
			runMonitorFailureTaxonomy = [];
			runMonitorBottleneckNodes = [];
			runMonitorAdaptiveHistoryRowsRaw = [];
			runMonitorAnalyticsError = String(error ?? 'Failed to load historical analytics');
		} finally {
			runMonitorAnalyticsLoading = false;
		}
	}

	async function resetRunUi() {
		await graphStore.hardCancelActiveRuns();
		graphStore.resetRunUi();
	}

	function runFromStart() {
		const adaptiveMode =
			runMonitorAdaptiveModeOverride === 'default'
				? null
				: (runMonitorAdaptiveModeOverride as 'off' | 'observe' | 'enforce');
		void graphStore.runRemote(null, 'from_start', globalCacheMode, adaptiveMode);
	}

	function runFromSelected() {
		const adaptiveMode =
			runMonitorAdaptiveModeOverride === 'default'
				? null
				: (runMonitorAdaptiveModeOverride as 'off' | 'observe' | 'enforce');
		void graphStore.runRemote(
			$selectedNode?.id ?? null,
			'from_selected_onward',
			globalCacheMode,
			adaptiveMode
		);
	}

	function pauseRun() {
		void graphStore.pauseActiveRun();
	}

	function resumeRun() {
		void graphStore.resumeActiveRun();
	}

	function pinSelectedPerRun() {
		const result = graphStore.setSelectedNodeFreezeMode('per_run');
		if (!result?.ok && result?.error) window.alert(String(result.error));
	}

	function pinSelectedSticky() {
		const result = graphStore.setSelectedNodeFreezeMode('sticky');
		if (!result?.ok && result?.error) window.alert(String(result.error));
	}

	function clearSelectedPin() {
		graphStore.setSelectedNodeFreezeMode(null);
	}

	function cycleSelectedPinMode() {
		if (selectedFreezeMode === 'per_run') {
			pinSelectedSticky();
			return;
		}
		if (selectedFreezeMode === 'sticky') {
			clearSelectedPin();
			return;
		}
		pinSelectedPerRun();
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
			reset: () => void resetRunUi()
		});
	}

	function onAddMenuSelect(actionId: string) {
		dispatchAddMenuAction(actionId, {
			addStarterTemplate: openStarterTemplatePicker,
			addSource: () => addNode('source'),
			addTransform: () => addNode('transform'),
			addModel: () => addNode('model'),
			addLlm: () => addNode('model'),
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
		const target = event.target as HTMLElement | null;
		const isEditableTarget =
			!!target &&
			(target.closest('input, textarea, [contenteditable=\"true\"], [role=\"textbox\"]') != null);
		const isCtrlK = (event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k';
		if (isCtrlK) {
			event.preventDefault();
			toggleCommandPalette();
			return;
		}
		const isUndo = (event.ctrlKey || event.metaKey) && !event.shiftKey && event.key.toLowerCase() === 'z';
		const isRedo = (event.ctrlKey || event.metaKey) && event.shiftKey && event.key.toLowerCase() === 'z';
		if ((isUndo || isRedo) && !isEditableTarget) {
			event.preventDefault();
			if (isUndo) graphStore.undo();
			if (isRedo) graphStore.redo();
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
		if (kind === 'llm' || kind === 'model') {
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
		lastSavedGraphSemanticSnapshotKey = null;
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
			const inputPayloadType = (primaryRoot ? deriveNodeIoForData(primaryRoot.data).in : null) as PayloadType | null;
			const outputPayloadType = (primaryLeaf ? deriveNodeIoForData(primaryLeaf.data).out : null) as PayloadType | null;
		const inputs: ComponentApiContract['inputs'] =
			inputPayloadType == null
				? []
				: [
						{
							name: 'in_data',
							required: true,
							typedSchema: {
								type:
									inputPayloadType as ComponentApiContract['inputs'][number]['typedSchema']['type'],
								fields: []
							}
						}
					];
		const outputs: ComponentApiContract['outputs'] =
			outputPayloadType == null
				? []
				: [
						{
							name: 'out_data',
							required: true,
							typedSchema: {
								type:
									outputPayloadType as ComponentApiContract['outputs'][number]['typedSchema']['type'],
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
				componentId,
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
				componentId,
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
			} else if (String((result as any)?.reason ?? '') === 'consistency_mismatch') {
				saveConsistencyModalContext = 'Save Graph';
				saveConsistencyModalError = String((result as any)?.error ?? 'Consistency mismatch detected.');
				saveConsistencyModalData = ((result as any)?.consistency ?? null) as SaveConsistencyMismatch | null;
				saveConsistencyModalOpen = true;
			}
			showToast(`Save Graph failed: ${(result as any)?.error ?? (result as any)?.reason ?? 'unknown'}`, 'error');
			return;
		}
		lastSavedGraphSnapshotKey = currentGraphSnapshotKey;
		lastSavedGraphSemanticSnapshotKey = currentGraphSemanticSnapshotKey;
		const resolvedName = String((result as any)?.graphName ?? graphName ?? '').trim();
		if (resolvedName) currentGraphName = resolvedName;
		const saveWarnings = (((result as any)?.diagnostics ?? []) as Array<any>).filter(
			(item) => String(item?.severity ?? '').toLowerCase() === 'warning'
		);
		if (saveWarnings.length > 0) {
			showToast(
				`Saved with ${saveWarnings.length} warning${saveWarnings.length === 1 ? '' : 's'} (see preflight diagnostics).`,
				'warn'
			);
		}
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
			} else if (String((result as any)?.reason ?? '') === 'consistency_mismatch') {
				saveConsistencyModalContext = 'Save Version';
				saveConsistencyModalError = String((result as any)?.error ?? 'Consistency mismatch detected.');
				saveConsistencyModalData = ((result as any)?.consistency ?? null) as SaveConsistencyMismatch | null;
				saveConsistencyModalOpen = true;
			}
			showToast(`Save Version failed: ${(result as any)?.error ?? (result as any)?.reason ?? 'unknown'}`, 'error');
			return;
		}
		lastSavedGraphSnapshotKey = currentGraphSnapshotKey;
		lastSavedGraphSemanticSnapshotKey = currentGraphSemanticSnapshotKey;
		const saveWarnings = (((result as any)?.diagnostics ?? []) as Array<any>).filter(
			(item) => String(item?.severity ?? '').toLowerCase() === 'warning'
		);
		if (saveWarnings.length > 0) {
			showToast(
				`Saved version with ${saveWarnings.length} warning${saveWarnings.length === 1 ? '' : 's'} (see preflight diagnostics).`,
				'warn'
			);
		}
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
			} else if (String((result as any)?.reason ?? '') === 'consistency_mismatch') {
				saveConsistencyModalContext = 'Save Graph As';
				saveConsistencyModalError = String((result as any)?.error ?? 'Consistency mismatch detected.');
				saveConsistencyModalData = ((result as any)?.consistency ?? null) as SaveConsistencyMismatch | null;
				saveConsistencyModalOpen = true;
			}
			showToast(`Save Graph As failed: ${(result as any)?.error ?? (result as any)?.reason ?? 'unknown'}`, 'error');
			return;
		}
		lastSavedGraphSnapshotKey = currentGraphSnapshotKey;
		lastSavedGraphSemanticSnapshotKey = currentGraphSemanticSnapshotKey;
		currentGraphName = graphName;
		const saveWarnings = (((result as any)?.diagnostics ?? []) as Array<any>).filter(
			(item) => String(item?.severity ?? '').toLowerCase() === 'warning'
		);
		if (saveWarnings.length > 0) {
			showToast(
				`Saved graph with ${saveWarnings.length} warning${saveWarnings.length === 1 ? '' : 's'} (see preflight diagnostics).`,
				'warn'
			);
		}
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
		await centerGraphAfterLoad();
		lastSavedGraphSnapshotKey = currentGraphSnapshotKey;
		lastSavedGraphSemanticSnapshotKey = currentGraphSemanticSnapshotKey;
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
		await centerGraphAfterLoad();
		lastSavedGraphSnapshotKey = currentGraphSnapshotKey;
		lastSavedGraphSemanticSnapshotKey = currentGraphSemanticSnapshotKey;
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
			await centerGraphAfterLoad();
			lastSavedGraphSnapshotKey = null;
			lastSavedGraphSemanticSnapshotKey = null;
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

	async function beginInspectorSplit(pair: InspectorSplitPair, event: PointerEvent) {
		if (pair === 'env_logs' && runLogsCollapsed) {
			runLogsCollapsed = false;
			await tick();
		}
		const topTargetsLogs = pair === 'top_env' && !environmentPaneEl;
		splitEnvLogsBypassEnvironment = pair === 'env_logs' && environmentCollapsed;
		const paneA =
			pair === 'top_env'
				? inspectorTopPaneEl
				: splitEnvLogsBypassEnvironment
					? inspectorTopPaneEl
					: environmentPaneEl;
		const paneB = pair === 'top_env' ? (topTargetsLogs ? runLogsPaneEl : environmentPaneEl) : runLogsPaneEl;
		if (!paneA || !paneB) return;
		const aRect = paneA.getBoundingClientRect();
		const bRect = paneB.getBoundingClientRect();
		const pairStartPx = aRect.height + bRect.height;
		if (pairStartPx <= 0) return;
		const totalWeight =
			pair === 'top_env'
				? topTargetsLogs
					? Math.max(0.001, inspectorTopWeight + runLogsWeight)
					: Math.max(0.001, inspectorTopWeight + environmentWeight)
				: Math.max(0.001, environmentWeight + runLogsWeight);
		const paneAWeight =
			pair === 'top_env'
				? inspectorTopWeight
				: environmentWeight;
		const paneAFromWeight = (pairStartPx * paneAWeight) / totalWeight;
		const paneBFromWeight = pairStartPx - paneAFromWeight;
		const paneAStart = aRect.height > 0 ? aRect.height : paneAFromWeight;
		const paneBStart = bRect.height > 0 ? bRect.height : paneBFromWeight;
		const normalizedA = Math.max(0, Math.min(pairStartPx, paneAStart));
		const normalizedB = Math.max(0, pairStartPx - normalizedA);
		activeInspectorSplit = pair;
		activeInspectorPaneAEl = paneA;
		activeInspectorPaneBEl = paneB;
		splitStartY = event.clientY;
		splitPaneAStartPx = normalizedB > 0 ? normalizedA : paneAFromWeight;
		splitPaneBStartPx = pairStartPx - splitPaneAStartPx;
		activeInspectorPaneANoScrollPx = estimatePaneNoScrollPx(paneA, splitPaneAStartPx);
		activeInspectorPaneBNoScrollPx = estimatePaneNoScrollPx(paneB, splitPaneBStartPx);
	}

	function estimatePaneNoScrollPx(paneEl: HTMLElement | null, paneStartPx: number): number {
		if (!paneEl || !(paneStartPx > 0)) return Number.POSITIVE_INFINITY;
		let maxDeficit = Math.max(0, Number(paneEl.scrollHeight || 0) - Number(paneEl.clientHeight || 0));
		const descendants = paneEl.querySelectorAll<HTMLElement>('*');
		for (const child of descendants) {
			const deficit = Math.max(0, Number(child.scrollHeight || 0) - Number(child.clientHeight || 0));
			if (deficit > maxDeficit) {
				maxDeficit = deficit;
			}
		}
		return paneStartPx + maxDeficit;
	}

	function onInspectorSplitMove(event: PointerEvent) {
		if (!activeInspectorSplit) return;
		const pairStartPx = splitPaneAStartPx + splitPaneBStartPx;
		if (pairStartPx <= 0) return;
		const minPanePx =
			activeInspectorSplit === 'env_logs' && splitEnvLogsBypassEnvironment ? 64 : 96;
		const dy = event.clientY - splitStartY;
		let nextPaneA = Math.max(minPanePx, Math.min(pairStartPx - minPanePx, splitPaneAStartPx + dy));
		const growingA = nextPaneA > splitPaneAStartPx;
		const growingB = nextPaneA < splitPaneAStartPx;
		const paneANoScrollLimit = Math.max(
			minPanePx,
			Math.min(
				pairStartPx - minPanePx,
				Math.ceil(Number(activeInspectorPaneANoScrollPx))
			)
		);
		const paneBNoScrollLimit = Math.max(
			minPanePx,
			Math.min(
				pairStartPx - minPanePx,
				Math.ceil(Number(activeInspectorPaneBNoScrollPx))
			)
		);
		if (growingA) {
			const cap = Math.max(splitPaneAStartPx, paneANoScrollLimit);
			nextPaneA = Math.min(nextPaneA, cap);
		} else if (growingB) {
			const floor = Math.min(splitPaneAStartPx, pairStartPx - paneBNoScrollLimit);
			nextPaneA = Math.max(nextPaneA, floor);
		}
		nextPaneA = Math.max(minPanePx, Math.min(pairStartPx - minPanePx, nextPaneA));

		if (activeInspectorSplit === 'top_env') {
			if (!environmentPaneEl) {
				const total = inspectorTopWeight + runLogsWeight;
				if (total <= 0) return;
				inspectorTopWeight = (total * nextPaneA) / pairStartPx;
				runLogsWeight = total - inspectorTopWeight;
			} else {
				const total = inspectorTopWeight + environmentWeight;
				if (total <= 0) return;
				inspectorTopWeight = (total * nextPaneA) / pairStartPx;
				environmentWeight = total - inspectorTopWeight;
			}
		} else {
			if (splitEnvLogsBypassEnvironment) {
				const total = inspectorTopWeight + runLogsWeight;
				if (total <= 0) return;
				inspectorTopWeight = (total * nextPaneA) / pairStartPx;
				runLogsWeight = total - inspectorTopWeight;
			} else {
				const total = environmentWeight + runLogsWeight;
				if (total <= 0) return;
				environmentWeight = (total * nextPaneA) / pairStartPx;
				runLogsWeight = total - environmentWeight;
			}
		}
	}

	function onInspectorSplitUp() {
		activeInspectorSplit = null;
		activeInspectorPaneAEl = null;
		activeInspectorPaneBEl = null;
		activeInspectorPaneANoScrollPx = Number.POSITIVE_INFINITY;
		activeInspectorPaneBNoScrollPx = Number.POSITIVE_INFINITY;
		splitEnvLogsBypassEnvironment = false;
	}

	function rebalanceInspectorForCollapse(target: 'top' | 'logs'): void {
		const total = Math.max(0.001, inspectorTopWeight + runLogsWeight);
		const collapsedWeight = Math.max(0.001, total * 0.05);
		if (target === 'top') {
			inspectorTopWeight = collapsedWeight;
			runLogsWeight = Math.max(0.001, total - collapsedWeight);
			return;
		}
		runLogsWeight = collapsedWeight;
		inspectorTopWeight = Math.max(0.001, total - collapsedWeight);
	}

	function snapshotInspectorCollapseRestore(): void {
		inspectorCollapseRestore = {
			top: Math.max(0.001, inspectorTopWeight),
			logs: Math.max(0.001, runLogsWeight)
		};
	}

	function restoreInspectorCollapseWeights(): void {
		if (!inspectorCollapseRestore) return;
		const total = Math.max(0.001, inspectorTopWeight + runLogsWeight);
		const savedTotal = Math.max(0.001, inspectorCollapseRestore.top + inspectorCollapseRestore.logs);
		inspectorTopWeight = (total * inspectorCollapseRestore.top) / savedTotal;
		runLogsWeight = Math.max(0.001, total - inspectorTopWeight);
		inspectorCollapseRestore = null;
	}

	function toggleNodeInspectorCollapsed(): void {
		const next = !nodeInspectorCollapsed;
		nodeInspectorCollapsed = next;
		if (!next) {
			restoreInspectorCollapseWeights();
			return;
		}
		snapshotInspectorCollapseRestore();
		if (runLogsCollapsed) runLogsCollapsed = false;
		rebalanceInspectorForCollapse('top');
	}

	function toggleRunLogsCollapsed(): void {
		const next = !runLogsCollapsed;
		runLogsCollapsed = next;
		if (!next) {
			restoreInspectorCollapseWeights();
			return;
		}
		snapshotInspectorCollapseRestore();
		if (nodeInspectorCollapsed) nodeInspectorCollapsed = false;
		rebalanceInspectorForCollapse('logs');
	}

	function runMonitorOpenStorageKey(graphId: string): string {
		return `flow.runMonitor.open.${graphId || 'default'}`;
	}

	function runMonitorWidthStorageKey(graphId: string): string {
		return `flow.runMonitor.width.${graphId || 'default'}`;
	}

	function runMonitorSectionSplitStorageKey(graphId: string): string {
		return `flow.runMonitor.split.section.${graphId || 'default'}`;
	}

	function runMonitorTableSplitStorageKey(graphId: string): string {
		return `flow.runMonitor.split.table.${graphId || 'default'}`;
	}

	function runMonitorAdaptiveOverrideStorageKey(graphId: string): string {
		return `flow.runMonitor.adaptiveOverride.${graphId || 'default'}`;
	}

	function inspectorSidebarWidthStorageKey(graphId: string): string {
		return `flow.inspector.width.${graphId || 'default'}`;
	}

	function loadRunMonitorSlideoutPrefs(graphId: string): void {
		if (typeof window === 'undefined') return;
		const gid = String(graphId || '').trim() || 'default';
		try {
			const openRaw = sessionStorage.getItem(runMonitorOpenStorageKey(gid));
			if (openRaw === '0' || openRaw === '1') {
				runMonitorSlideoutOpen = openRaw === '1';
			}
			const widthRaw = Number(sessionStorage.getItem(runMonitorWidthStorageKey(gid)));
			if (Number.isFinite(widthRaw)) {
				runMonitorSlideoutWidth = Math.min(720, Math.max(300, widthRaw));
			}
			const sectionRatioRaw = Number(sessionStorage.getItem(runMonitorSectionSplitStorageKey(gid)));
			if (Number.isFinite(sectionRatioRaw) && sectionRatioRaw > 0.1 && sectionRatioRaw < 0.9) {
				runMonitorMonitorWeight = sectionRatioRaw;
				runMonitorEnvWeight = Math.max(0.001, 1 - sectionRatioRaw);
			}
			const tableRatioRaw = Number(sessionStorage.getItem(runMonitorTableSplitStorageKey(gid)));
			if (Number.isFinite(tableRatioRaw) && tableRatioRaw > 0.1 && tableRatioRaw < 0.9) {
				runMonitorNodesWeight = tableRatioRaw;
				runMonitorEdgesWeight = Math.max(0.001, 1 - tableRatioRaw);
			}
			const adaptiveModeRaw = String(
				localStorage.getItem(runMonitorAdaptiveOverrideStorageKey(gid)) ??
					sessionStorage.getItem(runMonitorAdaptiveOverrideStorageKey(gid)) ??
					''
			).trim();
			if (
				adaptiveModeRaw === 'default' ||
				adaptiveModeRaw === 'off' ||
				adaptiveModeRaw === 'observe' ||
				adaptiveModeRaw === 'enforce'
			) {
				runMonitorAdaptiveModeOverride = adaptiveModeRaw;
			} else {
				runMonitorAdaptiveModeOverride = 'default';
			}
		} catch {
			// noop
		}
	}

	function loadInspectorSidebarPrefs(graphId: string): void {
		if (typeof window === 'undefined') return;
		const gid = String(graphId || '').trim() || 'default';
		try {
			const widthRaw = Number(sessionStorage.getItem(inspectorSidebarWidthStorageKey(gid)));
			if (Number.isFinite(widthRaw)) {
				inspectorSidebarWidth = Math.min(780, Math.max(340, widthRaw));
			}
		} catch {
			// noop
		}
	}

	function persistRunMonitorSlideoutPrefs(): void {
		if (typeof window === 'undefined') return;
		const gid = String(runMonitorPrefsGraphId || '').trim() || 'default';
		try {
			sessionStorage.setItem(runMonitorOpenStorageKey(gid), runMonitorSlideoutOpen ? '1' : '0');
			sessionStorage.setItem(runMonitorWidthStorageKey(gid), String(Math.round(runMonitorSlideoutWidth)));
			const totalSection = Math.max(0.001, runMonitorMonitorWeight + runMonitorEnvWeight);
			const sectionRatio = Math.min(0.95, Math.max(0.05, runMonitorMonitorWeight / totalSection));
			sessionStorage.setItem(runMonitorSectionSplitStorageKey(gid), sectionRatio.toFixed(4));
			const totalTable = Math.max(0.001, runMonitorNodesWeight + runMonitorEdgesWeight);
			const tableRatio = Math.min(0.95, Math.max(0.05, runMonitorNodesWeight / totalTable));
			sessionStorage.setItem(runMonitorTableSplitStorageKey(gid), tableRatio.toFixed(4));
			localStorage.setItem(
				runMonitorAdaptiveOverrideStorageKey(gid),
				String(runMonitorAdaptiveModeOverride || 'default')
			);
			sessionStorage.removeItem(runMonitorAdaptiveOverrideStorageKey(gid));
		} catch {
			// noop
		}
	}

	function persistInspectorSidebarPrefs(): void {
		if (typeof window === 'undefined') return;
		const gid = String(runMonitorPrefsGraphId || '').trim() || 'default';
		try {
			sessionStorage.setItem(inspectorSidebarWidthStorageKey(gid), String(Math.round(inspectorSidebarWidth)));
		} catch {
			// noop
		}
	}

	function toggleRunMonitorSlideout(): void {
		runMonitorSlideoutOpen = !runMonitorSlideoutOpen;
		persistRunMonitorSlideoutPrefs();
	}

	function beginRunMonitorResize(event: PointerEvent): void {
		runMonitorResizeActive = true;
		runMonitorResizeStartX = Number(event.clientX || 0);
		runMonitorResizeStartWidth = runMonitorSlideoutWidth;
		window.addEventListener('pointermove', onRunMonitorResizeMove);
		window.addEventListener('pointerup', endRunMonitorResize);
	}

	function beginInspectorResize(event: PointerEvent): void {
		inspectorResizeActive = true;
		inspectorResizeStartX = Number(event.clientX || 0);
		inspectorResizeStartWidth = inspectorSidebarWidth;
		window.addEventListener('pointermove', onInspectorResizeMove);
		window.addEventListener('pointerup', endInspectorResize);
	}

	function onInspectorResizeMove(event: PointerEvent): void {
		if (!inspectorResizeActive) return;
		const dx = Number(event.clientX || 0) - inspectorResizeStartX;
		const next = inspectorResizeStartWidth - dx;
		inspectorSidebarWidth = Math.min(780, Math.max(340, next));
	}

	function endInspectorResize(): void {
		if (!inspectorResizeActive) return;
		inspectorResizeActive = false;
		window.removeEventListener('pointermove', onInspectorResizeMove);
		window.removeEventListener('pointerup', endInspectorResize);
		persistInspectorSidebarPrefs();
	}

	function onRunMonitorResizeMove(event: PointerEvent): void {
		if (!runMonitorResizeActive) return;
		const dx = Number(event.clientX || 0) - runMonitorResizeStartX;
		const next = runMonitorResizeStartWidth - dx;
		runMonitorSlideoutWidth = Math.min(720, Math.max(300, next));
	}

	function endRunMonitorResize(): void {
		if (!runMonitorResizeActive) return;
		runMonitorResizeActive = false;
		window.removeEventListener('pointermove', onRunMonitorResizeMove);
		window.removeEventListener('pointerup', endRunMonitorResize);
		persistRunMonitorSlideoutPrefs();
	}

	function beginRunMonitorSplit(pair: RunMonitorSplitPair, event: PointerEvent): void {
		const paneA = pair === 'monitor_env' ? runMonitorPaneEl : runMonitorNodesPaneEl;
		const paneB = pair === 'monitor_env' ? runMonitorEnvPaneEl : runMonitorEdgesPaneEl;
		if (!paneA || !paneB) return;
		const aRect = paneA.getBoundingClientRect();
		const bRect = paneB.getBoundingClientRect();
		const pairStartPx = aRect.height + bRect.height;
		if (pairStartPx <= 0) return;
		const totalWeight =
			pair === 'monitor_env'
				? Math.max(0.001, runMonitorMonitorWeight + runMonitorEnvWeight)
				: Math.max(0.001, runMonitorNodesWeight + runMonitorEdgesWeight);
		const paneAWeight = pair === 'monitor_env' ? runMonitorMonitorWeight : runMonitorNodesWeight;
		const paneAFromWeight = (pairStartPx * paneAWeight) / totalWeight;
		const paneAStart = aRect.height > 0 ? aRect.height : paneAFromWeight;
		const normalizedA = Math.max(0, Math.min(pairStartPx, paneAStart));
		activeRunMonitorSplit = pair;
		activeRunMonitorPaneAEl = paneA;
		activeRunMonitorPaneBEl = paneB;
		runMonitorSplitStartY = event.clientY;
		runMonitorSplitPaneAStartPx = normalizedA;
		runMonitorSplitPaneBStartPx = pairStartPx - normalizedA;
		activeRunMonitorPaneANoScrollPx = estimatePaneNoScrollPx(paneA, runMonitorSplitPaneAStartPx);
		activeRunMonitorPaneBNoScrollPx = estimatePaneNoScrollPx(paneB, runMonitorSplitPaneBStartPx);
	}

	function onRunMonitorSplitMove(event: PointerEvent): void {
		if (!activeRunMonitorSplit) return;
		const pairStartPx = runMonitorSplitPaneAStartPx + runMonitorSplitPaneBStartPx;
		if (pairStartPx <= 0) return;
		const minPanePx = 96;
		const dy = event.clientY - runMonitorSplitStartY;
		let nextPaneA = Math.max(minPanePx, Math.min(pairStartPx - minPanePx, runMonitorSplitPaneAStartPx + dy));
		const growingA = nextPaneA > runMonitorSplitPaneAStartPx;
		const growingB = nextPaneA < runMonitorSplitPaneAStartPx;
		const paneANoScrollLimit = Math.max(
			minPanePx,
			Math.min(
				pairStartPx - minPanePx,
				Math.ceil(Number(activeRunMonitorPaneANoScrollPx))
			)
		);
		const paneBNoScrollLimit = Math.max(
			minPanePx,
			Math.min(
				pairStartPx - minPanePx,
				Math.ceil(Number(activeRunMonitorPaneBNoScrollPx))
			)
		);
		if (growingA) {
			const cap = Math.max(runMonitorSplitPaneAStartPx, paneANoScrollLimit);
			nextPaneA = Math.min(nextPaneA, cap);
		} else if (growingB) {
			const floor = Math.min(runMonitorSplitPaneAStartPx, pairStartPx - paneBNoScrollLimit);
			nextPaneA = Math.max(nextPaneA, floor);
		}
		nextPaneA = Math.max(minPanePx, Math.min(pairStartPx - minPanePx, nextPaneA));
		if (activeRunMonitorSplit === 'monitor_env') {
			const total = Math.max(0.001, runMonitorMonitorWeight + runMonitorEnvWeight);
			runMonitorMonitorWeight = (total * nextPaneA) / pairStartPx;
			runMonitorEnvWeight = Math.max(0.001, total - runMonitorMonitorWeight);
			return;
		}
		const total = Math.max(0.001, runMonitorNodesWeight + runMonitorEdgesWeight);
		runMonitorNodesWeight = (total * nextPaneA) / pairStartPx;
		runMonitorEdgesWeight = Math.max(0.001, total - runMonitorNodesWeight);
	}

	function onRunMonitorSplitUp(): void {
		if (!activeRunMonitorSplit) return;
		activeRunMonitorSplit = null;
		activeRunMonitorPaneAEl = null;
		activeRunMonitorPaneBEl = null;
		activeRunMonitorPaneANoScrollPx = Number.POSITIVE_INFINITY;
		activeRunMonitorPaneBNoScrollPx = Number.POSITIVE_INFINITY;
		persistRunMonitorSlideoutPrefs();
	}

	function onGlobalPointerMove(event: PointerEvent): void {
		onInspectorSplitMove(event);
		onRunMonitorSplitMove(event);
	}

	function onGlobalPointerUp(): void {
		onInspectorSplitUp();
		onRunMonitorSplitUp();
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

	async function refreshRuntimeEnvPanel(): Promise<void> {
		runtimeEnvLoading = true;
		runtimeEnvError = null;
		try {
			const payload = await listRuntimeEnvVars({ revealSensitive: runtimeEnvRevealSensitive });
			runtimeEnvVars = Array.isArray(payload.vars) ? payload.vars : [];
			const nextDrafts: Record<string, string> = {};
			for (const row of runtimeEnvVars) {
				const name = String(row?.name ?? '').trim();
				if (!name) continue;
				nextDrafts[name] = row.value ?? '';
			}
			runtimeEnvDraftByName = nextDrafts;
		} catch (error) {
			runtimeEnvError = String((error as Error)?.message ?? error ?? 'Failed to load runtime env vars.');
			runtimeEnvVars = [];
		} finally {
			runtimeEnvLoading = false;
		}
	}

	async function applyRuntimeEnvVar(name: string): Promise<void> {
		const key = String(name ?? '').trim();
		if (!key) return;
		runtimeEnvError = null;
		runtimeEnvSaving = { ...runtimeEnvSaving, [key]: true };
		try {
			const value = String(runtimeEnvDraftByName[key] ?? '');
			const payload = await updateRuntimeEnvVars([{ name: key, value, unset: false }]);
			runtimeEnvVars = Array.isArray(payload.vars) ? payload.vars : runtimeEnvVars;
			showToast(
				payload.restartRequired
					? `Updated ${key}. Restart required for some settings.`
					: `Updated ${key}.`,
				payload.restartRequired ? 'warn' : 'info'
			);
		} catch (error) {
			const message = String((error as Error)?.message ?? error ?? `Failed to update ${key}`);
			runtimeEnvError = message;
			showToast(message, 'error');
		} finally {
			const next = { ...runtimeEnvSaving };
			delete next[key];
			runtimeEnvSaving = next;
		}
	}

	async function unsetRuntimeEnvVar(name: string): Promise<void> {
		const key = String(name ?? '').trim();
		if (!key) return;
		runtimeEnvError = null;
		runtimeEnvSaving = { ...runtimeEnvSaving, [key]: true };
		try {
			const payload = await updateRuntimeEnvVars([{ name: key, unset: true }]);
			runtimeEnvVars = Array.isArray(payload.vars) ? payload.vars : runtimeEnvVars;
			const row = runtimeEnvVars.find((item) => item.name === key);
			runtimeEnvDraftByName = { ...runtimeEnvDraftByName, [key]: row?.value ?? '' };
			showToast(
				payload.restartRequired
					? `Cleared override for ${key}. Restart required for some settings.`
					: `Cleared override for ${key}.`,
				payload.restartRequired ? 'warn' : 'info'
			);
		} catch (error) {
			const message = String((error as Error)?.message ?? error ?? `Failed to clear ${key}`);
			runtimeEnvError = message;
			showToast(message, 'error');
		} finally {
			const next = { ...runtimeEnvSaving };
			delete next[key];
			runtimeEnvSaving = next;
		}
	}

	onDestroy(() => {
		if (subtypeErrorTimer) clearTimeout(subtypeErrorTimer);
		if (toastTimer) clearTimeout(toastTimer);
		endPortTypeLegendDrag();
		endRunMonitorResize();
		endInspectorResize();
		clearLongPressState();
	});

	onMount(async () => {
		await refreshSchemaCapabilitiesFromBackend();
		try {
			portTypeLegendMinimized = localStorage.getItem(PORT_TYPE_LEGEND_MINIMIZED_KEY) === '1';
			const savedX = Number(localStorage.getItem(PORT_TYPE_LEGEND_POS_X_KEY));
			const savedY = Number(localStorage.getItem(PORT_TYPE_LEGEND_POS_Y_KEY));
			if (Number.isFinite(savedX) && Number.isFinite(savedY)) {
				portTypeLegendPos = { x: Math.max(8, savedX), y: Math.max(8, savedY) };
			}
		} catch {
			portTypeLegendMinimized = false;
			portTypeLegendPos = { ...PORT_TYPE_LEGEND_DEFAULT_POS };
		}
		try {
			const config = await getGlobalCacheConfig();
			globalCacheMode = (config.mode ??
				(Boolean(config.enabled) ? 'default_on' : 'force_off')) as GlobalCacheMode;
		} catch (error) {
			console.warn('Failed to load global cache config', error);
		}
		void refreshRuntimeEnvPanel();
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
					await centerGraphAfterLoad(0);
					console.log(
						`[graph-v2-read] hydrated graphId=${(hydrated as any).graphId} revisionId=${(hydrated as any).revisionId}`
					);
				}
			} catch (error) {
				console.warn('Failed to hydrate graph from backend revision store', error);
			}
		}
		await tick();
		clampPortTypeLegendPosition();
		lastSavedGraphSnapshotKey = currentGraphSnapshotKey;
		lastSavedGraphSemanticSnapshotKey = currentGraphSemanticSnapshotKey;
	});

	function togglePortTypeLegendMinimized() {
		portTypeLegendMinimized = !portTypeLegendMinimized;
		try {
			localStorage.setItem(PORT_TYPE_LEGEND_MINIMIZED_KEY, portTypeLegendMinimized ? '1' : '0');
		} catch {
			/* noop */
		}
	}

	function beginPortTypeLegendDrag(event: PointerEvent): void {
		if (!flowPaneEl) return;
		clampPortTypeLegendPosition();
		isDraggingPortTypeLegend = true;
		const rect = flowPaneEl.getBoundingClientRect();
		portTypeLegendDragOffset = {
			x: event.clientX - (rect.left + portTypeLegendPos.x),
			y: event.clientY - (rect.top + portTypeLegendPos.y)
		};
		window.addEventListener('pointermove', onPortTypeLegendDragMove);
		window.addEventListener('pointerup', endPortTypeLegendDrag);
	}

	function onPortTypeLegendDragMove(event: PointerEvent): void {
		if (!isDraggingPortTypeLegend || !flowPaneEl) return;
		const rect = flowPaneEl.getBoundingClientRect();
		const nextX = event.clientX - rect.left - portTypeLegendDragOffset.x;
		const nextY = event.clientY - rect.top - portTypeLegendDragOffset.y;
		const minY = Math.max(8, Number(topbarEl?.offsetHeight ?? 0) + 8);
		const legendWidth = Number(portTypeLegendEl?.offsetWidth ?? 178);
		const legendHeight = Number(portTypeLegendEl?.offsetHeight ?? 38);
		const clampedX = Math.min(Math.max(8, nextX), Math.max(8, rect.width - legendWidth - 8));
		const clampedY = Math.min(Math.max(minY, nextY), Math.max(minY, rect.height - legendHeight - 8));
		portTypeLegendPos = { x: clampedX, y: clampedY };
	}

	function endPortTypeLegendDrag(): void {
		if (!isDraggingPortTypeLegend) return;
		isDraggingPortTypeLegend = false;
		window.removeEventListener('pointermove', onPortTypeLegendDragMove);
		window.removeEventListener('pointerup', endPortTypeLegendDrag);
		try {
			localStorage.setItem(PORT_TYPE_LEGEND_POS_X_KEY, String(Math.round(portTypeLegendPos.x)));
			localStorage.setItem(PORT_TYPE_LEGEND_POS_Y_KEY, String(Math.round(portTypeLegendPos.y)));
		} catch {
			/* noop */
		}
	}

	function clampPortTypeLegendPosition(): void {
		if (!flowPaneEl) return;
		const rect = flowPaneEl.getBoundingClientRect();
		const minY = Math.max(8, Number(topbarEl?.offsetHeight ?? 0) + 8);
		const legendWidth = Number(portTypeLegendEl?.offsetWidth ?? 178);
		const legendHeight = Number(portTypeLegendEl?.offsetHeight ?? 38);
		const clampedX = Math.min(Math.max(8, portTypeLegendPos.x), Math.max(8, rect.width - legendWidth - 8));
		const clampedY = Math.min(Math.max(minY, portTypeLegendPos.y), Math.max(minY, rect.height - legendHeight - 8));
		if (clampedX !== portTypeLegendPos.x || clampedY !== portTypeLegendPos.y) {
			portTypeLegendPos = { x: clampedX, y: clampedY };
		}
	}
</script>

<svelte:window
	on:pointermove={onGlobalPointerMove}
	on:pointerup={onGlobalPointerUp}
	on:keydown={onWindowKeyDown}
	on:resize={clampPortTypeLegendPosition}
/>

<div
	class="layout"
	style={`grid-template-columns: 1fr ${inspectorSidebarWidth}px ${runMonitorSlideoutOpen ? `${runMonitorSlideoutWidth}px` : '0px'};`}
>
	<div
		class="flow"
		role="region"
		aria-label="Flow canvas"
		bind:this={flowPaneEl}
		on:pointerdown={onFlowPointerDown}
		on:pointermove={onFlowPointerMove}
		on:pointerup={onFlowPointerUp}
		on:pointercancel={onFlowPointerUp}
	>
		{#if toastMessage}
			<div class={`toast toast-${toastLevel}`} role="status" aria-live="polite">
				<span>{toastMessage}</span>
				{#if toastActionLabel}
					<button type="button" class="toastAction" on:click={runToastAction}>{toastActionLabel}</button>
				{/if}
			</div>
		{/if}
		<div class="topbar" role="toolbar" aria-label="Graph toolbar" bind:this={topbarEl}>
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
				{#if runToolbarControls.showPause}
					<button
						class="runSecondary"
						on:click={pauseRun}
						disabled={runToolbarControls.disablePause}
						title={runToolbarControls.disablePause ? 'Pausing...' : 'Pause current run'}
					>
						Pause
					</button>
				{:else if runToolbarControls.showResume}
					<button
						class="runSecondary"
						on:click={resumeRun}
						disabled={runToolbarControls.disableResume}
						title={runToolbarControls.disableResume ? 'Resuming...' : 'Resume paused run'}
					>
						Resume
					</button>
				{/if}
				<button class="runSecondary" on:click={() => graphStore.undo()} disabled={!canUndo} title="Undo (Ctrl+Z)">
					Undo
				</button>
				<button
					class="runSecondary"
					on:click={() => graphStore.redo()}
					disabled={!canRedo}
					title="Redo (Ctrl+Shift+Z)"
				>
					Redo
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
				<ToolbarMenu
					label="+ Add"
					items={addMenuItems}
					onSelect={onAddMenuSelect}
					align="right"
					menuAriaLabel="Add node actions"
				/>
				<button
					type="button"
					class="runSecondary"
					title={runMonitorSlideoutOpen ? 'Hide Run Monitor slideout' : 'Show Run Monitor slideout'}
					on:click={toggleRunMonitorSlideout}
				>
					Monitor
				</button>
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
		{#if saveConsistencyModalOpen}
			<div class="commandPaletteBackdrop" role="dialog" aria-modal="true" aria-label="Save consistency mismatch">
				<div class="componentSaveApplyModal saveConsistencyModal">
					<div class="componentSaveApplyHead">
						<b>{saveConsistencyModalContext} blocked</b>
					</div>
					<div class="componentSaveApplyBody">
						<div class="saveConsistencyError">{saveConsistencyModalError}</div>
						{#if saveConsistencyModalData}
							<div class="saveConsistencyCounts">
								<div>Nodes: canvas {saveConsistencyModalData.canvasNodeCount} / persisted {saveConsistencyModalData.persistedNodeCount}</div>
								<div>Edges: canvas {saveConsistencyModalData.canvasEdgeCount} / persisted {saveConsistencyModalData.persistedEdgeCount}</div>
							</div>
							{#if saveConsistencyModalData.missingNodes.length > 0}
								<div class="saveConsistencyList">
									<div class="saveConsistencyListTitle">Missing nodes</div>
									<ul class="saveConsistencyListBody">
										{#each saveConsistencyModalData.missingNodes as entry (entry.id)}
											<li>
												<span>{entry.label}</span>
												<span class="saveConsistencyEntryId">{entry.id}</span>
											</li>
										{/each}
									</ul>
								</div>
							{/if}
							{#if saveConsistencyModalData.addedNodes.length > 0}
								<div class="saveConsistencyList">
									<div class="saveConsistencyListTitle">Added nodes</div>
									<ul class="saveConsistencyListBody">
										{#each saveConsistencyModalData.addedNodes as entry (entry.id)}
											<li>
												<span>{entry.label}</span>
												<span class="saveConsistencyEntryId">{entry.id}</span>
											</li>
										{/each}
									</ul>
								</div>
							{/if}
							{#if saveConsistencyModalData.changedNodes.length > 0}
								<div class="saveConsistencyList">
									<div class="saveConsistencyListTitle">Changed nodes</div>
									<ul class="saveConsistencyListBody">
										{#each saveConsistencyModalData.changedNodes as entry (entry.id)}
											<li>
												<span>{entry.label}</span>
												<span class="saveConsistencyEntryId">{entry.id}</span>
											</li>
										{/each}
									</ul>
								</div>
							{/if}
							{#if saveConsistencyModalData.missingEdges.length > 0}
								<div class="saveConsistencyList">
									<div class="saveConsistencyListTitle">Missing edges</div>
									<ul class="saveConsistencyListBody">
										{#each saveConsistencyModalData.missingEdges as entry (entry.id)}
											<li>
												<span>{entry.label}</span>
												<span class="saveConsistencyEntryId">{entry.id}</span>
											</li>
										{/each}
									</ul>
								</div>
							{/if}
							{#if saveConsistencyModalData.addedEdges.length > 0}
								<div class="saveConsistencyList">
									<div class="saveConsistencyListTitle">Added edges</div>
									<ul class="saveConsistencyListBody">
										{#each saveConsistencyModalData.addedEdges as entry (entry.id)}
											<li>
												<span>{entry.label}</span>
												<span class="saveConsistencyEntryId">{entry.id}</span>
											</li>
										{/each}
									</ul>
								</div>
							{/if}
							{#if saveConsistencyModalData.changedEdges.length > 0}
								<div class="saveConsistencyList">
									<div class="saveConsistencyListTitle">Changed edges</div>
									<ul class="saveConsistencyListBody">
										{#each saveConsistencyModalData.changedEdges as entry (entry.id)}
											<li>
												<span>{entry.label}</span>
												<span class="saveConsistencyEntryId">{entry.id}</span>
											</li>
										{/each}
									</ul>
								</div>
							{/if}
						{/if}
					</div>
					<div class="componentSaveApplyActions">
						<button
							type="button"
							class="primary"
							on:click={() => {
								saveConsistencyModalOpen = false;
								saveConsistencyModalData = null;
								saveConsistencyModalContext = '';
								saveConsistencyModalError = '';
							}}
						>
							Close
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

		<div
			bind:this={portTypeLegendEl}
			class={`portTypeLegend ${portTypeLegendMinimized ? 'is-minimized' : ''}`}
			role="note"
			aria-label="Port type key"
			style={`left:${portTypeLegendPos.x}px; top:${portTypeLegendPos.y}px;`}
		>
			<div
				class="portTypeLegendHead"
				role="button"
				tabindex="0"
				aria-label="Drag port type key"
				on:pointerdown={beginPortTypeLegendDrag}
			>
				<b>Port Type Key</b>
				<button
					type="button"
					class="portTypeLegendClose"
					on:pointerdown|stopPropagation
					on:click={togglePortTypeLegendMinimized}
					aria-label={portTypeLegendMinimized ? 'Expand port type key' : 'Minimize port type key'}
				>
					{portTypeLegendMinimized ? '+' : '-'}
				</button>
			</div>
			{#if !portTypeLegendMinimized}
				<div class="portTypeLegendRow">
					<span class="portTypeDot portTypeDot-work" aria-hidden="true"></span>
					<span>Data / work</span>
				</div>
				<div class="portTypeLegendRow">
					<span class="portTypeDot portTypeDot-param" aria-hidden="true"></span>
					<span>Parameter / param</span>
				</div>
				<div class="portTypeLegendRow">
					<span class="portTypeDot portTypeDot-control" aria-hidden="true"></span>
					<span>Control</span>
				</div>
			{/if}
		</div>
	</div>

	<aside class="inspector" bind:this={inspectorPane}>
		<button
			type="button"
			class="inspectorResizeHandle"
			aria-label="Resize node inspector sidebar"
			on:pointerdown={beginInspectorResize}
		></button>
		<div
			class="inspectorPane inspectorTop"
			bind:this={inspectorTopPaneEl}
			style={nodeInspectorCollapsed ? 'flex: 0 0 auto;' : `flex: ${inspectorTopWeight} 1 0;`}
		>
			<!-- <h3>Inspector</h3> -->
			{#if !guidedDsmlDismissed}
				<div class="card guidedCard" role="region" aria-label="Guided workflow recommendations">
					<div class="guidedHead">
						<b>Guided DS/ML</b>
						<div class="guidedHeadActions">
							<button
								type="button"
								class="tabBtn"
								on:click={openStarterTemplatePicker}
								aria-label="Add starter template"
							>
								Starter Templates
							</button>
							<button
								type="button"
								class="tabBtn guidedCloseBtn"
								aria-label="Close guided DS/ML panel"
								on:click={() => (guidedDsmlDismissed = true)}
							>
								Close
							</button>
						</div>
					</div>
					<div class="guidedBody">
						<div class="guidedHintTitle">{guidedNextStep.label}</div>
						<div class="guidedHintDescription">{guidedNextStep.description}</div>
						<div class="guidedActions">
							<button
								type="button"
								class="primary"
								on:click={() => runGuidedRecommendation(guidedNextStep)}
								aria-label="Apply recommended next step"
							>
								Do Next Step
							</button>
							{#if $selectedNode && guidedPresetsForSelectedKind.length > 0}
								<button
									type="button"
									class="runSecondary"
									on:click={openOperationPresetPickerForSelectedNode}
									aria-label="Open operation presets for selected node"
								>
									Operation Presets
								</button>
							{/if}
						</div>
						{#if $selectedNode && guidedInlinePreset}
							<div class="guidedInlineExample">
								<span class="mono">Inline example:</span> {guidedInlinePreset.name}
								<button
									type="button"
									class="tabBtn"
									on:click={() => applyGuidedOperationPresetToNode($selectedNode.id, guidedInlinePreset)}
									aria-label="Apply inline example"
								>
									Use Example
								</button>
							</div>
						{/if}
					</div>
				</div>
			{/if}

			{#if $selectedNode}
				<div class="card editorCard">
						<div class="head">
							<div style="min-width:0;display:flex;align-items:center;gap:8px;">
								<button
									type="button"
									class="tabBtn sectionToggle"
									title={nodeInspectorCollapsed ? 'Expand Node Inspector' : 'Collapse Node Inspector'}
									on:click={toggleNodeInspectorCollapsed}
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

							<span class={selectedKindPillClass}>{selectedKindPillText}</span>
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
								<button
									type="button"
									class={selectedPinPillClass}
									title="Cycle pin mode: unpinned → run-only → sticky → unpinned"
									on:click={cycleSelectedPinMode}
								>
									{selectedPinPillText}
								</button>
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
								{#if inspectorMode === 'edit' && $selectedNode}
									<select
										class="nodeTypeSwitch"
										aria-label="Node subtype"
										value={
											$selectedNode.data.kind === 'source'
												? selectedSourceKind
												: ($selectedNode.data.kind === 'llm' || $selectedNode.data.kind === 'model')
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
											<option value="object_store">object_store</option>
											<option value="warehouse">warehouse</option>
										{:else if $selectedNode.data.kind === 'llm' || $selectedNode.data.kind === 'model'}
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
											<option value="null_policy">null_policy</option>
											<option value="outlier_policy">outlier_policy</option>
											<option value="text_clean">text_clean</option>
											<option value="nlp_normalize">nlp_normalize</option>
											<option value="tokenize_chunk">tokenize_chunk</option>
											<option value="dataset_split">dataset_split</option>
											<option value="class_imbalance">class_imbalance</option>
											<option value="categorical_encode">categorical_encode</option>
											<option value="numeric_scale">numeric_scale</option>
											<option value="embedding">embedding</option>
											<option value="feature_selection">feature_selection</option>
											<option value="leakage_detect">leakage_detect</option>
											<option value="quality_profile">quality_profile</option>
											<option value="drift_compare">drift_compare</option>
											<option value="determinism_profile">determinism_profile</option>
											<option value="fit_state_registry">fit_state_registry</option>
											<option value="pii_guard">pii_guard</option>
											<option value="inference_parity">inference_parity</option>
											<option value="split">split</option>
											<option value="quality_gate">quality_gate</option>
											<option value="ml_contract">ml_contract</option>
											<option value="sql">sql</option>
											<option value="json_filter">json_filter</option>
											<option value="json_to_table">json_to_table</option>
											<option value="text_to_table">text_to_table</option>
											<option value="table_to_json">table_to_json</option>
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
										<div class="inputMissing">No declared inputs.</div>
									{:else}
									{#each inputResolutions as input (input.inputHandle)}
										<div class="inputCard">
											<div class="inputHead">
												<span class="inputPort">{input.inputHandle}</span>
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
													{upstreamLabel(input.edge.fromNodeId, input.edge.sourceHandle)}
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
												payloadType={inputMetaByArtifactId[inputPreviewArtifactId]?.contract}
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
										payloadType={nodeOut.payloadType}
										cached={nodeOut.cached}
										cacheDecision={nodeOut.cacheDecision}
										preview={nodeOut.preview}
										onJumpToNode={jumpToNodeFromArtifact}
									/>
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
		<button
			 type="button"
			 class="inspectorSplitter"
			 aria-label="Resize Node Inspector and Run Logs panels"
			 on:pointerdown={(event) => beginInspectorSplit('top_env', event)}
		></button>
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
					on:click={toggleRunLogsCollapsed}
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
				{#if warningSummaryRows.length > 0}
					<div class="runWarningSummary" aria-live="polite">
						<div class="runWarningSummaryHead">
							Warnings (deduped): {warningSummaryRows.length} keys / {warningSummaryTotalCount} events
						</div>
						{#each warningSummaryRows.slice(0, 5) as row (`${String((row as any)?.warningKey ?? '')}`)}
							<div class="runWarningSummaryRow">
								<span class="mono">{String((row as any)?.code ?? '')}</span>
								<span>node={String((row as any)?.nodeId ?? '')}</span>
								<span>handle={String((row as any)?.handle ?? '')}</span>
								<span>count={Number((row as any)?.count ?? 0)}</span>
							</div>
						{/each}
					</div>
				{/if}
				<div class="logs" bind:this={scrollElement} on:scroll={handleRunLogScroll}>
					{#each filteredLogs as l (l.id)}
						<div class={`log ${l.level}`}>
							<span class="ts">{l.ts}</span>
							<span class="msg">
								{#if l.componentPath?.length}
									<span class="nid">[Component: {l.componentPath.join(' > ')}]</span>
								{/if}
								{#if l.nodeId}
									<span class="nid">[{l.nodeId}{runLogNodeName(l) ? ` | ${runLogNodeName(l)}` : ''}]</span>
								{/if}
								{#if runLogEdgeTag(l)}
									<span class="nid">[edge: {runLogEdgeTag(l)}]</span>
								{/if}
								{l.message}
							</span>
						</div>
					{/each}
				</div>
			{/if}
		</div>
		</aside>
		{#if runMonitorSlideoutOpen}
			<aside class="runMonitorSlideout" style={`width:${runMonitorSlideoutWidth}px;`}>
				<button
					type="button"
					class="runMonitorResizeHandle"
					aria-label="Resize Run Monitor panel"
					on:pointerdown={beginRunMonitorResize}
				></button>
				<div class="runMonitorPanel">
					<div class="runMonitorSections">
						<div class="runMonitorSectionPane" style={`flex:${runMonitorMonitorWeight} 1 0;`} bind:this={runMonitorPaneEl}>
							<div class="runtimeEnvHead">
								<strong>Run Monitor</strong>
								<div class="runtimeEnvActions">
									<button
										type="button"
										class="tabBtn sectionToggle"
										title="Hide Run Monitor slideout"
										on:click={toggleRunMonitorSlideout}
									>
										<span class="sectionToggleIcon" aria-hidden="true">&lt;</span>
									</button>
								</div>
							</div>
							{#if !runMonitorSectionCollapsed}
								<div class="runMonitorMonitorBody">
									<div class="envPanelSummary">
										nodes {runMonitorNodeRows.length} | edges {runMonitorEdgeRows.length} | blocked {runMonitorBlockedCount} | waiting {runMonitorWaitingCount} | stalled {String(runMonitorGlobalStalled)}
									</div>
									<div class="runMonitorLegend">legend: solid=data/work | dashed teal=control link</div>
									<div class="monitorToolbar">
										<label class="monitorField">
											<span>Filter</span>
											<select bind:value={runMonitorNodeFilter}>
												<option value="all">All</option>
												<option value="blocked">Blocked</option>
												<option value="waiting">Waiting</option>
												<option value="stalled">Stalled</option>
											</select>
										</label>
										<label class="monitorField">
											<span>Sort</span>
											<select bind:value={runMonitorNodeSort}>
												<option value="depth_desc">Depth desc</option>
												<option value="depth_asc">Depth asc</option>
												<option value="pending_desc">Pending desc</option>
												<option value="pending_asc">Pending asc</option>
												<option value="label_asc">Label A-Z</option>
											</select>
										</label>
										<label class="monitorField">
											<span>Adaptive</span>
											<select
												bind:value={runMonitorAdaptiveModeOverride}
												on:change={() => persistRunMonitorSlideoutPrefs()}
											>
												<option value="default">Default (env)</option>
												<option value="off">Off</option>
												<option value="observe">Observe</option>
												<option value="enforce">Enforce</option>
											</select>
										</label>
									</div>
									<div class="envPanelSummary">
										adaptive override={runMonitorAdaptiveModeOverride === 'default' ? 'env default' : runMonitorAdaptiveModeOverride}
										| env={runMonitorAdaptiveEnvMode}
										| effective={runMonitorAdaptiveEffectiveMode}
										{#if runMonitorAdaptiveDecisionRows.length > 0}
											| last decision mode={runMonitorAdaptiveDecisionRows[0]?.mode ?? '-'}{runMonitorAdaptiveDecisionRows[0]?.enforced ? ' (enforced)' : ''}
										{/if}
									</div>
									<div class="runMonitorTablesSplit">
										<div class="runMonitorTablesPane" style={`flex:${runMonitorNodesWeight} 1 0;`} bind:this={runMonitorNodesPaneEl}>
											{#if runMonitorNodeRowsVisible.length === 0}
												<div class="envProfileEmpty">No monitor rows for current filter.</div>
											{:else}
												<div class="runMonitorNodeTable" role="table" aria-label="Run monitor nodes">
													<div class="runMonitorNodeHead" role="row">
														<span>node</span>
														<span>status</span>
														<span>pending</span>
														<span>depth</span>
														<span>blocked</span>
													</div>
													{#each runMonitorNodeRowsVisible as row (`${row.nodeId}`)}
														<button
															type="button"
															class="runMonitorNodeRow"
															role="row"
															on:click={() => focusNodeFromMonitor(row.nodeId)}
															title={`Focus ${row.label}`}
														>
															<span class="runMonitorNodeName">
																{row.label}
																{#if row.isLlmHolder}
																	<span class="mono"> (llm-holder)</span>
																{:else if row.isLlmWaiting}
																	<span class="mono"> (llm-wait)</span>
																{/if}
															</span>
															<span>{row.status}</span>
															<span>{row.pendingInputCount}</span>
															<span>{row.inboundDepth}</span>
															<span>{row.blockedReasonCode ?? '-'}</span>
														</button>
													{/each}
												</div>
											{/if}
										</div>
										<button
											type="button"
											class="runMonitorInnerSplitter"
											aria-label="Resize monitor nodes and edges sections"
											on:pointerdown={(event) => beginRunMonitorSplit('nodes_edges', event)}
										></button>
										<div class="runMonitorTablesPane" style={`flex:${runMonitorEdgesWeight} 1 0;`} bind:this={runMonitorEdgesPaneEl}>
											<div class="runMonitorEdgeTable" role="table" aria-label="Run monitor edges">
												<div class="runMonitorNodeHead" role="row">
													<span>edge</span>
													<span>port</span>
													<span>from</span>
													<span>to</span>
													<span>depth</span>
													<span>age</span>
												</div>
												{#if runMonitorEdgeRows.length === 0}
													<div class="envProfileEmpty">No edge telemetry yet.</div>
												{:else}
													{#each runMonitorEdgeRows.slice(0, 40) as row (`${row.edgeId}:${row.handle}`)}
														<button
															type="button"
															class="runMonitorNodeRow"
															role="row"
															on:click={() => focusEdgeFromMonitor(row.sourceNodeId, row.targetNodeId)}
															title={`Focus ${row.targetLabel}`}
														>
															<span class="runMonitorEdgeId">{compactEdgeId(row.edgeId)}</span>
															<span class="runMonitorEdgePort">{row.handle}</span>
															<span>{row.sourceLabel}</span>
															<span>{row.targetLabel}</span>
															<span>{row.depth}{row.blocked ? ' b' : ''}{row.full ? ' f' : ''}</span>
															<span>{row.oldestAgeSec === null ? '-' : row.oldestAgeSec.toFixed(1)}</span>
														</button>
													{/each}
												{/if}
											</div>
										</div>
									</div>
									<div class="runMonitorHistoryTable" role="table" aria-label="Adaptive decision timeline">
										<div class="monitorToolbar">
											<label class="monitorField">
												<span>Source</span>
												<ThemedSelect
													value={runMonitorAdaptiveDataSource}
													options={runMonitorAdaptiveDataSourceOptions}
													ariaLabel="Adaptive decision source"
													onValueChange={(next) => {
														if (next === 'live' || next === 'history') {
															runMonitorAdaptiveDataSource = next;
														}
													}}
												/>
											</label>
											{#if runMonitorAdaptiveDataSource === 'history'}
												<label class="monitorField">
													<span>Sort</span>
													<ThemedSelect
														value={runMonitorAdaptiveHistorySort}
														options={runMonitorAdaptiveHistorySortOptions}
														ariaLabel="Adaptive history sort"
														onValueChange={(next) => {
															if (
																next === 'created_desc' ||
																next === 'created_asc' ||
																next === 'impact_desc'
															) {
																runMonitorAdaptiveHistorySort = next;
															}
														}}
													/>
												</label>
											{/if}
											<label class="monitorField">
												<span>Mode</span>
												<select bind:value={runMonitorAdaptiveModeFilter}>
													<option value="all">All</option>
													<option value="off">off</option>
													<option value="observe">observe</option>
													<option value="enforce">enforce</option>
												</select>
											</label>
											<label class="monitorField">
												<span>Severity</span>
												<select bind:value={runMonitorAdaptiveSeverityFilter}>
													<option value="all">All</option>
													<option value="low">low</option>
													<option value="medium">medium</option>
													<option value="high">high</option>
												</select>
											</label>
											<label class="monitorField monitorField-inline">
												<span>Changed only</span>
												<input type="checkbox" bind:checked={runMonitorAdaptiveChangedOnly} />
											</label>
											<label class="monitorField">
												<span>Min score</span>
												<input
													type="number"
													min="0"
													max="100"
													step="1"
													bind:value={runMonitorAdaptiveMinScore}
												/>
											</label>
										</div>
										<div class="runMonitorNodeHead runMonitorAdaptiveTimelineHead" role="row">
											<span>time</span>
											<span>mode</span>
											<span>score</span>
											<span>changed</span>
											<span>diff</span>
											<span>effective caps</span>
											<span>reasons</span>
										</div>
										<div class="envPanelSummary">
											source={runMonitorAdaptiveDataSource}
											| 
											total={runMonitorAdaptiveDecisionSummary.total}
											| enforced={runMonitorAdaptiveDecisionSummary.enforced}
											| modes:
											observe={Number(runMonitorAdaptiveDecisionSummary.byMode.observe ?? 0)},
											enforce={Number(runMonitorAdaptiveDecisionSummary.byMode.enforce ?? 0)},
											off={Number(runMonitorAdaptiveDecisionSummary.byMode.off ?? 0)}
											| severity:
											low={runMonitorAdaptiveDecisionSummary.bySeverity.low},
											medium={runMonitorAdaptiveDecisionSummary.bySeverity.medium},
											high={runMonitorAdaptiveDecisionSummary.bySeverity.high}
										</div>
										{#if runMonitorAdaptiveSparkline}
											<div class="runMonitorSparklineCard">
												<div class="runMonitorSparklineHead">
													<span>Adaptive Score Trend</span>
													<span class="mono">
														last={runMonitorAdaptiveSparkline.lastValue.toFixed(1)}
														| delta={runMonitorAdaptiveSparkline.deltaValue >= 0 ? '+' : ''}{runMonitorAdaptiveSparkline.deltaValue.toFixed(1)}
														{#if runMonitorAdaptiveSparkline.deltaPct !== null}
															({runMonitorAdaptiveSparkline.deltaPct >= 0 ? '+' : ''}{runMonitorAdaptiveSparkline.deltaPct.toFixed(1)}%)
														{/if}
													</span>
												</div>
												<svg
													class="runMonitorSparkline"
													viewBox={`0 0 ${runMonitorAdaptiveSparkline.width} ${runMonitorAdaptiveSparkline.height}`}
													preserveAspectRatio="none"
													role="img"
													aria-label="Adaptive decision score trend"
													on:pointermove={onAdaptiveSparklineMove}
													on:pointerleave={() => (runMonitorAdaptiveHoverIndex = -1)}
												>
													<polyline
														points={`0,${runMonitorAdaptiveSparkline.height} ${runMonitorAdaptiveSparkline.width},${runMonitorAdaptiveSparkline.height}`}
														class="runMonitorSparklineBase"
													/>
													<line
														x1="0"
														y1={runMonitorAdaptiveSparkline.baselines.firstValueY}
														x2={runMonitorAdaptiveSparkline.width}
														y2={runMonitorAdaptiveSparkline.baselines.firstValueY}
														class="runMonitorSparklineBaseline runMonitorSparklineBaseline-first"
													/>
													<line
														x1="0"
														y1={runMonitorAdaptiveSparkline.baselines.meanValueY}
														x2={runMonitorAdaptiveSparkline.width}
														y2={runMonitorAdaptiveSparkline.baselines.meanValueY}
														class="runMonitorSparklineBaseline runMonitorSparklineBaseline-mean"
													/>
													<path d={runMonitorAdaptiveSparkline.path} class="runMonitorSparklinePath"></path>
													{#if runMonitorAdaptiveHoverPoint}
														<line
															x1={runMonitorAdaptiveHoverPoint.x}
															y1="0"
															x2={runMonitorAdaptiveHoverPoint.x}
															y2={runMonitorAdaptiveSparkline.height}
															class="runMonitorSparklineHoverGuide"
														/>
														<circle
															cx={runMonitorAdaptiveHoverPoint.x}
															cy={runMonitorAdaptiveHoverPoint.y}
															r="3.5"
															class="runMonitorSparklineHoverPoint"
														/>
													{/if}
												</svg>
												{#if runMonitorAdaptiveHoverPoint}
													<div class="runMonitorSparklineTooltip mono">
														{runMonitorAdaptiveHoverPoint.createdAt || '-'} | score={runMonitorAdaptiveHoverPoint.value.toFixed(1)}
													</div>
													<button
														type="button"
														class="tabBtn"
														on:click={focusAdaptiveSparklineHoverPoint}
														title="Focus the hovered adaptive decision point"
													>
														Focus point
													</button>
												{/if}
												<div class="runMonitorSparklineFoot mono">
													min={runMonitorAdaptiveSparkline.minValue.toFixed(1)}
													| max={runMonitorAdaptiveSparkline.maxValue.toFixed(1)}
													| points={runMonitorAdaptiveSparkline.pointsCount}
												</div>
											</div>
										{/if}
										{#if runMonitorAdaptiveRowsVisible.length === 0}
											<div class="envProfileEmpty">No adaptive scheduler decisions yet.</div>
										{:else}
											{#each runMonitorAdaptiveRowsVisible.slice(0, 20) as row (`${row.at}:${row.runId}`)}
												<button
													type="button"
													class="runMonitorNodeRow runMonitorAdaptiveTimelineRow"
													role="row"
													on:click={() => selectAdaptiveDecisionDrilldown(row)}
													title="Select adaptive decision details and filter logs by run"
												>
													<span class="mono">{row.at || '-'}</span>
													<span>{row.mode}{row.enforced ? ' (enforced)' : ''}</span>
													<span>
														<span class={adaptiveSeverityClass(row.explanation.severity)}>
															{row.explanation.score}
														</span>
													</span>
													<span>
														{#if Object.keys(row.changedCaps).length === 0}
															-
														{:else}
															{Object.entries(row.changedCaps)
																.map(([key, delta]) => `${key}:${delta.from}->${delta.to}`)
																.join(' | ')}
														{/if}
													</span>
													<span>
														{#if row.diffFromPrevious}
															{row.diffFromPrevious.scoreDelta >= 0 ? '+' : ''}{row.diffFromPrevious.scoreDelta}
															{#if row.diffFromPrevious.modeChanged}
																| mode
															{/if}
															{#if Object.keys(row.diffFromPrevious.capDelta).length > 0}
																| caps={Object.keys(row.diffFromPrevious.capDelta).length}
															{/if}
															{#if row.diffFromPrevious.reasonsAdded.length > 0}
																| +r={row.diffFromPrevious.reasonsAdded.length}
															{/if}
															{#if row.diffFromPrevious.reasonsRemoved.length > 0}
																| -r={row.diffFromPrevious.reasonsRemoved.length}
															{/if}
														{:else}
															-
														{/if}
													</span>
													<span class="mono">
														{#if Object.keys(row.effectiveCaps).length === 0}
															-
														{:else}
															{Object.entries(row.effectiveCaps)
																.map(([key, value]) => `${key}=${value}`)
																.join(' ')}
														{/if}
													</span>
													<span>
														{#if row.reasons.length === 0}
															-
														{:else}
															{row.reasons.join(', ')}
														{/if}
													</span>
												</button>
											{/each}
										{/if}
									</div>
									<div class="runMonitorHistoryTable" role="table" aria-label="Adaptive decision detail">
										<div class="runtimeEnvHead">
											<strong>Adaptive Decision Detail</strong>
										</div>
										{#if !selectedAdaptiveDecision}
											<div class="envProfileEmpty">Select an adaptive decision row to view full diagnostics.</div>
										{:else}
											<div class="envPanelSummary">
												run={selectedAdaptiveDecision.runId} | at={selectedAdaptiveDecision.at} | mode={selectedAdaptiveDecision.mode}{selectedAdaptiveDecision.enforced ? ' (enforced)' : ''}
											</div>
											<div class="envPanelSummary">
												score=
												<span class={adaptiveSeverityClass(selectedAdaptiveDecision.explanation.severity)}>
													{selectedAdaptiveDecision.explanation.score}
												</span>
												| signals={selectedAdaptiveDecision.explanation.signals.length}
												| components={selectedAdaptiveDecision.explanation.components.length}
											</div>
											{#if selectedAdaptiveDecision.explanation.components.length > 0}
												<div class="envPanelSummary mono">
													weights:
													{selectedAdaptiveDecision.explanation.components
														.slice(0, 8)
														.map((item) => `${item.label}:${item.delta >= 0 ? '+' : ''}${item.delta}`)
														.join(' | ')}
												</div>
												<div class="adaptiveComponentBars" role="list" aria-label="Adaptive decision component impact">
													{#each selectedAdaptiveDecisionComponents.slice(0, 8) as component (`${component.label}:${component.delta}`)}
														<div class="adaptiveComponentBarRow" role="listitem">
															<span class="adaptiveComponentLabel mono">{component.label}</span>
															<div class="adaptiveComponentTrack">
																<div
																	class={`adaptiveComponentFill ${component.direction === 'up' ? 'adaptiveComponentFill-up' : 'adaptiveComponentFill-down'}`}
																	style={`width:${component.percentOfMax.toFixed(1)}%;`}
																></div>
															</div>
															<span class="adaptiveComponentDelta mono">{component.delta >= 0 ? '+' : ''}{component.delta.toFixed(2)}</span>
														</div>
													{/each}
												</div>
											{/if}
											{#if selectedAdaptiveDecision.diffFromPrevious}
												<div class="envPanelSummary">
													diff score=
													{selectedAdaptiveDecision.diffFromPrevious.scoreDelta >= 0 ? '+' : ''}{selectedAdaptiveDecision.diffFromPrevious.scoreDelta}
													| mode changed={selectedAdaptiveDecision.diffFromPrevious.modeChanged ? 'yes' : 'no'}
													| cap changes={Object.keys(selectedAdaptiveDecision.diffFromPrevious.capDelta).length}
												</div>
												{#if selectedAdaptiveDecision.diffFromPrevious.reasonsAdded.length > 0}
													<div class="envPanelSummary">
														reasons added: {selectedAdaptiveDecision.diffFromPrevious.reasonsAdded.join(', ')}
													</div>
												{/if}
												{#if selectedAdaptiveDecision.diffFromPrevious.reasonsRemoved.length > 0}
													<div class="envPanelSummary">
														reasons removed: {selectedAdaptiveDecision.diffFromPrevious.reasonsRemoved.join(', ')}
													</div>
												{/if}
												{#if Object.keys(selectedAdaptiveDecision.diffFromPrevious.capDelta).length > 0}
													<div class="envPanelSummary mono">
														cap deltas:
														{Object.entries(selectedAdaptiveDecision.diffFromPrevious.capDelta)
															.map(([key, delta]) => `${key}:${delta.from}->${delta.to}`)
															.join(' | ')}
													</div>
												{/if}
											{/if}
											{#if selectedAdaptiveDecisionPrevious}
												<div class="envPanelSummary">
													previous at={selectedAdaptiveDecisionPrevious.at} mode={selectedAdaptiveDecisionPrevious.mode}
												</div>
											{/if}
											{#if selectedAdaptiveCapRows.length > 0}
												<div class="runMonitorNodeHead" role="row">
													<span>cap</span>
													<span>hard</span>
													<span>min</span>
													<span>proposed</span>
													<span>effective / changed</span>
												</div>
												{#each selectedAdaptiveCapRows as cap (`${cap.key}:${cap.changed}`)}
													<div class="runMonitorNodeRow" role="row">
														<span class="mono">{cap.key}</span>
														<span>{cap.hard}</span>
														<span>{cap.min}</span>
														<span>{cap.proposed}</span>
														<span>{cap.effective} | {cap.changed}</span>
													</div>
												{/each}
											{/if}
											<pre class="runMonitorJsonDetail">{JSON.stringify({
												explanation: selectedAdaptiveDecision.explanation,
												inputs: selectedAdaptiveDecision.inputs,
												reasons: selectedAdaptiveDecision.reasons,
												changedCaps: selectedAdaptiveDecision.changedCaps,
												hardCaps: selectedAdaptiveDecision.hardCaps,
												minCaps: selectedAdaptiveDecision.minCaps,
												proposedCaps: selectedAdaptiveDecision.proposedCaps,
												effectiveCaps: selectedAdaptiveDecision.effectiveCaps
											}, null, 2)}</pre>
										{/if}
									</div>
									<div class="runMonitorHistoryTable" role="table" aria-label="Regression detection alerts">
										<div class="runtimeEnvHead">
											<strong>Regression Alerts</strong>
											<div class="runtimeEnvActions">
												<button
													type="button"
													class="tabBtn"
													on:click={() =>
														void refreshRunMonitorRegressions(
															runMonitorRegressionPair.runId,
															runMonitorRegressionPair.baselineRunId
														)}
													disabled={
														runMonitorRegressionLoading ||
														!runMonitorRegressionPair.runId ||
														!runMonitorRegressionPair.baselineRunId
													}
												>
													{runMonitorRegressionLoading ? 'Loading...' : 'Reload'}
												</button>
												{#if runMonitorRegressionRunOverride && runMonitorRegressionBaselineOverride}
													<button
														type="button"
														class="tabBtn"
														on:click={clearRegressionHistoryPairOverride}
														title="Revert to latest run pair"
													>
														Use latest pair
													</button>
												{/if}
											</div>
										</div>
										<div class="monitorToolbar">
											<label class="monitorField">
												<span>Type</span>
												<select bind:value={runMonitorRegressionTypeFilter}>
													<option value="all">all</option>
													<option value="latency">latency</option>
													<option value="failure">failure</option>
												</select>
											</label>
											<label class="monitorField">
												<span>Severity</span>
												<select bind:value={runMonitorRegressionSeverityFilter}>
													<option value="all">all</option>
													<option value="high">high</option>
													<option value="medium">medium</option>
													<option value="low">low</option>
												</select>
											</label>
											<label class="monitorField">
												<span>Sort</span>
												<select bind:value={runMonitorRegressionSort}>
													<option value="default">default</option>
													<option value="impact_desc">impact desc</option>
													<option value="impact_asc">impact asc</option>
												</select>
											</label>
										</div>
										<div class="envPanelSummary">
											run={runMonitorRegressionRunId || runMonitorRegressionPair.runId || '-'} | baseline={runMonitorRegressionBaselineRunId || runMonitorRegressionPair.baselineRunId || '-'} | filter={runMonitorRegressionTypeFilter} | severity={runMonitorRegressionSeverityFilter}
										</div>
										<div class="runMonitorNodeHead" role="row">
											<span>type</span>
											<span>target</span>
											<span>baseline</span>
											<span>current</span>
											<span>drift</span>
										</div>
										{#if runMonitorRegressionError}
											<div class="envProfileError">{runMonitorRegressionError}</div>
										{:else if runMonitorRegressionAlerts.length === 0}
											<div class="envProfileEmpty">No regression alerts for the latest completed runs.</div>
										{:else}
											{#each runMonitorRegressionAlerts as alert, index (`${alert.reasonCode}:${alert.nodeId ?? alert.errorCode ?? ''}:${index}`)}
												<button
													type="button"
													class={`runMonitorNodeRow ${
														index === runMonitorRegressionSelectedIndex
															? 'runMonitorNodeRow-highlighted'
															: ''
													}`}
													role="row"
													on:click={() => selectRegressionAlertDrilldown(alert, index)}
													title="Focus alert node"
												>
													<span>
														{alert.reasonCode || alert.type || '-'}
														<span class={adaptiveSeverityClass(regressionSeverity(alert))}>
															{regressionSeverity(alert)}
														</span>
													</span>
													<span>{alert.nodeId || alert.errorCode || '-'}</span>
													<span>{Number.isFinite(Number(alert.baseline)) ? Number(alert.baseline).toFixed(1) : '-'}</span>
													<span>{Number.isFinite(Number(alert.current)) ? Number(alert.current).toFixed(1) : '-'}</span>
													<span>
														{#if Number.isFinite(Number(alert.driftPct))}
															{Number(alert.driftPct).toFixed(1)}%
														{:else if Number.isFinite(Number(alert.delta))}
															{Number(alert.delta)}
														{:else}
															-
														{/if}
													</span>
												</button>
											{/each}
										{/if}
										{#if selectedRegressionAlert}
											<div class="envPanelSummary">
												selected={selectedRegressionAlert.reasonCode || selectedRegressionAlert.type || '-'} | node={selectedRegressionAlert.nodeId || '-'} | metric={selectedRegressionAlert.metric || '-'}
												| severity=
												<span class={adaptiveSeverityClass(regressionSeverity(selectedRegressionAlert))}>
													{regressionSeverity(selectedRegressionAlert)}
												</span>
											</div>
											<div class="envPanelSummary mono">
												baseline={Number.isFinite(Number(selectedRegressionAlert.baseline)) ? Number(selectedRegressionAlert.baseline).toFixed(3) : '-'} | current={Number.isFinite(Number(selectedRegressionAlert.current)) ? Number(selectedRegressionAlert.current).toFixed(3) : '-'} | driftPct={Number.isFinite(Number(selectedRegressionAlert.driftPct)) ? Number(selectedRegressionAlert.driftPct).toFixed(3) : '-'} | delta={Number.isFinite(Number(selectedRegressionAlert.delta)) ? Number(selectedRegressionAlert.delta).toFixed(3) : '-'}
											</div>
											<div class="envPanelSummary">
												thresholdPct={Number.isFinite(Number(selectedRegressionAlert.thresholdPct)) ? Number(selectedRegressionAlert.thresholdPct).toFixed(3) : '-'} | thresholdAbs={Number.isFinite(Number(selectedRegressionAlert.thresholdAbs)) ? Number(selectedRegressionAlert.thresholdAbs).toFixed(3) : '-'} | errorCode={selectedRegressionAlert.errorCode || '-'}
											</div>
											{#if runMonitorRegressionSummaryLoading}
												<div class="envProfileEmpty">Loading run summaries…</div>
											{:else if runMonitorRegressionSummaryError}
												<div class="envProfileError">{runMonitorRegressionSummaryError}</div>
											{:else if runMonitorRegressionCurrentSummary && runMonitorRegressionBaselineSummary}
												<div class="envPanelSummary mono">
													current status={String((runMonitorRegressionCurrentSummary as any)?.status ?? '-')}
													| runtime={Number((runMonitorRegressionCurrentSummary as any)?.analytics?.runTelemetry?.runtime_ms ?? 0)}
													| peak={Number((runMonitorRegressionCurrentSummary as any)?.analytics?.runTelemetry?.peak_concurrency ?? 0)}
												</div>
												<div class="envPanelSummary mono">
													baseline status={String((runMonitorRegressionBaselineSummary as any)?.status ?? '-')}
													| runtime={Number((runMonitorRegressionBaselineSummary as any)?.analytics?.runTelemetry?.runtime_ms ?? 0)}
													| peak={Number((runMonitorRegressionBaselineSummary as any)?.analytics?.runTelemetry?.peak_concurrency ?? 0)}
												</div>
											{/if}
										{/if}
									</div>
									<div class="runMonitorHistoryTable" role="table" aria-label="State transition audit trail">
										<div class="runtimeEnvHead">
											<strong>State Transitions</strong>
											<div class="runtimeEnvActions">
												<button
													type="button"
													class="tabBtn"
													on:click={() => void refreshRunMonitorTransitions(runMonitorTransitionRunId)}
													disabled={runMonitorTransitionsLoading || !runMonitorTransitionRunId}
												>
													{runMonitorTransitionsLoading ? 'Loading...' : 'Reload'}
												</button>
											</div>
										</div>
										<div class="monitorToolbar">
											<label class="monitorField">
												<span>Filter</span>
												<select bind:value={runMonitorTransitionFilter}>
													<option value="all">All</option>
													<option value="run">Run only</option>
													<option value="node">Node only</option>
													<option value="violations">Violations</option>
												</select>
											</label>
										</div>
										<div class="envPanelSummary">
											run={runMonitorTransitionRunId || '-'} | events={runMonitorTransitionsVisible.length}
										</div>
										<div class="runMonitorNodeHead" role="row">
											<span>event</span>
											<span>entity</span>
											<span>from</span>
											<span>to</span>
											<span>reason/code</span>
										</div>
										{#if runMonitorTransitionsError}
											<div class="envProfileError">{runMonitorTransitionsError}</div>
										{:else if runMonitorTransitionsVisible.length === 0}
											<div class="envProfileEmpty">No state transition events for selected run.</div>
										{:else}
											{#each runMonitorTransitionsVisible as event (`${event.id}:${event.at}:${event.type}`)}
												<button
													type="button"
													class="runMonitorNodeRow"
													role="row"
													on:click={() => selectTransitionEventDrilldown(event)}
													title="Focus node transitions or filter run logs by run id"
												>
													<span>{event.type}</span>
													<span>{event.entity || '-'}:{event.entityId || '-'}</span>
													<span>{event.source || '-'}</span>
													<span>{event.target || '-'}</span>
													<span>{event.reasonCode || '-'}</span>
												</button>
											{/each}
										{/if}
									</div>
									<div class="runMonitorHistoryTable" role="table" aria-label="Historical analytics drilldown">
										<div class="runtimeEnvHead">
											<strong>Historical Drilldown</strong>
											<div class="runtimeEnvActions">
												<button
													type="button"
													class="tabBtn"
													on:click={() => void refreshRunMonitorAnalytics()}
													disabled={runMonitorAnalyticsLoading}
												>
													{runMonitorAnalyticsLoading ? 'Loading...' : 'Reload'}
												</button>
											</div>
										</div>
										<div class="monitorToolbar">
											<label class="monitorField">
												<span>Start (ISO)</span>
												<input
													type="text"
													placeholder="2026-03-31T00:00:00Z"
													bind:value={runMonitorAnalyticsStartAt}
												/>
											</label>
											<label class="monitorField">
												<span>End (ISO)</span>
												<input
													type="text"
													placeholder="2026-03-31T23:59:59Z"
													bind:value={runMonitorAnalyticsEndAt}
												/>
											</label>
											<label class="monitorField">
												<span>Run page</span>
												<input
													type="number"
													min="0"
													step="1"
													bind:value={runMonitorAnalyticsOffset}
												/>
											</label>
											<label class="monitorField">
												<span>Run sort</span>
												<select bind:value={runMonitorRunTrendSort}>
													<option value="created_asc">created asc</option>
													<option value="created_desc">created desc</option>
													<option value="runtime_desc">runtime desc</option>
												</select>
											</label>
											<label class="monitorField">
												<span>Node</span>
												<select bind:value={runMonitorTrendNodeId}>
													{#if runMonitorTrendNodeOptions.length === 0}
														<option value="">(none)</option>
													{:else}
														{#each runMonitorTrendNodeOptions as option (`${option.id}`)}
															<option value={option.id}>{option.label}</option>
														{/each}
													{/if}
												</select>
											</label>
											<label class="monitorField">
												<span>Metric</span>
												<select bind:value={runMonitorTrendMetric}>
													<option value="p95Ms">p95Ms</option>
													<option value="p50Ms">p50Ms</option>
													<option value="avgMs">avgMs</option>
													<option value="maxMs">maxMs</option>
													<option value="count">count</option>
												</select>
											</label>
											<label class="monitorField">
												<span>Node sort</span>
												<select bind:value={runMonitorNodeTrendSort}>
													<option value="created_asc">created asc</option>
													<option value="created_desc">created desc</option>
													<option value="value_desc">value desc</option>
												</select>
											</label>
											<label class="monitorField">
												<span>SLA p95</span>
												<input
													type="number"
													min="1"
													step="50"
													bind:value={runMonitorSlaThresholdMs}
												/>
											</label>
											<label class="monitorField">
												<span>Bottleneck sort</span>
												<select bind:value={runMonitorBottleneckSort}>
													<option value="score_desc">score desc</option>
													<option value="score_asc">score asc</option>
													<option value="p95_desc">p95 desc</option>
												</select>
											</label>
										</div>
										{#if runMonitorAnalyticsError}
											<div class="envProfileError">{runMonitorAnalyticsError}</div>
										{:else}
											<div class="runMonitorNodeHead" role="row">
												<span>run</span>
												<span>status</span>
												<span>runtime ms</span>
												<span>peak conc</span>
												<span>created</span>
											</div>
											{#if runMonitorRunTrendPoints.length === 0}
												<div class="envProfileEmpty">No run trend points in current window.</div>
											{:else}
												{#each runMonitorRunTrendPoints as point (`${point.runId}:${point.createdAt}`)}
													<button
														type="button"
														class={`runMonitorNodeRow ${
															String(point.runId ?? '').trim() === runMonitorSelectedRunTrendId
																? 'runMonitorNodeRow-highlighted'
																: ''
														}`}
														role="row"
														on:click={() => selectRunTrendDrilldown(point)}
														title="Filter run logs by run id"
													>
														<span class="mono">{point.runId}</span>
														<span>{point.status || '-'}</span>
														<span>{Number(point.runtimeMs ?? 0)}</span>
														<span>{Number(point.peakConcurrency ?? 0)}</span>
														<span>{point.createdAt}</span>
													</button>
												{/each}
											{/if}
											{#if runMonitorSelectedRunTrendId}
												<div class="runMonitorSparklineCard">
													<div class="runMonitorSparklineHead">
														<span>Run Drilldown</span>
														<span class="mono">run={runMonitorSelectedRunTrendId}</span>
													</div>
													{#if runMonitorSelectedRunSummaryLoading}
														<div class="envProfileEmpty">Loading run summary…</div>
													{:else if runMonitorSelectedRunSummaryError}
														<div class="envProfileError">{runMonitorSelectedRunSummaryError}</div>
													{:else if runMonitorSelectedRunSummary}
														<div class="runMonitorRunSummary mono">
															status={String((runMonitorSelectedRunSummary as any)?.status ?? '-')}
															| runtime={Number((runMonitorSelectedRunSummary as any)?.analytics?.runTelemetry?.runtime_ms ?? 0)}
															| peak={Number((runMonitorSelectedRunSummary as any)?.analytics?.runTelemetry?.peak_concurrency ?? 0)}
															| nodeLatencyKeys={Object.keys((runMonitorSelectedRunSummary as any)?.analytics?.nodeLatencyMs ?? {}).length}
															| failureKinds={Object.keys((runMonitorSelectedRunSummary as any)?.analytics?.failureCategories ?? {}).length}
														</div>
													{:else}
														<div class="envProfileEmpty">No run summary available.</div>
													{/if}
												</div>
											{/if}
											{#if runMonitorTrendSparkline}
												<div class="runMonitorSparklineCard">
													<div class="runMonitorSparklineHead">
														<span>Trend Sparkline</span>
														<span class="mono">
															last={runMonitorTrendSparkline.lastValue.toFixed(1)}
															| delta={runMonitorTrendSparkline.deltaValue >= 0 ? '+' : ''}{runMonitorTrendSparkline.deltaValue.toFixed(1)}
															{#if runMonitorTrendSparkline.deltaPct !== null}
																({runMonitorTrendSparkline.deltaPct >= 0 ? '+' : ''}{runMonitorTrendSparkline.deltaPct.toFixed(1)}%)
															{/if}
														</span>
													</div>
													<svg
														class="runMonitorSparkline"
														viewBox={`0 0 ${runMonitorTrendSparkline.width} ${runMonitorTrendSparkline.height}`}
														preserveAspectRatio="none"
														role="img"
														aria-label="Node metric trend sparkline"
														on:pointermove={onTrendSparklineMove}
														on:pointerleave={() => (runMonitorTrendHoverIndex = -1)}
													>
														<polyline
															points={`0,${runMonitorTrendSparkline.height} ${runMonitorTrendSparkline.width},${runMonitorTrendSparkline.height}`}
															class="runMonitorSparklineBase"
														/>
														<line
															x1="0"
															y1={runMonitorTrendSparkline.baselines.firstValueY}
															x2={runMonitorTrendSparkline.width}
															y2={runMonitorTrendSparkline.baselines.firstValueY}
															class="runMonitorSparklineBaseline runMonitorSparklineBaseline-first"
														/>
														<line
															x1="0"
															y1={runMonitorTrendSparkline.baselines.meanValueY}
															x2={runMonitorTrendSparkline.width}
															y2={runMonitorTrendSparkline.baselines.meanValueY}
															class="runMonitorSparklineBaseline runMonitorSparklineBaseline-mean"
														/>
														<path d={runMonitorTrendSparkline.path} class="runMonitorSparklinePath"></path>
														{#if runMonitorTrendHoverPoint}
															<line
																x1={runMonitorTrendHoverPoint.x}
																y1="0"
																x2={runMonitorTrendHoverPoint.x}
																y2={runMonitorTrendSparkline.height}
																class="runMonitorSparklineHoverGuide"
															/>
															<circle
																cx={runMonitorTrendHoverPoint.x}
																cy={runMonitorTrendHoverPoint.y}
																r="3.5"
																class="runMonitorSparklineHoverPoint"
															/>
														{/if}
													</svg>
													{#if runMonitorTrendHoverPoint}
														<div class="runMonitorSparklineTooltip mono">
															{runMonitorTrendHoverPoint.createdAt || '-'} | value={runMonitorTrendHoverPoint.value.toFixed(1)}
														</div>
													{/if}
													<div class="runMonitorSparklineFoot mono">
														min={runMonitorTrendSparkline.minValue.toFixed(1)} | max={runMonitorTrendSparkline.maxValue.toFixed(1)} | points={runMonitorTrendSparkline.pointsCount}
													</div>
												</div>
											{/if}
											<div class="runMonitorNodeHead" role="row">
												<span>trend run</span>
												<span>node</span>
												<span>metric</span>
												<span>value</span>
												<span>created</span>
											</div>
											{#if runMonitorTrendPoints.length === 0}
												<div class="envProfileEmpty">No trend points for selected node/metric.</div>
											{:else}
												{#each runMonitorTrendPoints as point (`${point.runId}:${point.createdAt}:${point.nodeId}`)}
													<button
														type="button"
														class={`runMonitorNodeRow ${runMonitorTrendHoverCreatedAt && runMonitorTrendHoverCreatedAt === String(point.createdAt ?? '').trim() ? 'runMonitorNodeRow-highlighted' : ''}`}
														role="row"
														on:click={() => selectTrendPointDrilldown(point)}
														title="Focus node and set trend metric"
													>
														<span class="mono">{point.runId}</span>
														<span>{point.nodeId}</span>
														<span>{point.metric}</span>
														<span>{Number(point.value ?? 0).toFixed(1)}</span>
														<span>{point.createdAt}</span>
													</button>
												{/each}
											{/if}
											<div class="runMonitorNodeHead" role="row">
												<span>sla run</span>
												<span>node</span>
												<span>p95</span>
												<span>threshold</span>
												<span>created</span>
											</div>
											{#if runMonitorSlaBreaches.length === 0}
												<div class="envProfileEmpty">No SLA breaches at current threshold.</div>
											{:else}
												{#each runMonitorSlaBreaches as breach (`${breach.runId}:${breach.nodeId}:${breach.createdAt}`)}
													<button
														type="button"
														class="runMonitorNodeRow"
														role="row"
														on:click={() => selectSlaBreachDrilldown(breach)}
														title="Focus node and open p95 trend"
													>
														<span class="mono">{breach.runId}</span>
														<span>{breach.nodeId}</span>
														<span>{Number(breach.p95Ms ?? 0).toFixed(1)}</span>
														<span>{Number(breach.thresholdMs ?? 0).toFixed(1)}</span>
														<span>{breach.createdAt}</span>
													</button>
												{/each}
											{/if}
											<div class="runMonitorNodeHead" role="row">
												<span>bottleneck node</span>
												<span>score</span>
												<span>p95 avg</span>
												<span>runs</span>
												<span>count sum</span>
											</div>
											{#if runMonitorBottleneckNodes.length === 0}
												<div class="envProfileEmpty">No bottleneck rows in current window.</div>
											{:else}
												{#each runMonitorBottleneckNodes as item (`${item.nodeId}`)}
													<button
														type="button"
														class="runMonitorNodeRow"
														role="row"
														on:click={() => selectBottleneckDrilldown(item)}
														title="Focus node and open p95 trend"
													>
														<span>{item.nodeId}</span>
														<span>{Number(item.bottleneckScore ?? 0).toFixed(1)}</span>
														<span>{Number(item.p95AvgMs ?? 0).toFixed(1)}</span>
														<span>{Number(item.runsSeen ?? 0)}</span>
														<span>{Number(item.countSum ?? 0)}</span>
													</button>
												{/each}
											{/if}
											<div class="runMonitorNodeHead" role="row">
												<span>error code</span>
												<span>count</span>
												<span></span>
												<span></span>
												<span></span>
											</div>
											{#if runMonitorFailureTaxonomy.length === 0}
												<div class="envProfileEmpty">No failure taxonomy rows.</div>
											{:else}
												{#each runMonitorFailureTaxonomy as item (`${item.errorCode}`)}
													<button
														type="button"
														class="runMonitorNodeRow"
														role="row"
														on:click={() => selectFailureTaxonomyDrilldown(item)}
														title="Filter run logs by error code"
													>
														<span>{item.errorCode}</span>
														<span>{item.count}</span>
														<span></span>
														<span></span>
														<span></span>
													</button>
												{/each}
											{/if}
										{/if}
									</div>
									{#if runMonitorShowHistory}
										<div class="runMonitorHistoryTable" role="table" aria-label="Run monitor history">
											<div class="runMonitorNodeHead" role="row">
												<span>run</span>
												<span>status</span>
												<span>runtime</span>
												<span>max q</span>
												<span>flags</span>
											</div>
											{#if runMonitorHistoryRows.length === 0}
												<div class="envProfileEmpty">No finished runs yet.</div>
											{:else}
												{#each runMonitorHistoryRows.slice(0, 8) as row, index (`${String(row.runId ?? '')}:${String(row.finishedAt ?? '')}`)}
													<button
														type="button"
														class={`runMonitorNodeRow ${
															String(row.runId ?? '').trim() === runMonitorRegressionPair.runId ||
															String(row.runId ?? '').trim() === runMonitorRegressionPair.baselineRunId
																? 'runMonitorNodeRow-highlighted'
																: ''
														}`}
														role="row"
														on:click={() => selectRegressionHistoryPair(index)}
														title="Use this run and the next row as regression pair"
														disabled={!runMonitorHistoryRows[index + 1]}
													>
														<span class="mono">{String(row.runId ?? '-')}</span>
														<span>{String(row.status ?? '-')}</span>
														<span>{Number(row.runtimeMs ?? 0)}</span>
														<span>{Number(row.maxPendingQueueDepth ?? 0)}</span>
														<span>
															{#if Boolean(row.hadStalledSnapshot ?? false)}
																stalled
															{/if}
															{#if Number(row.blockedEvents ?? 0) > 0}
																{Boolean(row.hadStalledSnapshot ?? false) ? ' | ' : ''}blocked={Number(row.blockedEvents ?? 0)}
															{/if}
															{#if !Boolean(row.hadStalledSnapshot ?? false) && Number(row.blockedEvents ?? 0) === 0}
																-
															{/if}
														</span>
													</button>
												{/each}
											{/if}
										</div>
									{/if}
								</div>
							{:else}
								<div class="envPanelSummary">Run monitor section is collapsed.</div>
							{/if}
						</div>

						{#if !runMonitorSectionCollapsed && !slideoutEnvironmentCollapsed}
							<button
								type="button"
								class="runMonitorInnerSplitter"
								aria-label="Resize run monitor and environment sections"
								on:pointerdown={(event) => beginRunMonitorSplit('monitor_env', event)}
							></button>
						{/if}

						<div
							class="runMonitorSectionPane runMonitorEnvSection"
							class:is-collapsed={slideoutEnvironmentCollapsed}
							style={`flex:${slideoutEnvironmentCollapsed ? '0 0 auto' : `${runMonitorEnvWeight} 1 0`};`}
							bind:this={runMonitorEnvPaneEl}
						>
							<div class="runtimeEnvHead">
								<strong>Environment Variables</strong>
								<div class="runtimeEnvActions">
									<button
										type="button"
										class="pill pinBtn"
										title={slideoutEnvironmentCollapsed ? 'Expand environment section' : 'Collapse environment section'}
										on:click={() => (slideoutEnvironmentCollapsed = !slideoutEnvironmentCollapsed)}
									>
										Env
									</button>
								</div>
							</div>
							{#if !slideoutEnvironmentCollapsed}
								<div class="runtimeEnvPanel">
									<div class="runtimeEnvHead">
										<strong>Runtime Env Vars</strong>
										<div class="runtimeEnvActions">
											<button class="tabBtn" on:click={() => void refreshRuntimeEnvPanel()} disabled={runtimeEnvLoading}>
												{runtimeEnvLoading ? 'Loading...' : 'Reload'}
											</button>
											<label class="runtimeEnvToggle">
												<input type="checkbox" bind:checked={runtimeEnvRevealSensitive} on:change={() => void refreshRuntimeEnvPanel()} />
												<span>Reveal sensitive</span>
											</label>
										</div>
									</div>
									<input class="logFilterInput" placeholder="Filter env vars..." bind:value={runtimeEnvFilter} aria-label="Filter runtime env vars" />
									{#if runtimeEnvError}
										<div class="envProfileError">{runtimeEnvError}</div>
									{/if}
									{#if runtimeEnvRows.length === 0}
										<div class="envProfileEmpty">No runtime env vars available.</div>
									{:else}
										<div class="runtimeEnvTable" role="table" aria-label="Runtime env vars">
											{#each runtimeEnvRows as row (`${row.name}`)}
												<div class="runtimeEnvRow" role="row">
													<div class="runtimeEnvMeta">
														<div class="mono runtimeEnvName">{row.name}{#if row.restartRequired}<span class="runtimeEnvRestart">restart</span>{/if}</div>
														<div class="runtimeEnvSub">{row.category} | source={row.source}{row.masked ? ' | masked' : ''}</div>
														<div class="runtimeEnvDesc">{row.description}</div>
													</div>
													<div class="runtimeEnvEdit">
														<input class="runtimeEnvInput" type="text" value={runtimeEnvDraftByName[row.name] ?? ''} disabled={Boolean(runtimeEnvSaving[row.name])} on:input={(event) => { const next = String((event.currentTarget as HTMLInputElement).value ?? ''); runtimeEnvDraftByName = { ...runtimeEnvDraftByName, [row.name]: next }; }} placeholder={row.masked ? 'masked value' : (row.defaultValue ?? '')} />
														<div class="runtimeEnvButtons">
															<button class="tabBtn" on:click={() => void applyRuntimeEnvVar(row.name)} disabled={Boolean(runtimeEnvSaving[row.name])}>Apply</button>
															<button class="tabBtn" on:click={() => void unsetRuntimeEnvVar(row.name)} disabled={Boolean(runtimeEnvSaving[row.name])}>Clear</button>
														</div>
													</div>
												</div>
											{/each}
										</div>
									{/if}
								</div>
							{:else}
								<div class="envPanelSummary">Environment variables are collapsed.</div>
							{/if}
						</div>
					</div>
				</div>
			</aside>
		{/if}
	</div>

<style>
	@import './styles/inspectorForm.css';

	:global(.edge path) {
		stroke: #2f3646;
		stroke-width: 2;
	}
	:global(.edge.edge-link-control path) {
		stroke: #43c9c2;
		stroke-dasharray: 6 5;
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
	:global(.edge.edge-schema-error path) {
		stroke: #ff6b6b;
	}
	:global(.edge.edge-schema-warning path) {
		stroke: #ffcc66;
	}
	@keyframes dashmove {
		to {
			stroke-dashoffset: -28;
		}
	}

	.runMonitorLegend {
		font-size: 12px;
		opacity: 0.78;
		margin-bottom: 6px;
	}

	.layout {
		display: grid;
		grid-template-columns: 1fr 460px;
		height: 100vh;
	}

	.runMonitorSlideout {
		min-width: 0;
		height: 100vh;
		border-left: 1px solid var(--color-control-border, #222);
		background: var(--color-control-bg, #0b0c10);
		color: var(--color-control-text, #e6e6e6);
		position: relative;
		display: flex;
		flex-direction: column;
	}

	.runMonitorResizeHandle {
		position: absolute;
		left: 0;
		top: 0;
		bottom: 0;
		width: 8px;
		padding: 0;
		border: 0;
		background: transparent;
		cursor: col-resize;
		z-index: 3;
	}

	.runMonitorResizeHandle::after {
		content: '';
		position: absolute;
		left: 3px;
		top: 10px;
		bottom: 10px;
		width: 2px;
		border-radius: 999px;
		background: var(--color-control-border, #2b3448);
	}

	.runMonitorResizeHandle:hover::after,
	.runMonitorResizeHandle:focus::after {
		background: var(--color-control-border-focus, #3f5c93);
	}

	.runMonitorResizeHandle:focus {
		outline: none;
	}

	.runMonitorPanel {
		min-height: 0;
		height: 100%;
		display: flex;
		flex-direction: column;
		padding: 10px 10px 12px 14px;
		overflow: hidden;
	}

	.runMonitorSections {
		min-height: 0;
		height: 100%;
		display: flex;
		flex-direction: column;
		gap: 0;
	}

	.runMonitorSectionPane {
		min-height: 96px;
		min-width: 0;
		overflow: hidden;
		display: flex;
		flex-direction: column;
	}

	.runMonitorEnvSection.is-collapsed {
		min-height: 0;
		overflow: visible;
	}

	.runMonitorMonitorBody {
		min-height: 0;
		flex: 1 1 auto;
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}

	.runMonitorEnvSection {
		border-top: 1px solid var(--color-control-border, #222c3f);
		padding-top: 8px;
	}

	.runMonitorInnerSplitter {
		display: block;
		width: 100%;
		height: 4px;
		padding: 0;
		border: 0;
		border-radius: 999px;
		background: var(--color-control-border, #283044);
		cursor: row-resize;
		flex: 0 0 auto;
		touch-action: none;
		user-select: none;
		margin: 6px 0;
	}

	.runMonitorInnerSplitter:hover,
	.runMonitorInnerSplitter:focus {
		background: var(--color-control-border-focus, #3c4d70);
		outline: none;
	}

	.runMonitorPanelHead {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
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
		display: inline-flex;
		align-items: center;
		gap: 8px;
	}

	.toastAction {
		padding: 3px 8px;
		border-radius: 8px;
		font-size: 11px;
		border: 1px solid #3b82f6;
		background: #133061;
		color: #dbeafe;
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

	.portTypeLegend {
		position: absolute;
		z-index: 8;
		padding: 8px 10px;
		border-radius: 10px;
		border: 1px solid #283044;
		background: rgba(11, 12, 16, 0.94);
		color: #e6e6e6;
		box-shadow: 0 8px 18px rgba(0, 0, 0, 0.34);
		min-width: 178px;
	}

	.portTypeLegend.is-minimized {
		min-width: 130px;
		padding-bottom: 6px;
	}

	.portTypeLegendHead {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
		margin-bottom: 6px;
		font-size: 12px;
		cursor: grab;
		user-select: none;
	}

	.portTypeLegendHead:active {
		cursor: grabbing;
	}

	.portTypeLegendClose {
		padding: 2px 6px;
		border-radius: 6px;
		border: 1px solid #2b3854;
		background: #10172a;
		color: #dbeafe;
		font-size: 11px;
		line-height: 1;
	}

	.portTypeLegendRow {
		display: flex;
		align-items: center;
		gap: 8px;
		font-size: 12px;
		line-height: 1.2;
	}

	.portTypeLegendRow + .portTypeLegendRow {
		margin-top: 4px;
	}

	.portTypeDot {
		width: 10px;
		height: 10px;
		border-radius: 999px;
		border: 1px solid rgba(255, 255, 255, 0.28);
		display: inline-block;
	}

	.portTypeDot-work {
		background: #4b8cff;
	}

	.portTypeDot-param {
		background: #d8ac3f;
	}

	.portTypeDot-control {
		background: #2fbf71;
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

	.graphStatus-canceled {
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

	.saveConsistencyModal {
		width: min(760px, calc(100% - 20px));
		color: #f4f8ff;
		max-height: min(80vh, 760px);
		background: #0a1324;
		border-color: #3a4e78;
	}

	.saveConsistencyModal .componentSaveApplyBody {
		max-height: calc(min(80vh, 760px) - 130px);
		overflow: auto;
		padding-right: 6px;
		gap: 10px;
	}

	.saveConsistencyCounts {
		display: grid;
		gap: 4px;
		padding: 10px;
		border: 1px solid #3b4f78;
		border-radius: 8px;
		background: #111e36;
		color: #edf4ff;
		font-weight: 600;
	}

	.saveConsistencyError {
		color: #ffd4d4;
		font-weight: 700;
		line-height: 1.35;
		background: rgba(239, 68, 68, 0.2);
		border: 1px solid rgba(248, 113, 113, 0.5);
		border-radius: 8px;
		padding: 10px;
	}

	.saveConsistencyList {
		display: grid;
		gap: 4px;
		border: 1px solid #3a4e78;
		background: #0f1b31;
		border-radius: 8px;
		padding: 10px;
	}

	.saveConsistencyListTitle {
		font-weight: 700;
		color: #e7f0ff;
	}

	.saveConsistencyListBody {
		margin: 0;
		padding-left: 18px;
		color: #eff5ff;
		line-height: 1.4;
		display: grid;
		gap: 6px;
	}

	.saveConsistencyListBody li {
		display: flex;
		flex-direction: column;
		gap: 2px;
	}

	.saveConsistencyEntryId {
		font-family: ui-monospace, Menlo, Consolas, monospace;
		font-size: 11px;
		color: #aecdff;
		overflow-wrap: anywhere;
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
		position: relative;

		display: flex;
		flex-direction: column;
		height: 100vh;
		gap: 8px;
	}

	.inspectorResizeHandle {
		position: absolute;
		left: 0;
		top: 0;
		bottom: 0;
		width: 8px;
		padding: 0;
		border: 0;
		background: transparent;
		cursor: col-resize;
		z-index: 4;
	}

	.inspectorResizeHandle::after {
		content: '';
		position: absolute;
		left: 3px;
		top: 10px;
		bottom: 10px;
		width: 2px;
		border-radius: 999px;
		background: #2b3448;
	}

	.inspectorResizeHandle:hover::after,
	.inspectorResizeHandle:focus::after {
		background: #3f5c93;
	}

	.inspectorResizeHandle:focus {
		outline: none;
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
		opacity: 1;
		font-weight: 600;
	}

	.sectionToggleIcon {
		font-size: 11px;
		opacity: 0.85;
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
		color: var(--color-control-text, #e6e6e6);
	}

	.monitorToolbar {
		margin-top: 8px;
		display: grid;
		grid-template-columns: 1fr 1fr;
		gap: 8px;
	}

	.monitorField {
		display: grid;
		gap: 4px;
		font-size: 11px;
	}

	.monitorField-inline {
		display: flex;
		align-items: center;
		gap: 8px;
		padding-top: 14px;
	}

	.monitorField-inline input[type='checkbox'] {
		width: 14px;
		height: 14px;
	}

	.monitorField select {
		width: 100%;
		padding: 4px 6px;
		border-radius: 8px;
		border: 1px solid var(--color-control-border, #2a3655);
		background: var(--color-control-bg, #0c1220);
		color: var(--color-control-text, #dbe7ff);
		font-size: 12px;
	}

	.runMonitorNodeTable,
	.runMonitorEdgeTable {
		margin-top: 8px;
		display: grid;
		gap: 4px;
		overflow: auto;
		padding-right: 2px;
	}

	.runMonitorTablesSplit {
		min-height: 180px;
		flex: 1 1 auto;
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}

	.runMonitorTablesPane {
		min-height: 96px;
		overflow: hidden;
		display: flex;
		flex-direction: column;
	}

	.runMonitorTablesPane .runMonitorNodeTable,
	.runMonitorTablesPane .runMonitorEdgeTable {
		margin-top: 8px;
		max-height: none;
		height: 100%;
		flex: 1 1 auto;
	}

	.runMonitorTablesPane .envProfileEmpty {
		margin-top: 8px;
	}

	.runMonitorHistoryTable {
		margin-top: 8px;
		display: grid;
		gap: 4px;
		padding-right: 2px;
		overflow: visible;
	}

	.runMonitorNodeHead,
	.runMonitorNodeRow {
		display: grid;
		gap: 6px;
		align-items: center;
		font-size: 12px;
	}

	.runMonitorNodeTable .runMonitorNodeHead,
	.runMonitorNodeTable .runMonitorNodeRow,
	.runMonitorHistoryTable .runMonitorNodeHead,
	.runMonitorHistoryTable .runMonitorNodeRow {
		grid-template-columns: 1.35fr 0.8fr 0.6fr 0.6fr 1.4fr;
	}

	.runMonitorEdgeTable .runMonitorNodeHead,
	.runMonitorEdgeTable .runMonitorNodeRow {
		grid-template-columns: 1.05fr 0.7fr 1fr 1fr 0.5fr 0.6fr;
	}

	.runMonitorAdaptiveTimelineHead,
	.runMonitorAdaptiveTimelineRow {
		grid-template-columns: 1.2fr 0.9fr 0.55fr 1.25fr 1.15fr 1.2fr 1.4fr;
	}

	.runMonitorNodeHead {
		opacity: 1;
		text-transform: lowercase;
		color: var(--color-control-text-muted, #9aa4b2);
		position: sticky;
		top: 0;
		z-index: 2;
		background-color: var(--color-panel-bg, #081327);
		box-shadow: 0 1px 0 rgba(99, 147, 255, 0.18);
		padding: 4px 0;
	}

	.runMonitorNodeRow {
		border: 1px solid var(--color-control-border, #1c2335);
		border-radius: 8px;
		padding: 6px;
		background: var(--color-control-option-bg, #0c1220);
		color: var(--color-control-option-text, #dbe7ff);
		text-align: left;
	}

	.runMonitorNodeRow:hover {
		border-color: var(--color-control-border-focus, #35548c);
	}

	.runMonitorNodeRow-highlighted {
		border-color: rgba(99, 160, 255, 0.78);
		box-shadow: inset 0 0 0 1px rgba(99, 160, 255, 0.32);
	}

	.adaptiveSeverity {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		min-width: 36px;
		padding: 1px 6px;
		border-radius: 999px;
		font-size: 11px;
		font-weight: 700;
		border: 1px solid transparent;
	}

	.adaptiveSeverity-low {
		background: rgba(47, 191, 113, 0.15);
		border-color: rgba(47, 191, 113, 0.4);
		color: #7de2a9;
	}

	.adaptiveSeverity-medium {
		background: rgba(242, 204, 96, 0.14);
		border-color: rgba(242, 204, 96, 0.42);
		color: #f4d98d;
	}

	.adaptiveSeverity-high {
		background: rgba(239, 68, 68, 0.16);
		border-color: rgba(248, 113, 113, 0.42);
		color: #ffc1c1;
	}

	.adaptiveComponentBars {
		display: grid;
		gap: 4px;
		margin-top: 4px;
	}

	.adaptiveComponentBarRow {
		display: grid;
		grid-template-columns: minmax(160px, 1fr) minmax(90px, 2fr) auto;
		align-items: center;
		gap: 8px;
		font-size: 11px;
	}

	.adaptiveComponentLabel {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.adaptiveComponentTrack {
		height: 7px;
		border-radius: 999px;
		background: rgba(108, 128, 160, 0.22);
		overflow: hidden;
	}

	.adaptiveComponentFill {
		height: 100%;
		border-radius: 999px;
	}

	.adaptiveComponentFill-up {
		background: linear-gradient(90deg, rgba(92, 184, 255, 0.95), rgba(114, 223, 170, 0.95));
	}

	.adaptiveComponentFill-down {
		background: linear-gradient(90deg, rgba(252, 165, 165, 0.92), rgba(248, 113, 113, 0.92));
	}

	.adaptiveComponentDelta {
		min-width: 54px;
		text-align: right;
	}

	.runMonitorSparklineCard {
		display: grid;
		gap: 6px;
		border: 1px solid var(--color-control-border, #1c2335);
		border-radius: 8px;
		background: var(--color-control-option-bg, #0c1220);
		color: var(--color-control-option-text, #dbe7ff);
		padding: 8px;
		margin-bottom: 6px;
	}

	.runMonitorSparklineHead {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
		font-size: 12px;
	}

	.runMonitorSparkline {
		width: 100%;
		height: 88px;
		display: block;
		background: rgba(8, 19, 39, 0.55);
		border-radius: 6px;
	}

	.runMonitorSparklineBase {
		fill: none;
		stroke: rgba(108, 128, 160, 0.35);
		stroke-width: 1;
	}

	.runMonitorSparklineBaseline {
		stroke-width: 1.1;
		stroke-dasharray: 4 3;
	}

	.runMonitorSparklineBaseline-first {
		stroke: rgba(107, 209, 150, 0.85);
	}

	.runMonitorSparklineBaseline-mean {
		stroke: rgba(245, 210, 120, 0.85);
	}

	.runMonitorSparklinePath {
		fill: none;
		stroke: #63a0ff;
		stroke-width: 2;
	}

	.runMonitorSparklineHoverGuide {
		stroke: rgba(190, 218, 255, 0.85);
		stroke-width: 1;
		stroke-dasharray: 2 3;
	}

	.runMonitorSparklineHoverPoint {
		fill: #9dcbff;
		stroke: #0b1323;
		stroke-width: 1.1;
	}

	.runMonitorSparklineTooltip {
		font-size: 11px;
		opacity: 0.9;
		color: var(--color-control-text, #dbe7ff);
	}

	.runMonitorSparklineFoot {
		font-size: 11px;
		opacity: 0.88;
	}

	.runMonitorRunSummary {
		font-size: 11px;
		line-height: 1.35;
		opacity: 0.95;
		word-break: break-word;
	}

	.runMonitorJsonDetail {
		margin: 0;
		padding: 8px;
		border: 1px solid var(--color-control-border, #1c2335);
		border-radius: 8px;
		background: var(--color-control-option-bg, #0c1220);
		color: var(--color-control-option-text, #dbe7ff);
		font-size: 11px;
		line-height: 1.35;
		max-height: 220px;
		overflow: auto;
	}

	.monitorField input[type='number'],
	.monitorField input[type='text'] {
		border: 1px solid var(--color-control-border, #2a3655);
		background: var(--color-control-bg, #0b1323);
		color: var(--color-control-text, #dbe7ff);
		padding: 6px 8px;
		border-radius: 8px;
		font-size: 12px;
	}

	.runMonitorNodeName {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.runMonitorEdgeId,
	.runMonitorEdgePort {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
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

	.runtimeEnvPanel {
		margin-top: 10px;
		display: flex;
		flex-direction: column;
		gap: 8px;
		min-height: 0;
		flex: 1 1 auto;
		overflow: hidden;
	}

	.runtimeEnvHead {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
		font-size: 12px;
		color: var(--color-control-text, #e6e6e6);
	}

	.runtimeEnvActions {
		display: inline-flex;
		align-items: center;
		gap: 8px;
	}

	.runtimeEnvToggle {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		font-size: 11px;
		opacity: 0.85;
		color: var(--color-control-text, #e6e6e6);
	}

	.runtimeEnvTable {
		display: grid;
		gap: 6px;
		max-height: none;
		min-height: 0;
		flex: 1 1 auto;
		overflow: auto;
		padding-right: 2px;
	}

	.runtimeEnvRow {
		display: grid;
		grid-template-columns: 1.1fr 1.2fr;
		gap: 8px;
		border: 1px solid var(--color-control-border, #1c2335);
		border-radius: 8px;
		padding: 8px;
		background: var(--color-control-option-bg, #0c1220);
		color: var(--color-control-option-text, #dbe7ff);
	}

	.runtimeEnvMeta {
		min-width: 0;
	}

	.runtimeEnvName {
		font-size: 12px;
	}

	.runtimeEnvSub {
		margin-top: 3px;
		font-size: 11px;
		opacity: 0.8;
		color: var(--color-control-text, #e6e6e6);
	}

	.runtimeEnvDesc {
		margin-top: 4px;
		font-size: 11px;
		opacity: 0.86;
		color: var(--color-control-text, #e6e6e6);
	}

	.runtimeEnvEdit {
		display: grid;
		gap: 6px;
	}

	.runtimeEnvInput {
		width: 100%;
		padding: 6px 8px;
		border-radius: 8px;
		border: 1px solid var(--color-control-border, #2a3655);
		background: var(--color-control-bg, #0b1323);
		color: var(--color-control-text, #dbe7ff);
		font-size: 12px;
	}

	.runtimeEnvButtons {
		display: inline-flex;
		gap: 6px;
	}

	.runtimeEnvRestart {
		margin-left: 6px;
		font-size: 10px;
		padding: 1px 5px;
		border-radius: 999px;
		background: #5f4b1a;
		color: #f6d58a;
	}

	.card {
		border: 1px solid #1f2430;
		border-radius: 12px;
		padding: 12px;
		background: #0f1115;
	}

	.guidedCard {
		display: grid;
		gap: 8px;
	}

	.guidedHead {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
	}

	.guidedHeadActions {
		display: flex;
		align-items: center;
		gap: 6px;
	}

	.guidedCloseBtn {
		padding-inline: 7px;
	}

	.guidedRestoreRow {
		display: flex;
		justify-content: flex-start;
	}

	.guidedBody {
		display: grid;
		gap: 6px;
	}

	.guidedHintTitle {
		font-size: 13px;
		font-weight: 600;
	}

	.guidedHintDescription {
		font-size: 12px;
		opacity: 0.82;
	}

	.guidedActions {
		display: flex;
		align-items: center;
		gap: 8px;
		flex-wrap: wrap;
	}

	.guidedActions .primary {
		font-size: 12px;
	}

	.guidedInlineExample {
		display: flex;
		align-items: center;
		gap: 8px;
		font-size: 12px;
		opacity: 0.9;
		flex-wrap: wrap;
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
		border: 1px solid var(--color-control-border, #283044);
		background: var(--color-control-bg, #111522);
		color: var(--color-control-text, #e6e6e6);
		border-radius: 999px;
		display: inline-flex;
		align-items: center;
		line-height: 1.2;
	}

	.pill-freeze-per-run {
		opacity: 1;
		color: #fff1c2;
		border-color: #f59e0b;
		background: rgba(245, 158, 11, 0.2);
	}

	.pill-freeze-sticky {
		opacity: 1;
		color: #cfe3ff;
		border-color: #3b82f6;
		background: rgba(59, 130, 246, 0.2);
	}

	.pinBtn {
		cursor: pointer;
		background: transparent;
		color: inherit;
	}

	.pinBtn.active {
		opacity: 1;
		color: #fff1c2;
		border-color: #f59e0b;
		background: rgba(245, 158, 11, 0.2);
	}

	.pinBtn.pinSticky.active {
		color: #cfe3ff;
		border-color: #3b82f6;
		background: rgba(59, 130, 246, 0.2);
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

	.runWarningSummary {
		margin: 8px 0;
		padding: 8px;
		border: 1px solid rgba(255, 204, 102, 0.35);
		border-radius: 8px;
		background: rgba(255, 204, 102, 0.08);
		font-size: 12px;
	}

	.runWarningSummaryHead {
		color: #ffcc66;
		font-weight: 600;
		margin-bottom: 4px;
	}

	.runWarningSummaryRow {
		display: flex;
		gap: 8px;
		flex-wrap: wrap;
		color: #dbe2f2;
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
		font-size: 12px; /* same scale as section labels */
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

