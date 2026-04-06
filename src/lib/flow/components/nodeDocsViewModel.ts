import type { Edge } from '@xyflow/svelte';
import {
	sanitizeNodeDocGeneratedExplanation,
	type NodeDocGeneratedExplanation,
	type NodeDocOverride,
	type NodeDocPlaneSection,
	type NodeDocPortRef,
	type NodeDocV1
} from '$lib/flow/schema/nodeDocs';
import type { PipelineEdgeData, PipelineNodeData } from '$lib/flow/types';
import type { GraphState } from '$lib/flow/store/graphStore';
import { resolveNodeHandles, type NodeHandleDef } from '$lib/flow/nodes/portHandles';
import { resolveNodeDocBase } from './nodeDocsResolver';
import { buildNodeDocLlmContext } from './nodeDocLlmContext';

export type NodeDocResolved = NodeDocV1 & {
	source: 'base' | 'runtime_only';
	disabled: boolean;
	overrideApplied: boolean;
	generated: NodeDocGeneratedExplanation | null;
	runtime: {
		blockedReasonCode?: string;
		pendingInputCount: number;
		inflight: number;
		readyWork: boolean;
	};
};

export type NodeDocResolver = (state: GraphState, nodeId: string) => NodeDocResolved | null;

type DocPlane = 'data' | 'control' | 'param';
type PortPlane = 'work' | 'param' | 'control';

function asPlane(planeRaw: unknown): PortPlane {
	const plane = String(planeRaw ?? '')
		.trim()
		.toLowerCase();
	if (plane === 'param' || plane === 'control') return plane;
	return 'work';
}

function toDocPlane(plane: PortPlane): DocPlane {
	if (plane === 'param') return 'param';
	if (plane === 'control') return 'control';
	return 'data';
}

function declarationByHandle(
	data: PipelineNodeData | null | undefined,
	direction: 'in' | 'out',
	handleId: string
): Record<string, unknown> | null {
	const declarations =
		data && typeof (data as any).portDeclarations === 'object'
			? ((data as any).portDeclarations as Record<string, unknown>)
			: null;
	const byDir =
		declarations && declarations[direction] && typeof declarations[direction] === 'object'
			? (declarations[direction] as Record<string, unknown>)
			: null;
	if (!byDir) return null;
	const direct = byDir[handleId];
	if (direct && typeof direct === 'object') return direct as Record<string, unknown>;
	const fallback = byDir.default;
	if (fallback && typeof fallback === 'object') return fallback as Record<string, unknown>;
	return null;
}

function toNodeDocPortRef(
	data: PipelineNodeData,
	direction: 'in' | 'out',
	handle: NodeHandleDef
): NodeDocPortRef {
	const id = String(handle?.id ?? '').trim() || (direction === 'in' ? 'in' : 'out');
	const decl = declarationByHandle(data, direction, id);
	const plane = asPlane(handle?.plane ?? (decl as any)?.plane ?? (decl as any)?.affinity);
	const cardinalityRaw = String((decl as any)?.cardinality ?? '').trim().toLowerCase();
	const cardinality = cardinalityRaw === 'one' || cardinalityRaw === 'many' ? cardinalityRaw : undefined;
	const required = typeof (decl as any)?.required === 'boolean' ? Boolean((decl as any).required) : undefined;
	const itemModeRaw = String((decl as any)?.item_mode ?? '').trim().toLowerCase();
	const itemMode =
		itemModeRaw === 'artifact' || itemModeRaw === 'json_items' || itemModeRaw === 'table_rows'
			? itemModeRaw
			: undefined;
	return {
		handle: id,
		plane: toDocPlane(plane),
		direction,
		cardinality: cardinality as 'one' | 'many' | undefined,
		required,
		item_mode: itemMode as 'artifact' | 'json_items' | 'table_rows' | undefined
	};
}

function dedupePorts(ports: NodeDocPortRef[]): NodeDocPortRef[] {
	const map = new Map<string, NodeDocPortRef>();
	for (const port of ports) {
		const key = `${port.direction}:${port.plane}:${port.handle}`;
		if (!map.has(key)) map.set(key, port);
	}
	return Array.from(map.values());
}

function dedupeNotes(notes: string[]): string[] {
	const seen = new Set<string>();
	const out: string[] = [];
	for (const noteRaw of notes) {
		const note = String(noteRaw ?? '').trim();
		if (!note || seen.has(note)) continue;
		seen.add(note);
		out.push(note);
	}
	return out;
}

function nodeSubtype(data: PipelineNodeData): string {
	const kind = String((data as any)?.kind ?? '').trim().toLowerCase();
	if (kind === 'source') return String((data as any)?.sourceKind ?? '').trim().toLowerCase();
	if (kind === 'transform') return String((data as any)?.transformKind ?? '').trim().toLowerCase();
	if (kind === 'model' || kind === 'llm') {
		return String((data as any)?.llmKind ?? (data as any)?.params?.provider ?? '').trim().toLowerCase();
	}
	if (kind === 'tool') return String((data as any)?.toolKind ?? (data as any)?.params?.provider ?? '').trim().toLowerCase();
	if (kind === 'component') return String((data as any)?.componentKind ?? '').trim().toLowerCase();
	return '';
}

function resolveRuntimeHandles(
	state: GraphState,
	nodeId: string,
	data: PipelineNodeData
): { inPorts: NodeDocPortRef[]; outPorts: NodeDocPortRef[] } {
	const connectedInputs: NodeHandleDef[] = [];
	const connectedOutputs: NodeHandleDef[] = [];
	for (const edge of state.edges ?? []) {
		const edgeAny = edge as Edge<PipelineEdgeData>;
		const mode = asPlane((edgeAny as any)?.data?.mode);
		if (String(edgeAny.target ?? '') === nodeId) {
			const id = String((edgeAny as any).targetHandle ?? 'in').trim() || 'in';
			connectedInputs.push({ id, plane: mode });
		}
		if (String(edgeAny.source ?? '') === nodeId) {
			const id = String((edgeAny as any).sourceHandle ?? 'out').trim() || 'out';
			connectedOutputs.push({ id, plane: mode });
		}
	}
	const inputHandles = resolveNodeHandles(data, 'in', connectedInputs, 'present');
	const outputHandles = resolveNodeHandles(data, 'out', connectedOutputs, 'present');
	return {
		inPorts: inputHandles.map((handle) => toNodeDocPortRef(data, 'in', handle)),
		outPorts: outputHandles.map((handle) => toNodeDocPortRef(data, 'out', handle))
	};
}

function runtimeControlNotes(state: GraphState, nodeId: string): {
	blockedReasonCode?: string;
	pendingInputCount: number;
	inflight: number;
	readyWork: boolean;
	notes: string[];
} {
	const schedulerRows = Array.isArray((state as any)?.queueRuntime?.schedulerSnapshot?.perNode)
		? (((state as any).queueRuntime.schedulerSnapshot.perNode as unknown[]) ?? [])
		: [];
	const row =
		schedulerRows.find((entry) => String((entry as any)?.nodeId ?? '') === String(nodeId)) ?? ({} as Record<string, unknown>);
	const pendingInputCount = Math.max(0, Number((row as any)?.pendingInputCount ?? 0));
	const inflight = Math.max(0, Number((row as any)?.inflight ?? 0));
	const readyWork = Boolean((row as any)?.readyWork ?? false);
	const blockedReasonCode = String((row as any)?.lastBlockedReasonCode ?? '').trim() || undefined;
	const notes = [
		`pending_input=${pendingInputCount}`,
		`inflight=${inflight}`,
		`ready_work=${String(readyWork)}`
	];
	if (blockedReasonCode) notes.push(`blocked_reason=${blockedReasonCode}`);
	return { blockedReasonCode, pendingInputCount, inflight, readyWork, notes };
}

function runtimeOnlyDoc(data: PipelineNodeData): NodeDocV1 {
	const nodeKindRaw = String((data as any)?.kind ?? '').trim().toLowerCase();
	const nodeKind = nodeKindRaw === 'llm' ? 'model' : nodeKindRaw;
	return {
		schema_version: 1,
		node_kind:
			nodeKind === 'source' || nodeKind === 'transform' || nodeKind === 'model' || nodeKind === 'tool' || nodeKind === 'component'
				? (nodeKind as NodeDocV1['node_kind'])
				: 'tool',
		subtype: '*',
		title: `${String((data as any)?.label ?? 'Node').trim() || 'Node'} Docs`,
		summary: 'Runtime-derived node documentation.',
		planes: {
			data: { title: 'Data Plane', summary: 'Runtime data-plane handles and contracts.' },
			control: { title: 'Control Plane', summary: 'Runtime scheduler/blocking context.' },
			param: { title: 'Param Plane', summary: 'Runtime parameter-plane handles and config context.' }
		},
		examples: [],
		see_also: []
	};
}

function mergeDocWithRuntime(base: NodeDocV1, runtime: {
	inPorts: NodeDocPortRef[];
	outPorts: NodeDocPortRef[];
	controlNotes: string[];
}): NodeDocV1 {
	const planeWithPorts = (section: NodeDocPlaneSection, docPlane: DocPlane): NodeDocPlaneSection => {
		const runtimePorts = [...runtime.inPorts, ...runtime.outPorts].filter((port) => port.plane === docPlane);
		const ports = dedupePorts([...(section.ports ?? []), ...runtimePorts]);
		const notes =
			docPlane === 'control'
				? dedupeNotes([...(section.notes ?? []), ...runtime.controlNotes])
				: dedupeNotes([...(section.notes ?? [])]);
		return { ...section, ports, notes };
	};
	return {
		...base,
		planes: {
			data: planeWithPorts(base.planes.data, 'data'),
			control: planeWithPorts(base.planes.control, 'control'),
			param: planeWithPorts(base.planes.param, 'param')
		}
	};
}

export function resolveNodeDocForState(state: GraphState, nodeIdRaw: string): NodeDocResolved | null {
	const nodeId = String(nodeIdRaw ?? '').trim();
	if (!nodeId) return null;
	const node = (state.nodes ?? []).find((entry) => String(entry?.id ?? '') === nodeId);
	if (!node) return null;
	const data = (node.data ?? null) as PipelineNodeData | null;
	if (!data) return null;
	const kind = String((data as any)?.kind ?? '').trim().toLowerCase();
	const subtype = nodeSubtype(data);
	const base = resolveNodeDocBase(kind, subtype);
	const runtimeHandles = resolveRuntimeHandles(state, nodeId, data);
	const controlRuntime = runtimeControlNotes(state, nodeId);
	const merged = mergeDocWithRuntime(base ?? runtimeOnlyDoc(data), {
		inPorts: runtimeHandles.inPorts,
		outPorts: runtimeHandles.outPorts,
		controlNotes: controlRuntime.notes
	});
	const override = ((data as any)?.meta?.nodeDoc ?? {}) as NodeDocOverride;
	const generated = sanitizeNodeDocGeneratedExplanation((override as any)?.generated ?? null);
	const disabled = Boolean(override?.disabled ?? false);
	const summary = String(override?.summary ?? '').trim() || merged.summary;
	const overrideNotes = Array.isArray(override?.notes)
		? (override.notes ?? []).map((item) => String(item ?? '').trim()).filter((item) => item.length > 0)
		: [];
	const next: NodeDocResolved = {
		...merged,
		summary,
		source: base ? 'base' : 'runtime_only',
		disabled,
		overrideApplied: Boolean(String(override?.summary ?? '').trim().length > 0 || overrideNotes.length > 0),
		generated,
		runtime: {
			blockedReasonCode: controlRuntime.blockedReasonCode,
			pendingInputCount: controlRuntime.pendingInputCount,
			inflight: controlRuntime.inflight,
			readyWork: controlRuntime.readyWork
		}
	};
	if (overrideNotes.length > 0) {
		next.planes.control.notes = dedupeNotes([...(next.planes.control.notes ?? []), ...overrideNotes]);
	}
	const context = buildNodeDocLlmContext(state, nodeId);
	if (context) {
		const contextNotes = Object.entries(context.settings)
			.slice(0, 5)
			.map(([key, value]) => `${key}=${value}`);
		if (contextNotes.length > 0) {
			next.planes.param.notes = dedupeNotes([...(next.planes.param.notes ?? []), ...contextNotes]);
		}
		const dataSnippet = [
			`inputs=${context.planes.data_inputs.join(',') || '(none)'}`,
			`outputs=${context.planes.data_outputs.join(',') || '(none)'}`
		];
		next.planes.data.notes = dedupeNotes([...(next.planes.data.notes ?? []), ...dataSnippet]);
	}
	return next;
}

export function buildNodeDocDependencySignature(state: GraphState, nodeIdRaw: string): string {
	const nodeId = String(nodeIdRaw ?? '').trim();
	if (!nodeId) return 'missing-node';
	const connectedEdges = (state.edges ?? [])
		.filter((edge) => String(edge?.source ?? '') === nodeId || String(edge?.target ?? '') === nodeId)
		.map((edge) => {
			const source = String((edge as any)?.source ?? '');
			const target = String((edge as any)?.target ?? '');
			const sourceHandle = String((edge as any)?.sourceHandle ?? 'out');
			const targetHandle = String((edge as any)?.targetHandle ?? 'in');
			const mode = String(((edge as any)?.data?.mode ?? 'work')).trim().toLowerCase();
			return `${source}:${sourceHandle}->${target}:${targetHandle}[${mode}]`;
		})
		.sort()
		.join('|');
	const schedulerRows = Array.isArray((state as any)?.queueRuntime?.schedulerSnapshot?.perNode)
		? (((state as any).queueRuntime.schedulerSnapshot.perNode as unknown[]) ?? [])
		: [];
	const row =
		schedulerRows.find((entry) => String((entry as any)?.nodeId ?? '') === nodeId) ?? ({} as Record<string, unknown>);
	const pending = Number((row as any)?.pendingInputCount ?? 0);
	const inflight = Number((row as any)?.inflight ?? 0);
	const readyWork = Boolean((row as any)?.readyWork ?? false);
	const blockedReasonCode = String((row as any)?.lastBlockedReasonCode ?? '').trim();
	const node = (state.nodes ?? []).find((entry) => String(entry?.id ?? '') === nodeId);
	const override = (node as any)?.data?.meta?.nodeDoc ?? {};
	const overrideSig = JSON.stringify({
		summary: String((override as any)?.summary ?? ''),
		notes: Array.isArray((override as any)?.notes) ? (override as any)?.notes : [],
		disabled: Boolean((override as any)?.disabled ?? false),
		generated_signature_key: String((override as any)?.generated?.signature_key ?? ''),
		generated_summary: String((override as any)?.generated?.summary ?? '')
	});
	return `${connectedEdges}::pending=${pending}::inflight=${inflight}::ready=${String(readyWork)}::blocked=${blockedReasonCode}::override=${overrideSig}`;
}

export function createMemoizedNodeDocResolver(): NodeDocResolver {
	const byNodeId = new Map<string, { signature: string; resolved: NodeDocResolved | null }>();
	return (state: GraphState, nodeIdRaw: string): NodeDocResolved | null => {
		const nodeId = String(nodeIdRaw ?? '').trim();
		if (!nodeId) return null;
		const signature = buildNodeDocDependencySignature(state, nodeId);
		const previous = byNodeId.get(nodeId);
		if (previous && previous.signature === signature) return previous.resolved;
		const resolved = resolveNodeDocForState(state, nodeId);
		byNodeId.set(nodeId, { signature, resolved });
		return resolved;
	};
}
