import type { Node } from '@xyflow/svelte';
import type { PipelineNodeData, PortType } from '$lib/flow/types';
import capsRaw from '../../../shared/port_capabilities.v1.json';
import { getBackendCapabilities } from '$lib/flow/client/capabilities';

type Direction = 'in' | 'out';

type NodeCapabilities = {
	in?: PortType[];
	out?: PortType[];
	toolInByProvider?: Record<string, PortType[]>;
	toolOutByProvider?: Record<string, PortType[]>;
};

let activeCaps = capsRaw as any;
let activeFlags: {
	STRICT_SCHEMA_EDGE_CHECKS: boolean;
	STRICT_SCHEMA_EDGE_CHECKS_V2: boolean;
	STRICT_COERCION_POLICY: boolean;
	GRAPH_PERSIST_DERIVED_PORTS_OMITTED: boolean;
} = {
	STRICT_SCHEMA_EDGE_CHECKS: true,
	STRICT_SCHEMA_EDGE_CHECKS_V2: true,
	STRICT_COERCION_POLICY: true,
	GRAPH_PERSIST_DERIVED_PORTS_OMITTED: false
};

function getNodesMap(): Record<string, any> {
	return ((activeCaps as any)?.nodes ?? {}) as Record<string, any>;
}

function getToolByProviderMap(): Record<string, any> {
	return (getNodesMap().tool?.byProvider ?? {}) as Record<string, any>;
}

function asPortTypes(values: unknown): PortType[] {
	if (!Array.isArray(values)) return [];
	return values
		.map((v) => String(v))
		.filter((v): v is PortType => ['table', 'json', 'text', 'binary', 'embeddings'].includes(v));
}

function buildNodeCapabilities(): Record<
	'llm' | 'transform' | 'source' | 'tool' | 'component',
	NodeCapabilities
> {
	const nodes = getNodesMap();
	const toolByProvider = getToolByProviderMap();
	return {
		llm: { in: asPortTypes(nodes.llm?.in), out: asPortTypes(nodes.llm?.out) },
		transform: { in: asPortTypes(nodes.transform?.in), out: asPortTypes(nodes.transform?.out) },
		source: { in: asPortTypes(nodes.source?.in), out: asPortTypes(nodes.source?.out) },
		tool: {
			in: asPortTypes(nodes.tool?.in),
			out: asPortTypes(nodes.tool?.out),
			toolInByProvider: Object.fromEntries(
				Object.entries(toolByProvider).map(([provider, value]) => [
					provider,
					asPortTypes((value as any)?.in)
				])
			),
			toolOutByProvider: Object.fromEntries(
				Object.entries(toolByProvider).map(([provider, value]) => [
					provider,
					asPortTypes((value as any)?.out)
				])
			)
		},
		component: { in: asPortTypes(nodes.component?.in), out: asPortTypes(nodes.component?.out) }
	};
}

export let NODE_CAPABILITIES: Record<
	'llm' | 'transform' | 'source' | 'tool' | 'component',
	NodeCapabilities
> = buildNodeCapabilities();

export function getStrictSchemaFeatureFlags(): {
	STRICT_SCHEMA_EDGE_CHECKS: boolean;
	STRICT_SCHEMA_EDGE_CHECKS_V2: boolean;
	STRICT_COERCION_POLICY: boolean;
} {
	return { ...activeFlags };
}

export function getGraphPersistenceFeatureFlags(): {
	GRAPH_PERSIST_DERIVED_PORTS_OMITTED: boolean;
} {
	return {
		GRAPH_PERSIST_DERIVED_PORTS_OMITTED: Boolean(activeFlags.GRAPH_PERSIST_DERIVED_PORTS_OMITTED)
	};
}

export function __setStrictSchemaFeatureFlagsForTest(flags: {
	STRICT_SCHEMA_EDGE_CHECKS?: boolean;
	STRICT_SCHEMA_EDGE_CHECKS_V2?: boolean;
	STRICT_COERCION_POLICY?: boolean;
	GRAPH_PERSIST_DERIVED_PORTS_OMITTED?: boolean;
}): void {
	activeFlags = {
		...activeFlags,
		...flags
	};
}

export function __setGraphPersistenceFeatureFlagsForTest(flags: {
	GRAPH_PERSIST_DERIVED_PORTS_OMITTED?: boolean;
}): void {
	activeFlags = {
		...activeFlags,
		...flags
	};
}

export async function refreshPortCapabilitiesFromBackend(): Promise<void> {
	try {
		const response = await getBackendCapabilities();
		const caps = (response?.capabilities ?? null) as any;
		if (!caps || typeof caps !== 'object' || !caps.nodes || typeof caps.nodes !== 'object') return;
		activeCaps = caps;
		activeFlags = {
			STRICT_SCHEMA_EDGE_CHECKS: Boolean(response?.featureFlags?.STRICT_SCHEMA_EDGE_CHECKS ?? true),
			STRICT_SCHEMA_EDGE_CHECKS_V2: Boolean(response?.featureFlags?.STRICT_SCHEMA_EDGE_CHECKS_V2 ?? true),
			STRICT_COERCION_POLICY: Boolean(response?.featureFlags?.STRICT_COERCION_POLICY ?? true),
			GRAPH_PERSIST_DERIVED_PORTS_OMITTED: Boolean(
				response?.featureFlags?.GRAPH_PERSIST_DERIVED_PORTS_OMITTED ?? false
			)
		};
		NODE_CAPABILITIES = buildNodeCapabilities();
	} catch {
		// Fall back to bundled shared capabilities in offline/dev edge-cases.
	}
}

export function getAllowedPortsForNode(
	node: Node<PipelineNodeData> | null | undefined,
	direction: Direction
): PortType[] {
	if (!node) return [];
	const kind = node.data.kind as keyof typeof NODE_CAPABILITIES;
	const cap = NODE_CAPABILITIES[kind];
	if (!cap) return [];
	if (kind === 'tool') {
		const provider = String((node.data as any)?.params?.provider ?? 'mcp');
		if (direction === 'in') {
			return cap.toolInByProvider?.[provider] ?? cap.in ?? [];
		}
		return cap.toolOutByProvider?.[provider] ?? cap.out ?? [];
	}
	return (direction === 'in' ? cap.in : cap.out) ?? [];
}

export function isPortAllowedForNode(
	node: Node<PipelineNodeData> | null | undefined,
	direction: Direction,
	port: PortType | null | undefined
): boolean {
	if (port == null) return true;
	return getAllowedPortsForNode(node, direction).includes(port);
}
