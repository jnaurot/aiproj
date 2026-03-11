import type { Node } from '@xyflow/svelte';
import type { PipelineNodeData, PayloadType } from '$lib/flow/types';
import capsRaw from '../../../shared/schema_capabilities.v1.json';
import { getBackendCapabilities } from '$lib/flow/client/capabilities';

type Direction = 'in' | 'out';

type NodeCapabilities = {
	in?: PayloadType[];
	out?: PayloadType[];
	toolInByProvider?: Record<string, PayloadType[]>;
	toolOutByProvider?: Record<string, PayloadType[]>;
};

let activeCaps = capsRaw as any;
let activeFlags: {
	STRICT_SCHEMA_EDGE_CHECKS: boolean;
	STRICT_SCHEMA_EDGE_CHECKS_V2: boolean;
	STRICT_COERCION_POLICY: boolean;
} = {
	STRICT_SCHEMA_EDGE_CHECKS: true,
	STRICT_SCHEMA_EDGE_CHECKS_V2: true,
	STRICT_COERCION_POLICY: true
};

function getNodesMap(): Record<string, any> {
	return ((activeCaps as any)?.nodes ?? {}) as Record<string, any>;
}

function getToolByProviderMap(): Record<string, any> {
	return (getNodesMap().tool?.byProvider ?? {}) as Record<string, any>;
}

function asPayloadTypes(values: unknown): PayloadType[] {
	if (!Array.isArray(values)) return [];
	return values
		.map((v) => String(v))
		.filter((v): v is PayloadType => ['table', 'json', 'text', 'binary', 'embeddings'].includes(v));
}

function buildNodeCapabilities(): Record<
	'llm' | 'transform' | 'source' | 'tool' | 'component',
	NodeCapabilities
> {
	const nodes = getNodesMap();
	const toolByProvider = getToolByProviderMap();
	return {
		llm: { in: asPayloadTypes(nodes.llm?.in), out: asPayloadTypes(nodes.llm?.out) },
		transform: { in: asPayloadTypes(nodes.transform?.in), out: asPayloadTypes(nodes.transform?.out) },
		source: { in: asPayloadTypes(nodes.source?.in), out: asPayloadTypes(nodes.source?.out) },
		tool: {
			in: asPayloadTypes(nodes.tool?.in),
			out: asPayloadTypes(nodes.tool?.out),
			toolInByProvider: Object.fromEntries(
				Object.entries(toolByProvider).map(([provider, value]) => [
					provider,
					asPayloadTypes((value as any)?.in)
				])
			),
			toolOutByProvider: Object.fromEntries(
				Object.entries(toolByProvider).map(([provider, value]) => [
					provider,
					asPayloadTypes((value as any)?.out)
				])
			)
		},
		component: { in: asPayloadTypes(nodes.component?.in), out: asPayloadTypes(nodes.component?.out) }
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

export function __setStrictSchemaFeatureFlagsForTest(flags: {
	STRICT_SCHEMA_EDGE_CHECKS?: boolean;
	STRICT_SCHEMA_EDGE_CHECKS_V2?: boolean;
	STRICT_COERCION_POLICY?: boolean;
}): void {
	activeFlags = {
		...activeFlags,
		...flags
	};
}

export async function refreshSchemaCapabilitiesFromBackend(): Promise<void> {
	try {
		const response = await getBackendCapabilities();
		const caps = (response?.capabilities ?? null) as any;
		if (!caps || typeof caps !== 'object' || !caps.nodes || typeof caps.nodes !== 'object') return;
		activeCaps = caps;
		activeFlags = {
			STRICT_SCHEMA_EDGE_CHECKS: Boolean(response?.featureFlags?.STRICT_SCHEMA_EDGE_CHECKS ?? true),
			STRICT_SCHEMA_EDGE_CHECKS_V2: Boolean(response?.featureFlags?.STRICT_SCHEMA_EDGE_CHECKS_V2 ?? true),
			STRICT_COERCION_POLICY: Boolean(response?.featureFlags?.STRICT_COERCION_POLICY ?? true)
		};
		NODE_CAPABILITIES = buildNodeCapabilities();
	} catch {
		// Fall back to bundled shared capabilities in offline/dev edge-cases.
	}
}

export function getAllowedPayloadTypesForNode(
	node: Node<PipelineNodeData> | null | undefined,
	direction: Direction
): PayloadType[] {
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

export function isPayloadTypeAllowedForNode(
	node: Node<PipelineNodeData> | null | undefined,
	direction: Direction,
	payloadType: PayloadType | null | undefined
): boolean {
	if (payloadType == null) return true;
	return getAllowedPayloadTypesForNode(node, direction).includes(payloadType);
}
