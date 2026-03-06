import type { Node } from '@xyflow/svelte';
import type { PipelineNodeData, PortType } from '$lib/flow/types';
import capsRaw from '../../../shared/port_capabilities.v1.json';

type Direction = 'in' | 'out';

type NodeCapabilities = {
	in?: PortType[];
	out?: PortType[];
	toolInByProvider?: Record<string, PortType[]>;
	toolOutByProvider?: Record<string, PortType[]>;
};

const caps = capsRaw as any;
const nodes = (caps?.nodes ?? {}) as Record<string, any>;
const toolByProvider = (nodes.tool?.byProvider ?? {}) as Record<string, any>;

function asPortTypes(values: unknown): PortType[] {
	if (!Array.isArray(values)) return [];
	return values
		.map((v) => String(v))
		.filter((v): v is PortType => ['table', 'json', 'text', 'binary', 'embeddings'].includes(v));
}

export const NODE_CAPABILITIES: Record<'llm' | 'transform' | 'source' | 'tool' | 'component', NodeCapabilities> = {
	llm: { in: asPortTypes(nodes.llm?.in), out: asPortTypes(nodes.llm?.out) },
	transform: { in: asPortTypes(nodes.transform?.in), out: asPortTypes(nodes.transform?.out) },
	source: { in: asPortTypes(nodes.source?.in), out: asPortTypes(nodes.source?.out) },
	tool: {
		in: asPortTypes(nodes.tool?.in),
		out: asPortTypes(nodes.tool?.out),
		toolInByProvider: Object.fromEntries(
			Object.entries(toolByProvider).map(([provider, value]) => [provider, asPortTypes((value as any)?.in)])
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
