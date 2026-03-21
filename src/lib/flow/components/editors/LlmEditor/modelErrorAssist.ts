import type { NodeExecutionError, NodeSchemaContractEdge } from '$lib/flow/store/graphStore';

export type ModelAssistFix =
	| { id: string; label: string; patch: Record<string, unknown> }
	| { id: string; label: string; edgeId: string };

export function buildModelAutoFixes(input: {
	nodeError: NodeExecutionError | null;
	params: Record<string, unknown>;
	schemaEdges: NodeSchemaContractEdge[];
}): ModelAssistFix[] {
	const err = input.nodeError;
	if (!err) return [];
	const code = String(err.errorCode ?? '').trim().toUpperCase();
	const message = String(err.message ?? '').trim().toLowerCase();
	const fixes: ModelAssistFix[] = [];

	if (code === 'CONTRACT_EDGE_PAYLOAD_TYPE_MISMATCH') {
		const incomingWithAdapter = (input.schemaEdges ?? []).find(
			(edge) => edge.direction === 'incoming' && edge.adapterKind && edge.severity !== 'clean'
		);
		if (incomingWithAdapter) {
			fixes.push({
				id: 'insert_adapter',
				label: `Insert ${incomingWithAdapter.adapterKind} adapter`,
				edgeId: incomingWithAdapter.edgeId
			});
		}
	}

	if (code === 'MISSING_SECRET' || message.includes('missing_secret') || message.includes('connection_ref')) {
		fixes.push({
			id: 'clear_connection_ref',
			label: 'Clear missing connectionRef',
			patch: { connectionRef: undefined }
		});
	}

	if (
		message.includes('unsupported output type') ||
		message.includes('output_mode') ||
		(code === 'INVALID_VALUE' && String(err.paramPath ?? '').includes('output'))
	) {
		fixes.push({
			id: 'set_output_text',
			label: 'Set output mode to text',
			patch: {
				output: {
					...((input.params?.output as Record<string, unknown> | undefined) ?? {}),
					mode: 'text',
					strict: true
				}
			}
		});
	}

	return fixes;
}
