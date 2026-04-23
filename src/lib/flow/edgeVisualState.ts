export type EdgeVisualStateInput = {
	edgeMode: string;
	edgeExec: string;
	sourceLifecycle: string;
	targetLifecycle: string;
	waiting: boolean;
	blocked: boolean;
	full: boolean;
};

export type GraphViewMode = 'execution' | 'schema';

export type EdgeVisualInput = EdgeVisualStateInput & {
	viewMode: GraphViewMode;
	schemaClass: '' | 'edge-schema-warning' | 'edge-schema-error';
};

export function resolveEdgeVisualClass(input: EdgeVisualStateInput):
	| 'edge-state-nonwork'
	| 'edge-state-running'
	| 'edge-state-settled'
	| 'edge-state-blocked'
	| 'edge-state-waiting'
	| 'edge-state-inactive' {
	const edgeMode = String(input.edgeMode ?? '').trim().toLowerCase();
	const edgeExec = String(input.edgeExec ?? '').trim().toLowerCase();
	const sourceLifecycle = String(input.sourceLifecycle ?? '').trim().toLowerCase();
	const targetLifecycle = String(input.targetLifecycle ?? '').trim().toLowerCase();
	const waiting = Boolean(input.waiting);
	const blocked = Boolean(input.blocked);
	const full = Boolean(input.full);

	if (edgeMode !== 'work') {
		return 'edge-state-nonwork';
	}
	if (edgeExec === 'active') {
		return 'edge-state-running';
	}
	if (sourceLifecycle === 'completed' && targetLifecycle === 'completed') {
		return 'edge-state-settled';
	}
	if (blocked || full) {
		return 'edge-state-blocked';
	}
	if (waiting) {
		return 'edge-state-waiting';
	}
	return 'edge-state-inactive';
}

/**
 * Schema-view-aware edge visual class resolver.
 *
 * In schema view all execution signals are suppressed; edge colour is
 * derived solely from the contract validation result (schemaClass).
 * In execution view this delegates to resolveEdgeVisualClass.
 */
export function computeEdgeVisualClass(input: EdgeVisualInput): string {
	if (input.viewMode === 'schema') {
		if (input.schemaClass === 'edge-schema-error') return 'edge-state-blocked';
		if (input.schemaClass === 'edge-schema-warning') return 'edge-state-waiting';
		return 'edge-state-inactive';
	}
	return resolveEdgeVisualClass(input);
}
