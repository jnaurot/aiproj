export type EdgeVisualStateInput = {
	edgeMode: string;
	edgeExec: string;
	sourceLifecycle: string;
	targetLifecycle: string;
	waiting: boolean;
	blocked: boolean;
	full: boolean;
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
