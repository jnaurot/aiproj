export type HeaderStatusTone = 'running' | 'succeeded' | 'failed' | 'cancelled' | 'never_run';

export type StatusSnapshot = {
	runStatus: string;
	lastRunStatus: string;
	freshness: string;
	staleNodeCount: number;
	unsaved: boolean;
};

export type ScopedStatusInput = {
	editingContext: 'graph' | 'component';
	graph: StatusSnapshot;
	component: StatusSnapshot;
};

export type ScopedStatusOutput = {
	statusText: string;
	tone: HeaderStatusTone;
	unsaved: boolean;
};

function formatStatus(snapshot: StatusSnapshot): { text: string; tone: HeaderStatusTone } {
	const run = String(snapshot.runStatus ?? '').toLowerCase();
	const last = String(snapshot.lastRunStatus ?? '').toLowerCase();
	const freshness = String(snapshot.freshness ?? '').toLowerCase();
	const staleCount = Number(snapshot.staleNodeCount ?? 0);
	if (run === 'running') {
		return { text: 'Running', tone: 'running' };
	}
	const baseText =
		last === 'never_run'
			? 'Never run'
			: `${last === 'cancelled' ? 'Cancelled' : last.charAt(0).toUpperCase() + last.slice(1)}${
					freshness === 'stale' ? ` + Needs rerun${staleCount > 0 ? ` (${staleCount} stale)` : ''}` : ''
				}`;
	const tone: HeaderStatusTone =
		last === 'succeeded'
			? 'succeeded'
			: last === 'failed'
			? 'failed'
			: last === 'cancelled'
			? 'cancelled'
			: 'never_run';
	return { text: baseText, tone };
}

export function buildScopedStatus(input: ScopedStatusInput): ScopedStatusOutput {
	const active = input.editingContext === 'component' ? input.component : input.graph;
	const formatted = formatStatus(active);
	return {
		statusText: formatted.text,
		tone: formatted.tone,
		unsaved: Boolean(active.unsaved)
	};
}

