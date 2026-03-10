import { backendUrl } from '$lib/flow/client/backend';
import { applyRunEvent, currentRunId } from './stores';
import type { KnownRunEvent, RunEvent } from './types';

let es: EventSource | null = null;

function isKnownRunEvent(ev: RunEvent): ev is KnownRunEvent {
	return (
		ev !== null &&
		typeof ev === 'object' &&
		'type' in ev &&
		typeof (ev as any).type === 'string' &&
		['run_started', 'run_finished', 'node_started', 'node_finished', 'edge_exec', 'log', 'node_output'].includes(
			(ev as any).type
		)
	);
}

export function connectRunEvents(runId: string) {
	disconnectRunEvents();
	currentRunId.set(runId);

	es = new EventSource(backendUrl(`/api/runs/${encodeURIComponent(runId)}/events`));
	es.onerror = () => {
		// Browser retries automatically for SSE.
	};
	es.onmessage = (msg) => {
		try {
			const ev = JSON.parse(msg.data) as RunEvent;
			if (isKnownRunEvent(ev)) applyRunEvent(ev);
		} catch {
			// ignore malformed events
		}
	};
}

export function disconnectRunEvents() {
	if (!es) return;
	es.close();
	es = null;
}
