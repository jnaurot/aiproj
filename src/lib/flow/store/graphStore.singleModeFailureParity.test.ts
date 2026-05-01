import { describe, expect, it } from 'vitest';

type LogEntry = { nodeId?: string; level?: string; message?: string };

function rejectedFromRuntimeMetrics(state: any, nodeId: string): number {
	const byHandle =
		(state?.queueRuntime?.runScoped?.runtimeItemMetrics?.byHandle ??
			state?.queueRuntime?.runtimeItemMetrics?.byHandle ??
			{}) as Record<string, any>;
	let rejected = 0;
	for (const metric of Object.values(byHandle)) {
		if (!metric || typeof metric !== 'object') continue;
		if (String((metric as any).nodeId ?? '') !== String(nodeId)) continue;
		const plane = String((metric as any).plane ?? 'work').trim().toLowerCase();
		if (plane !== 'work') continue;
		rejected += Number((metric as any).itemsRejected ?? 0);
	}
	return Math.max(0, rejected);
}

function terminalModelFailures(logs: LogEntry[], nodeId: string): number {
	return logs.filter((entry) => {
		if (String(entry?.nodeId ?? '') !== String(nodeId)) return false;
		if (String(entry?.level ?? '').trim().toLowerCase() !== 'error') return false;
		return /\bMODEL_EXECUTION_FAILED\b/i.test(String(entry?.message ?? ''));
	}).length;
}

describe('single-item model failure parity', () => {
	it('keeps skipped count aligned with terminal model execution failures', () => {
		const nodeId = 'n_model_score_job';
		const state = {
			queueRuntime: {
				runScoped: {
					runtimeItemMetrics: {
						byHandle: {
							'a:in': { nodeId, plane: 'work', itemsAccepted: 4, itemsRejected: 2 },
							'b:in': { nodeId: 'other', plane: 'work', itemsAccepted: 1, itemsRejected: 0 }
						}
					}
				}
			},
			logs: [
				{ nodeId, level: 'error', message: 'MODEL_EXECUTION_FAILED: ollama request failed:' },
				{ nodeId, level: 'error', message: 'MODEL_EXECUTION_FAILED: ollama request failed:' },
				{ nodeId, level: 'warn', message: 'Ollama request failed (attempt 1/2):' }
			] satisfies LogEntry[]
		};
		expect(rejectedFromRuntimeMetrics(state, nodeId)).toBe(2);
		expect(terminalModelFailures(state.logs, nodeId)).toBe(2);
		expect(rejectedFromRuntimeMetrics(state, nodeId)).toBe(terminalModelFailures(state.logs, nodeId));
	});
});

