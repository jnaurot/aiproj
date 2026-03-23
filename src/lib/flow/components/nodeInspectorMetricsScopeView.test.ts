import { describe, expect, it } from 'vitest';

import { queueMetricScopeSummary } from './nodeInspectorSchema';

describe('nodeInspector metrics scope view helpers', () => {
	it('detects both run-scoped and aggregate queue metric sections', () => {
		const summary = queueMetricScopeSummary({
			runScoped: { runId: 'run_1', scope: 'run' },
			aggregateDiagnostics: { queueMetricEvents: 3 }
		});
		expect(summary.runScopedPresent).toBe(true);
		expect(summary.aggregatePresent).toBe(true);
	});
});
