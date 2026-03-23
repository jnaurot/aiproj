import { describe, expect, it } from 'vitest';

import { schemaEdgeDriftGuidance } from './nodeInspectorSchema';

describe('schemaEdgeDriftGuidance', () => {
	it('returns null when no drift is present', () => {
		const msg = schemaEdgeDriftGuidance({
			edgeId: 'e1',
			mode: 'work',
			direction: 'incoming',
			sourceNodeId: 'a',
			targetNodeId: 'b',
			sourceHandle: 'out',
			targetHandle: 'in',
			providedSchema: { type: 'text' },
			requiredSchema: { type: 'text' },
			severity: 'clean',
			suggestions: [],
			adapterKind: null
		} as any);
		expect(msg).toBeNull();
	});

	it('formats guidance when snapshot drift is present', () => {
		const msg = schemaEdgeDriftGuidance({
			edgeId: 'e1',
			mode: 'work',
			direction: 'incoming',
			sourceNodeId: 'a',
			targetNodeId: 'b',
			sourceHandle: 'out',
			targetHandle: 'in',
			providedSchema: { type: 'text' },
			requiredSchema: { type: 'text' },
			severity: 'warning',
			snapshotDrift: true,
			snapshotSourceSchemaFingerprint: 'abcdef1234567890',
			snapshotTargetSchemaFingerprint: '1122334455667788',
			currentSourceSchemaFingerprint: 'fedcba9876543210',
			currentTargetSchemaFingerprint: '8877665544332211',
			suggestions: [],
			adapterKind: null
		} as any);
		expect(String(msg ?? '')).toContain('Contract drift detected');
		expect(String(msg ?? '')).toContain('abcdef123456');
		expect(String(msg ?? '')).toContain('fedcba987654');
	});
});

