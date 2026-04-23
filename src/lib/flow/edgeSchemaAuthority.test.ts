import { describe, expect, it } from 'vitest';

import { buildSchemaTooltip, resolveSchemaClassFromSnapshot } from './edgeSchemaAuthority';

describe('edge schema authority presentation', () => {
	it('does not produce warning class when contract is clean and schema plane is warning', () => {
		const schemaClass = resolveSchemaClassFromSnapshot({
			edgeId: 'e1',
			contractSeverity: 'clean',
			schemaPlaneState: 'warning',
			runtimeState: 'settled',
			effectiveSeverity: 'clean',
			schemaPlaneMessage: 'Schema unverified: upstream output is opaque.'
		});
		expect(schemaClass).toBe('');
	});

	it('uses warning class when contract severity is warning', () => {
		const schemaClass = resolveSchemaClassFromSnapshot({
			edgeId: 'e2',
			contractSeverity: 'warning',
			schemaPlaneState: 'valid',
			runtimeState: 'settled',
			effectiveSeverity: 'warning',
			contractMessage: 'Work payload mismatch: lossy coercion text -> json'
		});
		expect(schemaClass).toBe('edge-schema-warning');
	});

	it('includes contract authority and schema-plane note in tooltip', () => {
		const text = buildSchemaTooltip(
			{
				edgeId: 'e3',
				contractSeverity: 'warning',
				schemaPlaneState: 'warning',
				runtimeState: 'inactive',
				effectiveSeverity: 'warning',
				contractMessage: 'Edge contract snapshot drift detected.',
				schemaPlaneMessage: 'Schema unverified: upstream output is opaque.'
			},
			undefined,
			undefined
		);
		expect(String(text ?? '')).toContain('Schema: warning (contract)');
		expect(String(text ?? '')).toContain('Schema-plane note: warning');
	});

	it('supports feature-flag parity for authority selection', () => {
		const snapshot = {
			edgeId: 'e4',
			contractSeverity: 'clean',
			schemaPlaneState: 'warning',
			runtimeState: 'settled',
			effectiveSeverity: 'clean'
		} as const;
		const onClass = resolveSchemaClassFromSnapshot(snapshot as any, 'edge-schema-warning', true);
		const offClass = resolveSchemaClassFromSnapshot(snapshot as any, 'edge-schema-warning', false);
		expect(onClass).toBe('');
		expect(offClass).toBe('edge-schema-warning');
	});
});
