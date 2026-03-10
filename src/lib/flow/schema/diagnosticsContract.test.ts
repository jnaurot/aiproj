import { describe, expect, it } from 'vitest';

import { isSchemaDiagnosticCode, SCHEMA_DIAGNOSTIC_CODE_SET } from './diagnosticsContract';

describe('schema diagnostics contract', () => {
	it('contains the frontend/backend parity codes', () => {
		expect(SCHEMA_DIAGNOSTIC_CODE_SET.has('TYPE_MISMATCH')).toBe(true);
		expect(SCHEMA_DIAGNOSTIC_CODE_SET.has('PAYLOAD_SCHEMA_MISMATCH')).toBe(true);
	});

	it('validates known diagnostic codes', () => {
		expect(isSchemaDiagnosticCode('TYPE_MISMATCH')).toBe(true);
		expect(isSchemaDiagnosticCode('PAYLOAD_SCHEMA_MISMATCH')).toBe(true);
		expect(isSchemaDiagnosticCode('EDGE_SCHEMA_TYPE_MISMATCH')).toBe(false);
	});
});
