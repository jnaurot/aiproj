import contractRaw from '../../../../shared/schema_diagnostics.v1.json';

const contract = contractRaw as {
	version: number;
	codes: string[];
};

const knownCodes = Array.isArray(contract.codes) ? contract.codes : [];

export const SCHEMA_DIAGNOSTIC_CODE_SET = new Set<string>(knownCodes);

export type SchemaDiagnosticCode = 'TYPE_MISMATCH' | 'PAYLOAD_SCHEMA_MISMATCH';

export function isSchemaDiagnosticCode(value: unknown): value is SchemaDiagnosticCode {
	return typeof value === 'string' && SCHEMA_DIAGNOSTIC_CODE_SET.has(value);
}
