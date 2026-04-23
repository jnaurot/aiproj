import type { EdgeDiagnosticSnapshot } from '$lib/flow/store/graphStore.types';

export function resolveSchemaClassFromSnapshot(snapshot: EdgeDiagnosticSnapshot | null): '' | 'edge-schema-warning' | 'edge-schema-error' {
	const severity = String(snapshot?.effectiveSeverity ?? 'clean').trim().toLowerCase();
	if (severity === 'error') return 'edge-schema-error';
	if (severity === 'warning') return 'edge-schema-warning';
	return '';
}

export function buildSchemaTooltip(
	snapshot: EdgeDiagnosticSnapshot | null,
	fallbackDiagMessage?: string | null,
	fallbackSchemaMessage?: string | null
): string | undefined {
	const severity = String(snapshot?.contractSeverity ?? 'clean').trim().toLowerCase();
	const contractMsg = String(snapshot?.contractMessage ?? '').trim();
	const schemaPlaneState = String(snapshot?.schemaPlaneState ?? '').trim().toLowerCase();
	const schemaPlaneMsg = String(snapshot?.schemaPlaneMessage ?? '').trim();
	const fallbackDiag = String(fallbackDiagMessage ?? '').trim();
	const fallbackSchema = String(fallbackSchemaMessage ?? '').trim();
	const lines: string[] = [];
	if (severity === 'error' || severity === 'warning') {
		lines.push(`Schema: ${severity} (contract)`);
		if (contractMsg) lines.push(contractMsg);
		else if (fallbackDiag) lines.push(fallbackDiag);
	} else if (severity === 'clean') {
		lines.push('Schema: clean (contract)');
	}
	if (schemaPlaneState === 'warning' || schemaPlaneState === 'error') {
		lines.push(`Schema-plane note: ${schemaPlaneState}`);
		if (schemaPlaneMsg) lines.push(schemaPlaneMsg);
		else if (fallbackSchema) lines.push(fallbackSchema);
	}
	return lines.length > 0 ? lines.join('\n') : undefined;
}

