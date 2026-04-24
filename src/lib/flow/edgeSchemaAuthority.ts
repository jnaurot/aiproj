import type { EdgeDiagnosticSnapshot } from '$lib/flow/store/graphStore.types';

export const USE_CONTRACT_SEVERITY_AUTHORITY = (() => {
	try {
		const raw = String(import.meta.env.VITE_USE_CONTRACT_SEVERITY_AUTHORITY ?? 'true')
			.trim()
			.toLowerCase();
		return raw !== 'false' && raw !== '0' && raw !== 'off';
	} catch {
		return true;
	}
})();

export function resolveSchemaClassFromSnapshot(
	snapshot: EdgeDiagnosticSnapshot | null,
	legacySchemaClass: '' | 'edge-schema-warning' | 'edge-schema-error' = '',
	useContractSeverityAuthority: boolean = USE_CONTRACT_SEVERITY_AUTHORITY
): '' | 'edge-schema-warning' | 'edge-schema-error' {
	if (!useContractSeverityAuthority) return legacySchemaClass;
	const severity = String(snapshot?.effectiveSeverity ?? 'clean').trim().toLowerCase();
	if (severity === 'error') return 'edge-schema-error';
	if (severity === 'warning') return 'edge-schema-warning';
	return '';
}

export function resolveSchemaClassForView(
	viewMode: 'execution' | 'schema',
	snapshot: EdgeDiagnosticSnapshot | null,
	legacySchemaClass: '' | 'edge-schema-warning' | 'edge-schema-error' = '',
	useContractSeverityAuthority: boolean = USE_CONTRACT_SEVERITY_AUTHORITY
): '' | 'edge-schema-warning' | 'edge-schema-error' {
	if (viewMode !== 'schema') return '';
	return resolveSchemaClassFromSnapshot(snapshot, legacySchemaClass, useContractSeverityAuthority);
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
	const effective = String(snapshot?.effectiveSeverity ?? (severity || 'clean')).trim().toLowerCase();
	if (snapshot) {
		lines.push(`Authority: effective=${effective} contract=${severity || 'clean'} schemaPlane=${schemaPlaneState || 'neutral'}`);
	}
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

export function summarizeSchemaOverlayCounts(
	snapshots: Array<EdgeDiagnosticSnapshot | null | undefined>
): { errorCount: number; warningCount: number } {
	let errorCount = 0;
	let warningCount = 0;
	for (const snapshot of snapshots) {
		const severity = String(snapshot?.effectiveSeverity ?? 'clean').trim().toLowerCase();
		if (severity === 'error') errorCount += 1;
		else if (severity === 'warning') warningCount += 1;
	}
	return { errorCount, warningCount };
}
