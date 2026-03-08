import type { ComponentValidationDiagnostic } from '$lib/flow/client/components';

export type ComponentPreflightSummary = {
	ok: boolean;
	errorCount: number;
	warningCount: number;
	headline: string;
	detail: string;
};

export function summarizeComponentPreflight(
	ok: boolean,
	diagnostics: ComponentValidationDiagnostic[] | undefined,
	componentId: string,
	revisionId: string
): ComponentPreflightSummary {
	const items = Array.isArray(diagnostics) ? diagnostics : [];
	const errors = items.filter((d) => String(d?.severity ?? 'error').toLowerCase() === 'error');
	const warnings = items.filter((d) => String(d?.severity ?? '').toLowerCase() === 'warning');
	const title = `${componentId || '(component)'}${revisionId ? `@${revisionId}` : ''}`;
	const lines = items.map((d, i) => {
		const level = String(d?.severity ?? 'error').toUpperCase();
		const code = String(d?.code ?? 'VALIDATION');
		const path = String(d?.path ?? '').trim();
		const msg = String(d?.message ?? '').trim();
		const where = path ? ` (${path})` : '';
		return `${i + 1}. [${level}] ${code}${where}${msg ? `: ${msg}` : ''}`;
	});
	if (!ok || errors.length > 0) {
		return {
			ok: false,
			errorCount: errors.length || 1,
			warningCount: warnings.length,
			headline: `Component publish blocked for ${title}`,
			detail: lines.join('\n') || 'Validation failed with no diagnostic details.'
		};
	}
	if (warnings.length > 0) {
		return {
			ok: true,
			errorCount: 0,
			warningCount: warnings.length,
			headline: `Component preflight warnings for ${title}`,
			detail: lines.join('\n')
		};
	}
	return {
		ok: true,
		errorCount: 0,
		warningCount: 0,
		headline: `Component preflight passed for ${title}`,
		detail: 'No diagnostics.'
	};
}
