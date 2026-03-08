import type { ComponentValidationDiagnostic } from '$lib/flow/client/components';

export type ComponentPreflightSummary = {
	ok: boolean;
	errorCount: number;
	warningCount: number;
	headline: string;
	detail: string;
};

function diagnosticLine(
	index: number,
	diagnostic: { severity?: string; code?: string; path?: string; message?: string }
): string {
	const level = String(diagnostic?.severity ?? 'error').toUpperCase();
	const code = String(diagnostic?.code ?? 'VALIDATION');
	const path = String(diagnostic?.path ?? '').trim();
	const msg = String(diagnostic?.message ?? '').trim();
	const where = path ? ` (${path})` : '';
	return `${index + 1}. [${level}] ${code}${where}${msg ? `: ${msg}` : ''}`;
}

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
	const lines = items.map((d, i) => diagnosticLine(i, d));
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

export function summarizeComponentPublishFailure(
	error: unknown,
	componentId: string,
	revisionId: string
): ComponentPreflightSummary {
	const title = `${componentId || '(component)'}${revisionId ? `@${revisionId}` : ''}`;
	const raw = String(error ?? '');
	const jsonStart = raw.indexOf('{');
	let diagnostics: Array<{ severity?: string; code?: string; path?: string; message?: string }> = [];
	let message = raw;
	if (jsonStart >= 0) {
		try {
			const parsed = JSON.parse(raw.slice(jsonStart));
			const detail = (parsed as any)?.detail ?? {};
			if (Array.isArray(detail?.diagnostics)) diagnostics = detail.diagnostics;
			if (typeof detail?.message === 'string' && detail.message.trim()) message = detail.message;
		} catch {
			// Keep raw message fallback.
		}
	}
	if (diagnostics.length > 0) {
		return {
			ok: false,
			errorCount: diagnostics.length,
			warningCount: 0,
			headline: `Component publish blocked for ${title}`,
			detail: diagnostics.map((d, i) => diagnosticLine(i, d)).join('\n')
		};
	}
	return {
		ok: false,
		errorCount: 1,
		warningCount: 0,
		headline: `Component publish failed for ${title}`,
		detail: message || 'Unknown publish error.'
	};
}
