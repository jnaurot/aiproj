import { asString } from '$lib/flow/components/editors/shared';
import type { SourceAPIParams, SourceDatabaseParams } from '$lib/flow/schema/source';

export type SourceValidationHint = {
	controlId: string;
	message: string;
	level: 'info' | 'warning' | 'error';
};

export function sourceDatabaseValidationHints(
	params: Partial<SourceDatabaseParams>
): SourceValidationHint[] {
	const hints: SourceValidationHint[] = [];
	const hasConnectionString = asString(params.connection_string, '').trim().length > 0;
	const hasConnectionRef = asString(params.connection_ref, '').trim().length > 0;
	const hasQuery = asString(params.query, '').trim().length > 0;
	const hasTable = asString(params.table_name, '').trim().length > 0;

	if (!hasConnectionString && !hasConnectionRef) {
		hints.push({
			controlId: 'connection',
			message: 'Provide connection_string or connection_ref.',
			level: 'error'
		});
	}
	if (!hasQuery && !hasTable) {
		hints.push({
			controlId: 'input',
			message: 'Provide query or table_name.',
			level: 'error'
		});
	}
	if (hasQuery && hasTable) {
		hints.push({
			controlId: 'input',
			message: 'query takes precedence when both query and table_name are set.',
			level: 'info'
		});
	}
	return hints;
}

export function sourceApiValidationHints(params: Partial<SourceAPIParams>): SourceValidationHint[] {
	const hints: SourceValidationHint[] = [];
	const mode = asString(params.bodyMode, 'none');
	const contentType = asString(params.contentType, '');
	if (mode === 'none' && contentType) {
		hints.push({
			controlId: 'content_type',
			message: 'Content-Type is set but body mode is none.',
			level: 'warning'
		});
	}
	if (mode === 'json' && contentType && contentType !== 'application/json') {
		hints.push({
			controlId: 'content_type',
			message: 'JSON body mode usually expects Content-Type application/json.',
			level: 'warning'
		});
	}
	if ((mode === 'form' || mode === 'multipart') && contentType === 'application/json') {
		hints.push({
			controlId: 'content_type',
			message: 'Form-based body mode should not use application/json Content-Type.',
			level: 'warning'
		});
	}
	return hints;
}

export function sourceControlFromParamPath(
	sourceKind: 'database' | 'api',
	paramPath: string
): string | null {
	const path = String(paramPath ?? '').trim();
	if (!path) return null;
	if (sourceKind === 'database') {
		if (path.startsWith('connection_string') || path.startsWith('connection_ref')) return 'connection';
		if (path.startsWith('query') || path.startsWith('table_name')) return 'input';
		return null;
	}
	if (path.startsWith('bodyMode') || path.startsWith('contentType')) return 'content_type';
	if (path.startsWith('auth_')) return 'auth';
	if (path.startsWith('url') || path.startsWith('method')) return 'request';
	return null;
}

