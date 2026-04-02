import { asString } from '$lib/flow/components/editors/shared';
import type {
	SourceAPIParams,
	SourceDatabaseParams,
	SourceFileParams,
	SourceObjectStoreParams,
	SourceOutputMode,
	SourceWarehouseParams
} from '$lib/flow/schema/source';

export type SourceKind = 'api' | 'database' | 'file' | 'object_store' | 'warehouse';

export type EffectiveConfigLine = {
	key: string;
	value: string;
};

export function effectiveConfigForSource(
	sourceKind: SourceKind,
	params: Record<string, unknown>
): EffectiveConfigLine[] {
	if (sourceKind === 'database') {
		const db = params as Partial<SourceDatabaseParams>;
		const connectionString = asString(db.connection_string, '').trim();
		const connectionRef = asString(db.connection_ref, '').trim();
		const query = asString(db.query, '').trim();
		const table = asString(db.table_name, '').trim();
		const output = (asString(db.output?.mode, 'table') as SourceOutputMode) ?? 'table';
		return [
			{ key: 'output', value: output },
			{
				key: 'connection',
				value: connectionRef ? 'connection_ref' : connectionString ? 'connection_string' : 'missing'
			},
			{ key: 'input', value: query ? 'query' : table ? 'table_name' : 'missing' }
		];
	}

	if (sourceKind === 'object_store') {
		const store = params as Partial<SourceObjectStoreParams>;
		const mode = (asString(
			store.object_store_mode,
			'provider'
		) as SourceObjectStoreParams['object_store_mode']) ?? 'provider';
		const output = (asString(store.output?.mode, 'text') as SourceOutputMode) ?? 'text';
		const connectionRef = asString(store.connection_ref, '').trim();
		return [
			{ key: 'mode', value: mode },
			{ key: 'output', value: output },
			{ key: 'provider', value: asString(store.provider, 's3') },
			{
				key: 'connection',
				value: mode === 'mock' ? 'mock' : connectionRef ? 'connection_ref' : 'missing'
			}
		];
	}

	if (sourceKind === 'warehouse') {
		const wh = params as Partial<SourceWarehouseParams>;
		const connectionString = asString(wh.connection_string, '').trim();
		const connectionRef = asString(wh.connection_ref, '').trim();
		const query = asString(wh.query, '').trim();
		const output = (asString(wh.output?.mode, 'table') as SourceOutputMode) ?? 'table';
		return [
			{ key: 'provider', value: asString(wh.provider, 'snowflake') },
			{ key: 'output', value: output },
			{
				key: 'connection',
				value: connectionRef ? 'connection_ref' : connectionString ? 'connection_string' : 'missing'
			},
			{ key: 'input', value: query ? 'query' : 'missing' }
		];
	}

	if (sourceKind === 'api') {
		const api = params as Partial<SourceAPIParams>;
		return [
			{ key: 'method', value: asString(api.method, 'GET') },
			{ key: 'output', value: asString(api.output?.mode, 'json') },
			{ key: 'body_mode', value: asString(api.bodyMode, 'none') },
			{ key: 'auth', value: asString(api.auth_type, 'none') }
		];
	}

	const file = params as Partial<SourceFileParams>;
	return [
		{ key: 'format', value: asString(file.file_format, 'csv') },
		{ key: 'encoding', value: asString(file.encoding, 'utf-8') },
		{ key: 'output', value: asString(file.output?.mode, 'text') },
		{ key: 'snapshot', value: asString(file.snapshotId, '').trim() ? 'set' : 'missing' }
	];
}

export function fileAutoAdjustmentNotices(
	currentParams: Partial<SourceFileParams>,
	nextPatch: Partial<SourceFileParams>,
	reason: string
): string[] {
	const notices: string[] = [];
	const nextFormat = asString(nextPatch.file_format, currentParams.file_format ?? '');
	const currentOutput = asString(currentParams.output?.mode, '');
	const nextOutput = asString(nextPatch.output?.mode, currentOutput);
	if (nextOutput && nextOutput !== currentOutput) {
		notices.push(`${reason}: output.mode auto-set to ${nextOutput} for ${nextFormat || 'format change'}`);
	}
	if (Object.prototype.hasOwnProperty.call(nextPatch, 'delimiter')) {
		notices.push(`${reason}: delimiter normalized for ${nextFormat || 'selected format'}`);
	}
	if (Object.prototype.hasOwnProperty.call(nextPatch, 'json_mode')) {
		notices.push(`${reason}: json_mode adjusted for ${nextFormat || 'json format'}`);
	}
	if (Object.prototype.hasOwnProperty.call(nextPatch, 'sheet_name')) {
		notices.push(`${reason}: sheet_name adjusted for ${nextFormat || 'excel format'}`);
	}
	return notices;
}

