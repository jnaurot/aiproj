import type {
	SourceAPIParams,
	SourceDatabaseParams,
	SourceOutputMode,
	SourceWarehouseParams
} from '$lib/flow/schema/source';

export function withOutputMode<T extends { output?: Record<string, unknown> }>(
	params: T,
	mode: SourceOutputMode
): Partial<T> {
	return {
		output: {
			...(params.output ?? {}),
			mode
		}
	} as Partial<T>;
}

export function withDatabaseConnection(
	params: Partial<SourceDatabaseParams>,
	patch: { connection_string?: string; connection_ref?: string }
): Partial<SourceDatabaseParams> {
	return {
		connection_string: patch.connection_string ?? params.connection_string,
		connection_ref: patch.connection_ref ?? params.connection_ref
	};
}

export function withWarehouseConnection(
	params: Partial<SourceWarehouseParams>,
	patch: { connection_string?: string; connection_ref?: string }
): Partial<SourceWarehouseParams> {
	return {
		connection_string: patch.connection_string ?? params.connection_string,
		connection_ref: patch.connection_ref ?? params.connection_ref
	};
}

export function withIncrementalEnabled<
	T extends { incremental?: Record<string, unknown> }
>(params: T, enabled: boolean): Partial<T> {
	return {
		incremental: {
			...(params.incremental ?? {}),
			enabled
		}
	} as Partial<T>;
}

export function withPartitionEnabled<T extends { partition?: Record<string, unknown> }>(
	params: T,
	enabled: boolean
): Partial<T> {
	return {
		partition: {
			...(params.partition ?? {}),
			enabled
		}
	} as Partial<T>;
}

export function withApiCacheMode(
	params: Partial<SourceAPIParams>,
	mode: 'default' | 'never' | 'ttl',
	ttlSeconds = 60
): Partial<SourceAPIParams> {
	return {
		cache_policy: {
			...(params.cache_policy ?? {}),
			mode,
			ttl_seconds: mode === 'ttl' ? ttlSeconds : undefined
		}
	};
}

export function deepMergeWithoutParamLoss<T extends Record<string, unknown>>(
	current: T,
	patch: Partial<T>
): T {
	const output: Record<string, unknown> = { ...current };
	for (const [key, value] of Object.entries(patch)) {
		if (
			value &&
			typeof value === 'object' &&
			!Array.isArray(value) &&
			current[key] &&
			typeof current[key] === 'object' &&
			!Array.isArray(current[key])
		) {
			output[key] = deepMergeWithoutParamLoss(
				current[key] as Record<string, unknown>,
				value as Record<string, unknown>
			);
		} else {
			output[key] = value;
		}
	}
	return output as T;
}

