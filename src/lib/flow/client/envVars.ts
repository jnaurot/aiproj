import { backendUrl } from '$lib/flow/client/backend';

export type RuntimeEnvVar = {
	name: string;
	description: string;
	category: string;
	defaultValue: string | null;
	value: string | null;
	masked: boolean;
	hasValue: boolean;
	source: 'override' | 'env' | 'default' | 'unset';
	restartRequired: boolean;
	sensitive: boolean;
	supported: boolean;
};

export type RuntimeEnvListResponse = {
	schemaVersion: 1;
	vars: RuntimeEnvVar[];
};

export async function listRuntimeEnvVars(
	options?: { revealSensitive?: boolean }
): Promise<RuntimeEnvListResponse> {
	const qs = new URLSearchParams();
	if (options?.revealSensitive) qs.set('revealSensitive', '1');
	const path = qs.toString() ? `/api/env/vars?${qs.toString()}` : '/api/env/vars';
	const res = await fetch(backendUrl(path));
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`listRuntimeEnvVars failed: ${res.status} ${text}`);
	}
	return (await res.json()) as RuntimeEnvListResponse;
}

export async function updateRuntimeEnvVars(
	updates: Array<{ name: string; value?: string | null; unset?: boolean }>
): Promise<{ schemaVersion: 1; updated: string[]; restartRequired: boolean; vars: RuntimeEnvVar[] }> {
	const res = await fetch(backendUrl('/api/env/vars'), {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ updates })
	});
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`updateRuntimeEnvVars failed: ${res.status} ${text}`);
	}
	return (await res.json()) as {
		schemaVersion: 1;
		updated: string[];
		restartRequired: boolean;
		vars: RuntimeEnvVar[];
	};
}

