import { backendUrl } from '$lib/flow/client/backend';

export type EnvProfileStatus = {
	profileId: string;
	packages: string[];
	installed: boolean;
	missingPackages: string[];
	health: 'ok' | 'missing' | string;
	platformNotes: string[];
	available?: boolean;
};

type ListEnvProfilesResponse = {
	schemaVersion: number;
	profiles: EnvProfileStatus[];
	python?: {
		executable?: string;
		version?: string;
	};
};

type InstallEnvProfileResponse = {
	schemaVersion: number;
	profileId: string;
	source?: string;
	packages: string[];
	status: 'installed' | 'already_installed' | 'partial' | string;
	installed: boolean;
	missingPackages: string[];
	audit?: Record<string, unknown> | null;
};

function _asErrorText(status: number, text: string): string {
	const parsed = (() => {
		try {
			return JSON.parse(text ?? '{}') as Record<string, unknown>;
		} catch {
			return null;
		}
	})();
	if (parsed && typeof parsed.detail === 'object' && parsed.detail) {
		const detail = parsed.detail as Record<string, unknown>;
		const code = String(detail.code ?? '').trim();
		const message = String(detail.message ?? '').trim();
		if (code || message) return `${code}${code && message ? ': ' : ''}${message}`;
	}
	return text;
}

export async function listEnvProfiles(): Promise<ListEnvProfilesResponse> {
	const res = await fetch(backendUrl('/api/env/profiles'));
	if (!res.ok) {
		const text = _asErrorText(res.status, await res.text().catch(() => ''));
		throw new Error(`listEnvProfiles failed: ${res.status} ${text}`);
	}
	return (await res.json()) as ListEnvProfilesResponse;
}

export async function installEnvProfile(profileId: string): Promise<InstallEnvProfileResponse> {
	const pid = String(profileId ?? '').trim();
	if (!pid) throw new Error('profileId is required');
	const res = await fetch(backendUrl('/api/env/profiles/install'), {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ profileId: pid })
	});
	if (!res.ok) {
		const text = _asErrorText(res.status, await res.text().catch(() => ''));
		throw new Error(`installEnvProfile failed: ${res.status} ${text}`);
	}
	return (await res.json()) as InstallEnvProfileResponse;
}
