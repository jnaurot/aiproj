import type { PortType } from '$lib/flow/types';

export type BackendCapabilitiesResponse = {
	schemaVersion: number;
	signature: string;
	featureFlags?: {
		STRICT_SCHEMA_EDGE_CHECKS?: boolean;
		STRICT_COERCION_POLICY?: boolean;
	};
	capabilities: {
		schemaVersion?: number;
		allowedPortTypes?: PortType[];
		nodes?: Record<string, { in?: PortType[]; out?: PortType[]; byProvider?: Record<string, { in?: PortType[]; out?: PortType[] }> }>;
	};
};

export async function getBackendCapabilities(): Promise<BackendCapabilitiesResponse> {
	const res = await fetch('/api/capabilities');
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`/api/capabilities failed: ${res.status} ${text}`);
	}
	return (await res.json()) as BackendCapabilitiesResponse;
}
