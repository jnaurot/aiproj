import type { PortType } from '$lib/flow/types';
import { backendUrl } from '$lib/flow/client/backend';

export type BackendCapabilitiesResponse = {
	schemaVersion: number;
	signature: string;
	featureFlags?: {
		GRAPH_PERSIST_DERIVED_PORTS_OMITTED?: boolean;
		STRICT_SCHEMA_EDGE_CHECKS?: boolean;
		STRICT_SCHEMA_EDGE_CHECKS_V2?: boolean;
		STRICT_COERCION_POLICY?: boolean;
	};
	capabilities: {
		schemaVersion?: number;
		allowedPortTypes?: PortType[];
		nodes?: Record<string, { in?: PortType[]; out?: PortType[]; byProvider?: Record<string, { in?: PortType[]; out?: PortType[] }> }>;
	};
};

export async function getBackendCapabilities(): Promise<BackendCapabilitiesResponse> {
	const url = backendUrl('/api/capabilities');
	const res = await fetch(url);
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`${url} failed: ${res.status} ${text}`);
	}
	return (await res.json()) as BackendCapabilitiesResponse;
}
