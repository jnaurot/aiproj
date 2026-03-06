export type GraphFeatureFlags = {
	schemaVersion: number;
	flags: {
		GRAPH_STORE_V2_READ: boolean;
		GRAPH_STORE_V2_WRITE: boolean;
		GRAPH_EXPORT_V2: boolean;
	};
};

export type LatestGraphRevisionResponse = {
	schemaVersion: number;
	graphId: string;
	revisionId: string;
	parentRevisionId?: string | null;
	createdAt: string;
	message?: string | null;
	revisionSchemaVersion: number;
	checksum: string;
	graph: {
		version?: number;
		nodes: unknown[];
		edges: unknown[];
		meta?: Record<string, unknown>;
	};
};

async function _fetchJsonWithFallback<T>(primary: string, fallback: string): Promise<T> {
	let res = await fetch(primary);
	if (res.status === 404) {
		res = await fetch(fallback);
	}
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`${primary} failed: ${res.status} ${text}`);
	}
	return (await res.json()) as T;
}

export async function getGraphFeatureFlags(): Promise<GraphFeatureFlags> {
	return await _fetchJsonWithFallback<GraphFeatureFlags>(
		'/graphs/feature-flags',
		'/api/graphs/feature-flags'
	);
}

export async function getLatestGraphRevision(graphId: string): Promise<LatestGraphRevisionResponse> {
	const gid = String(graphId ?? '').trim();
	if (!gid) throw new Error('graphId is required');
	return await _fetchJsonWithFallback<LatestGraphRevisionResponse>(
		`/graphs/${encodeURIComponent(gid)}/latest`,
		`/api/graphs/${encodeURIComponent(gid)}/latest`
	);
}

