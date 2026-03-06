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

export type GraphRevisionSummary = {
	graph_id: string;
	revision_id: string;
	parent_revision_id?: string | null;
	created_at: string;
	message?: string | null;
	schema_version: number;
	checksum: string;
};

export type ListGraphRevisionsResponse = {
	schemaVersion: number;
	graphId: string;
	revisions: GraphRevisionSummary[];
};

export type CreateGraphRevisionRequest = {
	graphId?: string;
	revisionId?: string;
	parentRevisionId?: string;
	message?: string;
	schemaVersion?: number;
	graph: {
		version?: number;
		nodes: unknown[];
		edges: unknown[];
		meta?: Record<string, unknown>;
	};
};

export type CreateGraphRevisionResponse = {
	schemaVersion: number;
	graphId: string;
	revisionId: string;
	parentRevisionId?: string | null;
	createdAt: string;
	message?: string | null;
	checksum: string;
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

export async function listGraphRevisions(
	graphId: string,
	limit = 50,
	offset = 0
): Promise<ListGraphRevisionsResponse> {
	const gid = String(graphId ?? '').trim();
	if (!gid) throw new Error('graphId is required');
	const query = `limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`;
	return await _fetchJsonWithFallback<ListGraphRevisionsResponse>(
		`/graphs/${encodeURIComponent(gid)}/revisions?${query}`,
		`/api/graphs/${encodeURIComponent(gid)}/revisions?${query}`
	);
}

export async function getGraphRevision(
	graphId: string,
	revisionId: string
): Promise<LatestGraphRevisionResponse> {
	const gid = String(graphId ?? '').trim();
	const rid = String(revisionId ?? '').trim();
	if (!gid) throw new Error('graphId is required');
	if (!rid) throw new Error('revisionId is required');
	return await _fetchJsonWithFallback<LatestGraphRevisionResponse>(
		`/graphs/${encodeURIComponent(gid)}/revisions/${encodeURIComponent(rid)}`,
		`/api/graphs/${encodeURIComponent(gid)}/revisions/${encodeURIComponent(rid)}`
	);
}

export async function createGraphRevision(
	req: CreateGraphRevisionRequest
): Promise<CreateGraphRevisionResponse> {
	let res = await fetch('/graphs', {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(req)
	});
	if (res.status === 404) {
		res = await fetch('/api/graphs', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(req)
		});
	}
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`createGraphRevision failed: ${res.status} ${text}`);
	}
	return (await res.json()) as CreateGraphRevisionResponse;
}
