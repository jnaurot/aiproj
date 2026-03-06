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
	graphId: string;
	revisionId: string;
	parentRevisionId?: string | null;
	createdAt: string;
	message?: string | null;
	schemaVersion: number;
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

export type GraphPackageV2 = {
	manifest: {
		packageType: 'aipgraph';
		packageVersion: 2;
		schemaVersion: number;
		engineVersion: string;
		exportedAt: string;
		source?: { graphId?: string; revisionId?: string };
		includes?: { artifacts?: boolean; schemas?: boolean };
		dependencies?: {
			components?: Array<{
				componentId: string;
				revisionId: string;
				apiVersion?: string;
			}>;
		};
		warnings?: string[];
	};
	graph: {
		version?: number;
		nodes: unknown[];
		edges: unknown[];
		meta?: Record<string, unknown>;
	};
	schemas?: Record<string, unknown> | null;
	artifacts?: unknown[] | null;
};

export type ExportGraphPackageResponse = {
	schemaVersion: number;
	package: GraphPackageV2;
};

export type ImportGraphPackageRequest = {
	package: Record<string, unknown>;
	targetGraphId?: string;
	message?: string;
};

export type ImportGraphPackageResponse = {
	schemaVersion: number;
	graphId: string;
	revisionId: string;
	createdAt: string;
	migrationReport: {
		format: string;
		migrated: boolean;
		warnings: string[];
		componentDependencies?: Array<{
			componentId: string;
			revisionId: string;
			apiVersion?: string;
		}>;
		unresolvedComponentDependencies?: Array<{
			componentId: string;
			revisionId: string;
			reason: string;
		}>;
	};
	graph: {
		version?: number;
		nodes: unknown[];
		edges: unknown[];
		meta?: Record<string, unknown>;
	};
};

async function _fetchJson<T>(url: string): Promise<T> {
	const res = await fetch(url);
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`${url} failed: ${res.status} ${text}`);
	}
	return (await res.json()) as T;
}

export async function getGraphFeatureFlags(): Promise<GraphFeatureFlags> {
	return await _fetchJson<GraphFeatureFlags>('/api/graphs/feature-flags');
}

export async function getLatestGraphRevision(graphId: string): Promise<LatestGraphRevisionResponse> {
	const gid = String(graphId ?? '').trim();
	if (!gid) throw new Error('graphId is required');
	return await _fetchJson<LatestGraphRevisionResponse>(`/api/graphs/${encodeURIComponent(gid)}/latest`);
}

export async function listGraphRevisions(
	graphId: string,
	limit = 50,
	offset = 0
): Promise<ListGraphRevisionsResponse> {
	const gid = String(graphId ?? '').trim();
	if (!gid) throw new Error('graphId is required');
	const query = `limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`;
	return await _fetchJson<ListGraphRevisionsResponse>(
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
	return await _fetchJson<LatestGraphRevisionResponse>(
		`/api/graphs/${encodeURIComponent(gid)}/revisions/${encodeURIComponent(rid)}`
	);
}

export async function createGraphRevision(
	req: CreateGraphRevisionRequest
): Promise<CreateGraphRevisionResponse> {
	const res = await fetch('/api/graphs', {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(req)
	});
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`createGraphRevision failed: ${res.status} ${text}`);
	}
	return (await res.json()) as CreateGraphRevisionResponse;
}

export async function exportGraphPackage(
	graphId: string,
	opts?: { revisionId?: string; includeArtifacts?: boolean; includeSchemas?: boolean }
): Promise<ExportGraphPackageResponse> {
	const gid = String(graphId ?? '').trim();
	if (!gid) throw new Error('graphId is required');
	const params = new URLSearchParams();
	if (opts?.revisionId) params.set('revisionId', String(opts.revisionId));
	if (typeof opts?.includeArtifacts === 'boolean') params.set('include_artifacts', String(opts.includeArtifacts));
	if (typeof opts?.includeSchemas === 'boolean') params.set('include_schemas', String(opts.includeSchemas));
	const query = params.toString();
	const suffix = query ? `?${query}` : '';
	return await _fetchJson<ExportGraphPackageResponse>(`/api/graphs/${encodeURIComponent(gid)}/export${suffix}`);
}

export async function importGraphPackage(
	req: ImportGraphPackageRequest
): Promise<ImportGraphPackageResponse> {
	const res = await fetch('/api/graphs/import', {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(req)
	});
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`importGraphPackage failed: ${res.status} ${text}`);
	}
	return (await res.json()) as ImportGraphPackageResponse;
}
