export type ComponentCatalogItem = {
	componentId: string;
	createdAt: string;
	updatedAt: string;
	latestRevisionId: string | null;
};

export type ComponentRevisionSummary = {
	revisionId: string;
	componentId: string;
	parentRevisionId?: string | null;
	createdAt: string;
	message?: string | null;
	schemaVersion: number;
	checksum: string;
};

export type ComponentTypedField = {
	name: string;
	type: string;
	nativeType?: string;
	nullable?: boolean;
};

export type ComponentTypedSchema = {
	type: string;
	fields?: ComponentTypedField[];
};

export type ComponentApiPort = {
	name: string;
	portType: string;
	required?: boolean;
	typedSchema?: ComponentTypedSchema;
};

export type ComponentApiContract = {
	inputs: ComponentApiPort[];
	outputs: ComponentApiPort[];
};

export type ComponentRevisionDefinition = {
	graph: {
		nodes: unknown[];
		edges: unknown[];
	};
	api: ComponentApiContract;
	configSchema?: Record<string, unknown>;
};

export type ComponentRevisionDetail = {
	schemaVersion: number;
	componentId: string;
	revisionId: string;
	parentRevisionId?: string | null;
	createdAt: string;
	message?: string | null;
	revisionSchemaVersion: number;
	checksum: string;
	definition: ComponentRevisionDefinition;
};

export type CreateComponentRevisionRequest = {
	componentId: string;
	revisionId?: string;
	parentRevisionId?: string;
	message?: string;
	schemaVersion?: number;
	graph: {
		nodes: unknown[];
		edges: unknown[];
	};
	api: ComponentApiContract;
	configSchema?: Record<string, unknown>;
};

export type CreateComponentRevisionResponse = {
	schemaVersion: number;
	componentId: string;
	revisionId: string;
	parentRevisionId?: string | null;
	createdAt: string;
	message?: string | null;
	checksum: string;
};

type ListComponentsResponse = {
	schemaVersion: number;
	components: ComponentCatalogItem[];
};

type ListComponentRevisionsResponse = {
	schemaVersion: number;
	componentId: string;
	revisions: ComponentRevisionSummary[];
};

async function _fetchJson<T>(url: string): Promise<T> {
	const res = await fetch(url);
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`${url} failed: ${res.status} ${text}`);
	}
	return (await res.json()) as T;
}

export async function listComponents(limit = 100, offset = 0): Promise<ComponentCatalogItem[]> {
	const q = `limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`;
	const res = await _fetchJson<ListComponentsResponse>(`/api/components?${q}`);
	return Array.isArray(res.components) ? res.components : [];
}

export async function listComponentRevisions(
	componentId: string,
	limit = 100,
	offset = 0
): Promise<ComponentRevisionSummary[]> {
	const cid = String(componentId ?? '').trim();
	if (!cid) return [];
	const q = `limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`;
	const res = await _fetchJson<ListComponentRevisionsResponse>(
		`/api/components/${encodeURIComponent(cid)}/revisions?${q}`
	);
	return Array.isArray(res.revisions) ? res.revisions : [];
}

export async function getComponentRevision(
	componentId: string,
	revisionId: string
): Promise<ComponentRevisionDetail> {
	const cid = String(componentId ?? '').trim();
	const rid = String(revisionId ?? '').trim();
	if (!cid) throw new Error('componentId is required');
	if (!rid) throw new Error('revisionId is required');
	return await _fetchJson<ComponentRevisionDetail>(
		`/api/components/${encodeURIComponent(cid)}/revisions/${encodeURIComponent(rid)}`
	);
}

export async function createComponentRevision(
	req: CreateComponentRevisionRequest
): Promise<CreateComponentRevisionResponse> {
	const body = {
		componentId: String(req.componentId ?? '').trim(),
		revisionId: req.revisionId ? String(req.revisionId).trim() : undefined,
		parentRevisionId: req.parentRevisionId ? String(req.parentRevisionId).trim() : undefined,
		message: req.message ?? '',
		schemaVersion: Number(req.schemaVersion ?? 1),
		graph: req.graph,
		api: req.api,
		configSchema: req.configSchema ?? {}
	};
	if (!body.componentId) {
		throw new Error('componentId is required');
	}
	const res = await fetch('/api/components', {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify(body)
	});
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`createComponentRevision failed: ${res.status} ${text}`);
	}
	return (await res.json()) as CreateComponentRevisionResponse;
}
