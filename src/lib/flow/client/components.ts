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

export type ComponentTypedField = CanonicalComponentTypedField;
export type ComponentTypedSchema = CanonicalComponentTypedSchema;

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
	componentSchemaVersion?: number;
	componentId: string;
	revisionId: string;
	parentRevisionId?: string | null;
	createdAt: string;
	message?: string | null;
	revisionSchemaVersion: number;
	checksum: string;
	definition: ComponentRevisionDefinition;
	contractSnapshot?: ComponentApiContract;
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
	componentSchemaVersion?: number;
	componentId: string;
	revisionId: string;
	parentRevisionId?: string | null;
	createdAt: string;
	message?: string | null;
	checksum: string;
	migrationNotes?: Array<Record<string, unknown>>;
};

export type ComponentValidationDiagnostic = {
	code: string;
	path: string;
	message: string;
	severity: 'error' | 'warning';
};

export type ValidateComponentRevisionRequest = {
	graph: {
		nodes: unknown[];
		edges: unknown[];
	};
	api: ComponentApiContract;
	configSchema?: Record<string, unknown>;
	schemaVersion?: number;
};

export type ValidateComponentRevisionResponse = {
	schemaVersion: number;
	componentSchemaVersion: number;
	ok: boolean;
	diagnostics: ComponentValidationDiagnostic[];
	migrationNotes: Array<Record<string, unknown>>;
	normalizedDefinition: ComponentRevisionDefinition;
};

export type RenameComponentResponse = {
	schemaVersion: number;
	componentId: string;
	renamedFrom: string;
};

export type DeleteComponentResponse = {
	schemaVersion: number;
	componentId: string;
	deletedRevisions: number;
	deletedComponents: number;
};

export type DeleteComponentRevisionResponse = {
	schemaVersion: number;
	componentId: string;
	revisionId: string;
	deletedRevisions: number;
	remainingLatestRevisionId: string | null;
	componentDeleted: boolean;
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

export async function validateComponentRevision(
	req: ValidateComponentRevisionRequest
): Promise<ValidateComponentRevisionResponse> {
	const res = await fetch('/api/components/validate', {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({
			graph: req.graph,
			api: req.api,
			configSchema: req.configSchema ?? {},
			schemaVersion: Number(req.schemaVersion ?? 1)
		})
	});
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`validateComponentRevision failed: ${res.status} ${text}`);
	}
	return (await res.json()) as ValidateComponentRevisionResponse;
}

export async function renameComponent(
	componentId: string,
	nextComponentId: string
): Promise<RenameComponentResponse> {
	const cid = String(componentId ?? '').trim();
	const next = String(nextComponentId ?? '').trim();
	if (!cid) throw new Error('componentId is required');
	if (!next) throw new Error('nextComponentId is required');
	const res = await fetch(`/api/components/${encodeURIComponent(cid)}`, {
		method: 'PATCH',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ componentId: next })
	});
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`renameComponent failed: ${res.status} ${text}`);
	}
	return (await res.json()) as RenameComponentResponse;
}

export async function deleteComponent(componentId: string): Promise<DeleteComponentResponse> {
	const cid = String(componentId ?? '').trim();
	if (!cid) throw new Error('componentId is required');
	const res = await fetch(`/api/components/${encodeURIComponent(cid)}`, {
		method: 'DELETE'
	});
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`deleteComponent failed: ${res.status} ${text}`);
	}
	return (await res.json()) as DeleteComponentResponse;
}

export async function deleteComponentRevision(
	componentId: string,
	revisionId: string
): Promise<DeleteComponentRevisionResponse> {
	const cid = String(componentId ?? '').trim();
	const rid = String(revisionId ?? '').trim();
	if (!cid) throw new Error('componentId is required');
	if (!rid) throw new Error('revisionId is required');
	const res = await fetch(
		`/api/components/${encodeURIComponent(cid)}/revisions/${encodeURIComponent(rid)}`,
		{ method: 'DELETE' }
	);
	if (!res.ok) {
		const text = await res.text().catch(() => '');
		throw new Error(`deleteComponentRevision failed: ${res.status} ${text}`);
	}
	return (await res.json()) as DeleteComponentRevisionResponse;
}
import type {
	ComponentTypedField as CanonicalComponentTypedField,
	ComponentTypedSchema as CanonicalComponentTypedSchema
} from '$lib/flow/schema/component';
