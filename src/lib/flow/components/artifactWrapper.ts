export type ComponentWrapperOutputRef = {
	name: string;
	artifactId: string;
	payloadType: string;
	mimeType: string;
};

export function extractComponentWrapperOutputs(value: unknown): ComponentWrapperOutputRef[] {
	if (!value || typeof value !== 'object') return [];
	const outputs = (value as any)?.outputs;
	if (!outputs || typeof outputs !== 'object') return [];
	const refs: ComponentWrapperOutputRef[] = [];
	for (const [nameRaw, detailsRaw] of Object.entries(outputs as Record<string, unknown>)) {
		const name = String(nameRaw ?? '').trim();
		const details = (detailsRaw ?? {}) as Record<string, unknown>;
		const artifactId = String(details?.artifactId ?? '').trim();
		if (!name || !artifactId) continue;
		refs.push({
			name,
			artifactId,
			payloadType: String(details?.payloadType ?? 'unknown').trim() || 'unknown',
			mimeType: String(details?.mimeType ?? '-').trim() || '-'
		});
	}
	return refs.sort((a, b) => a.name.localeCompare(b.name));
}
