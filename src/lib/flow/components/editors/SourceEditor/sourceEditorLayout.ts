import type { SourceKind } from '$lib/flow/types/paramsMap';

export type SourceSectionId =
	| 'connection'
	| 'input'
	| 'parsing'
	| 'execution'
	| 'advanced'
	| 'debug';

export type SourceSectionDescriptor = {
	id: SourceSectionId;
	title: 'Connection' | 'Input' | 'Parsing' | 'Execution' | 'Advanced' | 'Debug';
	defaultOpen: boolean;
};

const CANONICAL_ORDER: SourceSectionId[] = [
	'connection',
	'input',
	'parsing',
	'execution',
	'advanced',
	'debug'
];

const TITLES: Record<SourceSectionId, SourceSectionDescriptor['title']> = {
	connection: 'Connection',
	input: 'Input',
	parsing: 'Parsing',
	execution: 'Execution',
	advanced: 'Advanced',
	debug: 'Debug'
};

const DEFAULT_OPEN: Record<SourceSectionId, boolean> = {
	connection: true,
	input: true,
	parsing: false,
	execution: true,
	advanced: false,
	debug: false
};

const KIND_VISIBILITY: Record<SourceKind, SourceSectionId[]> = {
	file: ['connection', 'input', 'parsing', 'execution', 'advanced', 'debug'],
	database: ['connection', 'input', 'execution', 'advanced', 'debug'],
	api: ['connection', 'input', 'parsing', 'execution', 'advanced', 'debug'],
	object_store: ['connection', 'input', 'parsing', 'execution', 'advanced', 'debug'],
	warehouse: ['connection', 'input', 'execution', 'advanced', 'debug']
};

export function sourceSectionLayoutForKind(sourceKind: SourceKind): SourceSectionDescriptor[] {
	const visible = new Set(KIND_VISIBILITY[sourceKind] ?? KIND_VISIBILITY.file);
	return CANONICAL_ORDER.filter((id) => visible.has(id)).map((id) => ({
		id,
		title: TITLES[id],
		defaultOpen: DEFAULT_OPEN[id]
	}));
}

export function defaultSourceSectionOpenState(sourceKind: SourceKind): Record<string, boolean> {
	const sections = sourceSectionLayoutForKind(sourceKind);
	const out: Record<string, boolean> = {};
	for (const section of sections) out[section.id] = section.defaultOpen;
	return out;
}

export function sourceFileFormatCollapsedGroups(format: string): string[] {
	const normalized = String(format ?? '').trim().toLowerCase();
	if (normalized === 'csv' || normalized === 'tsv') return ['csv_group'];
	if (normalized === 'json') return ['json_group'];
	if (normalized === 'excel') return ['excel_group'];
	if (normalized === 'pdf') return ['pdf_group'];
	if (['png', 'jpg', 'jpeg', 'webp', 'gif', 'svg', 'tiff', 'tif'].includes(normalized)) {
		return ['image_group'];
	}
	if (['mp3', 'wav', 'flac', 'ogg', 'm4a', 'aac'].includes(normalized)) return ['audio_group'];
	if (['mp4', 'mov', 'webm'].includes(normalized)) return ['video_group'];
	return ['generic_group'];
}

