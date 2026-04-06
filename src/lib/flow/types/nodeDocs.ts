export type NodeDocPlaneKind = 'data' | 'control' | 'param';
export type NodeDocExplanationMode = 'default' | 'llm';
export type NodeDocTrainingMode = 'off' | 'on';

export type NodeDocPortRef = {
	handle: string;
	plane: NodeDocPlaneKind;
	direction: 'in' | 'out';
	cardinality?: 'one' | 'many';
	required?: boolean;
	item_mode?: 'artifact' | 'json_items' | 'table_rows';
};

export type NodeDocPlaneSection = {
	title: string;
	summary: string;
	ports?: NodeDocPortRef[];
	notes?: string[];
};

export type NodeDocExample = {
	label: string;
	input?: string;
	output?: string;
};

export type NodeDocV1 = {
	schema_version: 1;
	node_kind: 'source' | 'transform' | 'model' | 'tool' | 'component';
	subtype?: string;
	title: string;
	summary: string;
	planes: {
		data: NodeDocPlaneSection;
		control: NodeDocPlaneSection;
		param: NodeDocPlaneSection;
	};
	examples?: NodeDocExample[];
	see_also?: string[];
};

export type NodeDocOverride = {
	summary?: string;
	notes?: string[];
	disabled?: boolean;
};

export type NodeDocGeneratedProviderMeta = {
	provider?: string;
	model?: string;
};

export type NodeDocGeneratedExplanation = {
	summary: string;
	settings_explained: string[];
	context_notes: string[];
	generated_at: string;
	signature_key: string;
	provider_meta?: NodeDocGeneratedProviderMeta;
};
