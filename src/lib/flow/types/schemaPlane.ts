export type SchemaPlaneColumnType =
	| 'string'
	| 'number'
	| 'boolean'
	| 'datetime'
	| 'binary'
	| 'tensor'
	| 'unknown';

export type SchemaPlaneMode = 'table' | 'tensor' | 'text' | 'binary' | 'model_artifact' | 'opaque';

export type AbstractProperties = {
	range?: [number, number] | null;
	normalized?: boolean;
	device?: 'cpu' | 'gpu' | 'any';
	dtype?: 'float32' | 'float16' | 'int32' | 'int64' | 'uint8';
	non_negative?: boolean;
	cardinality?: 'one' | 'many' | 'stream';
	class_set?: string[] | null;
	consume_once?: boolean;
	sample_rate?: number;
	architecture_signature?: string;
	[key: string]: unknown;
};

export type SchemaPlaneColumn = {
	name: string;
	type: SchemaPlaneColumnType;
	nullable: boolean;
	properties: AbstractProperties;
};

export type SchemaPlaneOutput = {
	mode: SchemaPlaneMode;
	columns: SchemaPlaneColumn[];
	shape?: Array<number | string>;
	dtype?: 'float32' | 'float16' | 'int32' | 'int64' | 'uint8';
	properties?: AbstractProperties;
	note?: string;
};

export type SchemaErrorCode =
	| 'SHAPE_MISMATCH'
	| 'TYPE_MISMATCH'
	| 'MISSING_REQUIRED_INPUT'
	| 'CARDINALITY_CONFLICT'
	| 'PROPERTY_VIOLATION'
	| 'OPAQUE_DEPENDENCY'
	| 'CYCLE_DETECTED';

export type SchemaError = {
	code: SchemaErrorCode;
	message: string;
	handles: string[];
};

export type SchemaPlaneResult =
	| { ok: true; output: SchemaPlaneOutput }
	| { ok: false; error: SchemaError; output?: SchemaPlaneOutput };

export type SchemaFunction = (
	inputs: readonly SchemaPlaneOutput[],
	params: Record<string, unknown>
) => SchemaPlaneResult;

export type SchemaPlaneState = {
	nodeSchemas: Record<string, SchemaPlaneResult>;
	edgeSchemas: Record<string, SchemaPlaneOutput>;
};

