import type { SchemaFunction, SchemaPlaneColumn } from '$lib/flow/types/schemaPlane';
import { OPAQUE_SCHEMA } from '$lib/flow/schema/schemaRegistry';

export const schemaFn_training_job: SchemaFunction = (inputs, params) => {
	const train = inputs[0];
	const val = inputs[1];
	if (!train || !val) {
		return {
			ok: false,
			error: {
				code: 'MISSING_REQUIRED_INPUT',
				message: 'Training job requires train and validation inputs',
				handles: ['train', 'validation']
			}
		};
	}
	if (train.mode !== 'tensor' && train.mode !== 'table') {
		return {
			ok: false,
			error: {
				code: 'TYPE_MISMATCH',
				message: 'Training input must be tensor or table',
				handles: ['train']
			}
		};
	}
	const expectedInputDim = Number((params as any)?.input_dim ?? (params as any)?.inputDim ?? NaN);
	const providedDim = Array.isArray(train.shape) && typeof train.shape[train.shape.length - 1] === 'number'
		? Number(train.shape[train.shape.length - 1])
		: NaN;
	if (Number.isFinite(expectedInputDim) && Number.isFinite(providedDim) && expectedInputDim !== providedDim) {
		return {
			ok: false,
			error: {
				code: 'SHAPE_MISMATCH',
				message: `Expected input_dim ${expectedInputDim}, got ${providedDim}`,
				handles: ['train']
			}
		};
	}
	const numClasses = Number((params as any)?.num_classes ?? (params as any)?.numClasses ?? 0);
	return {
		ok: true,
		output: {
			mode: 'model_artifact',
			columns: [],
			properties: {
				architecture_signature: String((params as any)?.architecture_signature ?? 'model'),
				dtype: train.dtype ?? train.properties?.dtype,
				class_set:
					Array.isArray((params as any)?.class_set) && (params as any).class_set.length > 0
						? (params as any).class_set
						: Number.isFinite(numClasses) && numClasses > 0
							? Array.from({ length: numClasses }, (_, i) => `class_${i}`)
							: null
			}
		}
	};
};

export const schemaFn_evaluation: SchemaFunction = (inputs, params) => {
	const input = inputs[0];
	if (!input) {
		return {
			ok: false,
			error: { code: 'MISSING_REQUIRED_INPUT', message: 'Evaluation requires model/input artifact', handles: ['in'] }
		};
	}
	const expectedClasses = Number((params as any)?.num_classes ?? (params as any)?.numClasses ?? NaN);
	const classSetLen = Array.isArray(input.properties?.class_set) ? input.properties.class_set.length : NaN;
	if (Number.isFinite(expectedClasses) && Number.isFinite(classSetLen) && expectedClasses !== classSetLen) {
		return {
			ok: false,
			error: {
				code: 'PROPERTY_VIOLATION',
				message: `Expected ${expectedClasses} classes but got ${classSetLen}`,
				handles: ['in']
			}
		};
	}
	const metrics = Array.isArray((params as any)?.metrics_config) ? (params as any).metrics_config : [];
	const columns: SchemaPlaneColumn[] = metrics
		.map((metric: any) => String(metric?.name ?? metric ?? '').trim())
		.filter((name: string) => name.length > 0)
		.map((name: string) => ({ name, type: 'number', nullable: true, properties: {} }));
	return {
		ok: true,
		output: {
			mode: 'table',
			columns: columns.length > 0 ? columns : [{ name: 'metric', type: 'number', nullable: true, properties: {} }]
		}
	};
};

export const schemaFn_ml_fallback: SchemaFunction = () => ({ ok: true, output: OPAQUE_SCHEMA });

