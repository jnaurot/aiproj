import { describe, expect, it } from 'vitest';

import { NodeSchemaEnvelopeSchema } from './schemaContract';

describe('schemaContract port contracts', () => {
	it('accepts input contract classes with defaults and handles', () => {
		const parsed = NodeSchemaEnvelopeSchema.parse({
			workInputs: {
				defaultSchema: {
					source: 'declared',
					state: 'fresh',
					typedSchema: { type: 'json', fields: [] }
				},
				handles: {
					in: {
						source: 'declared',
						state: 'fresh',
						typedSchema: { type: 'json', fields: [] }
					}
				}
			},
			paramInputs: {
				handles: {
					param_config: {
						source: 'declared',
						state: 'fresh',
						typedSchema: { type: 'json', fields: [] }
					}
				}
			},
			controlInputs: {
				handles: {
					control_in: {
						source: 'declared',
						state: 'fresh',
						typedSchema: { type: 'unknown', fields: [] }
					}
				}
			}
		});

		expect(parsed.workInputs?.handles?.in?.typedSchema?.type).toBe('json');
		expect(parsed.paramInputs?.handles?.param_config?.typedSchema?.type).toBe('json');
		expect(parsed.controlInputs?.handles?.control_in?.typedSchema?.type).toBe('unknown');
	});
});
