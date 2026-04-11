import { describe, expect, it } from 'vitest';
import { schemaFn_source } from './source';

describe('schemaFn_source', () => {
	it('derives table schema from priming sample', () => {
		const result = schemaFn_source([], {
			sourceKind: 'file',
			priming: {
				sample_schema: {
					fields: [
						{ name: 'a', type: 'number' },
						{ name: 'b', type: 'string' }
					]
				}
			}
		});
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.output.columns.map((c) => c.name)).toEqual(['a', 'b']);
	});

	it('returns opaque output when no priming available', () => {
		const result = schemaFn_source([], { sourceKind: 'file' });
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.output.mode).toBe('opaque');
	});

	it('stream source carries stream cardinality + consume_once', () => {
		const result = schemaFn_source([], { sourceKind: 'stream' });
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.output.properties?.cardinality).toBe('stream');
			expect(result.output.properties?.consume_once).toBe(true);
		}
	});
});

