import { describe, expect, it } from 'vitest';

import { extractComponentWrapperOutputs } from './artifactWrapper';

describe('component wrapper output extraction', () => {
	it('extracts named output artifact refs from wrapper payload', () => {
		const payload = {
			ok: true,
			outputs: {
				out_data: {
					artifactId: 'a1',
					payloadType: 'text',
					mimeType: 'text/plain'
				},
				out_2: {
					artifactId: 'a2',
					payloadType: 'json',
					mimeType: 'application/json'
				}
			}
		};
		const refs = extractComponentWrapperOutputs(payload);
		expect(refs).toEqual([
			{ name: 'out_2', artifactId: 'a2', payloadType: 'json', mimeType: 'application/json' },
			{ name: 'out_data', artifactId: 'a1', payloadType: 'text', mimeType: 'text/plain' }
		]);
	});

	it('returns empty for non-wrapper payloads', () => {
		expect(extractComponentWrapperOutputs({ foo: 'bar' })).toEqual([]);
		expect(extractComponentWrapperOutputs(null)).toEqual([]);
	});
});
