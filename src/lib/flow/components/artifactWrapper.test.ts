import { describe, expect, it } from 'vitest';

import { extractComponentWrapperOutputs } from './artifactWrapper';

describe('component wrapper output extraction', () => {
	it('extracts named output artifact refs from wrapper payload', () => {
		const payload = {
			ok: true,
			outputs: {
				out_data: {
					artifact_id: 'a1',
					port_type: 'text',
					mime_type: 'text/plain'
				},
				out_2: {
					artifact_id: 'a2',
					port_type: 'json',
					mime_type: 'application/json'
				}
			}
		};
		const refs = extractComponentWrapperOutputs(payload);
		expect(refs).toEqual([
			{ name: 'out_2', artifactId: 'a2', portType: 'json', mimeType: 'application/json' },
			{ name: 'out_data', artifactId: 'a1', portType: 'text', mimeType: 'text/plain' }
		]);
	});

	it('returns empty for non-wrapper payloads', () => {
		expect(extractComponentWrapperOutputs({ foo: 'bar' })).toEqual([]);
		expect(extractComponentWrapperOutputs(null)).toEqual([]);
	});
});
