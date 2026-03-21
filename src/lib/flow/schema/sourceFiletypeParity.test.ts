import { describe, expect, it } from 'vitest';

import fixture from '../../../../shared/test_fixtures/source_filetype_parity.v1.json';
import { SourceFileParamsSchema } from './source';

describe('source filetype parity fixture', () => {
	it('matches frontend default output mode derivation for all file formats', () => {
		for (const c of fixture.cases) {
			const parsed = SourceFileParamsSchema.parse({
				rel_path: '.',
				filename: `data.${c.file_format}`,
				file_format: c.file_format
			});
			expect(parsed.output?.mode).toBe(c.expected_output);
		}
	});

	it('has unique file formats to prevent snapshot drift', () => {
		const formats = fixture.cases.map((c) => c.file_format);
		const unique = new Set(formats);
		expect(unique.size).toBe(formats.length);
	});
});
