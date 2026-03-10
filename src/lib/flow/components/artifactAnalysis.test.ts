import { describe, expect, it } from 'vitest';

import { extractAnalysisArtifacts } from './artifactAnalysis';

describe('analysis artifact extraction', () => {
	it('extracts typed analysis artifacts from payload', () => {
		const payload = {
			analysis_artifacts: [
				{
					name: 'feature_importance',
					kind: 'table',
					description: 'Feature importance scores.',
					row_count: 3,
					typed_schema: {
						type: 'table',
						fields: [
							{ name: 'feature', type: 'string', nullable: false },
							{ name: 'importance', type: 'float', nullable: false }
						]
					}
				},
				{
					name: 'confusion_matrix',
					kind: 'table',
					description: 'Confusion matrix.',
					row_count: 4,
					typed_schema: {
						type: 'table',
						fields: [
							{ name: 'actual', type: 'string', nullable: false },
							{ name: 'predicted', type: 'string', nullable: false },
							{ name: 'count', type: 'int', nullable: false }
						]
					}
				}
			]
		};

		expect(extractAnalysisArtifacts(payload)).toEqual([
			{
				name: 'feature_importance',
				kind: 'table',
				description: 'Feature importance scores.',
				rowCount: 3,
				fields: [
					{ name: 'feature', type: 'string', nullable: false },
					{ name: 'importance', type: 'float', nullable: false }
				]
			},
			{
				name: 'confusion_matrix',
				kind: 'table',
				description: 'Confusion matrix.',
				rowCount: 4,
				fields: [
					{ name: 'actual', type: 'string', nullable: false },
					{ name: 'predicted', type: 'string', nullable: false },
					{ name: 'count', type: 'int', nullable: false }
				]
			}
		]);
	});

	it('ignores malformed analysis artifacts', () => {
		const payload = {
			analysis_artifacts: [
				null,
				{ name: '' },
				{ name: 'ok', kind: 'table', row_count: 1, typed_schema: { fields: [{ type: 'x' }, 1] } }
			]
		};

		expect(extractAnalysisArtifacts(payload)).toEqual([
			{ name: 'ok', kind: 'table', description: '', rowCount: 1, fields: [] }
		]);
		expect(extractAnalysisArtifacts(null)).toEqual([]);
		expect(extractAnalysisArtifacts({})).toEqual([]);
	});
});
