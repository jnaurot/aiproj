import { describe, expect, it } from 'vitest';

import {
	getArtifactConsumersUrl,
	getArtifactLineageUrl,
	getArtifactMetaUrl,
	getArtifactPreviewUrl,
	getArtifactUrl
} from './runs';

describe('artifact url builders', () => {
	it('always includes graphId for artifact endpoints', () => {
		const graphId = 'graph-A';
		expect(getArtifactUrl('art-1', graphId)).toBe('/runs/artifacts/art-1?graphId=graph-A');
		expect(getArtifactMetaUrl('art-1', graphId)).toBe('/runs/artifacts/art-1/meta?graphId=graph-A');
		expect(getArtifactConsumersUrl('art-1', graphId, 50)).toBe(
			'/runs/artifacts/art-1/consumers?graphId=graph-A&limit=50'
		);
		expect(getArtifactLineageUrl('art-1', graphId, 2)).toBe(
			'/runs/artifacts/art-1/lineage?graphId=graph-A&depth=2'
		);
		expect(getArtifactPreviewUrl('art-1', graphId, 20, 100)).toBe(
			'/runs/artifacts/art-1/preview?graphId=graph-A&offset=20&limit=100'
		);
	});

	it('uses the current graph id and url-encodes it', () => {
		const oldGraphId = 'graph old';
		const newGraphId = 'graph/new';
		expect(getArtifactUrl('a', oldGraphId)).toContain('graphId=graph+old');
		expect(getArtifactUrl('a', newGraphId)).toContain('graphId=graph%2Fnew');
		expect(getArtifactUrl('a', newGraphId)).not.toContain('graph+old');
	});

	it('throws when graphId is missing', () => {
		expect(() => getArtifactUrl('a', '')).toThrow(/graphId is required/i);
	});
});
