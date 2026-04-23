import { describe, expect, it } from 'vitest';

import { projectNodeDisplayState } from './displayState';

describe('projectNodeDisplayState', () => {
	it('maps runtime running states to display running', () => {
		expect(projectNodeDisplayState({ status: 'running' }, 'running')).toBe('running');
		expect(projectNodeDisplayState({ status: 'active' }, 'active')).toBe('running');
	});

	it('maps blocked/paused/busy runtime states to display busy', () => {
		expect(projectNodeDisplayState({ status: 'blocked' }, 'blocked')).toBe('busy');
		expect(projectNodeDisplayState({ status: 'paused' }, 'paused')).toBe('busy');
		expect(projectNodeDisplayState({ status: 'busy' }, 'busy')).toBe('busy');
	});

	it('maps failed and canceled directly', () => {
		expect(projectNodeDisplayState({ status: 'failed' }, 'failed')).toBe('failed');
		expect(projectNodeDisplayState({ status: 'canceled' }, 'canceled')).toBe('canceled');
	});

	it('maps skipped directly', () => {
		expect(projectNodeDisplayState({ status: 'skipped' }, 'skipped')).toBe('skipped');
	});

	it('maps stale from explicit stale runtime status', () => {
		expect(projectNodeDisplayState({ status: 'stale' }, 'stale')).toBe('stale');
		// isUpToDate: false alone no longer drives staleness under the three-state model.
		// Only runtime === 'stale' or exec-key drift triggers stale.
		expect(projectNodeDisplayState({ status: 'succeeded_up_to_date', isUpToDate: false })).toBe('succeeded');
	});

	it('maps succeeded states from runtime or artifact fallback', () => {
		expect(projectNodeDisplayState({ status: 'succeeded_up_to_date', isUpToDate: true })).toBe('succeeded');
		expect(projectNodeDisplayState({ currentArtifactId: 'art-1' })).toBe('succeeded');
	});

	it('maps mismatched exec key pair to stale', () => {
		expect(
			projectNodeDisplayState({
				status: 'succeeded_up_to_date',
				current: { execKey: 'next', artifactId: 'a2' },
				last: { execKey: 'prev', artifactId: 'a1' }
			})
		).toBe('stale');
	});
});
