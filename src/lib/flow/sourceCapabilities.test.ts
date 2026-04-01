import { describe, expect, it } from 'vitest';

import {
	buildSourceCapabilityNotices,
	resolveSourceCapabilityDescriptor,
	type SourceCapabilityDescriptor
} from '$lib/flow/sourceCapabilities';

describe('source capability descriptors', () => {
	it('resolves support level from capability matrix', () => {
		const caps = {
			kindCapabilities: {
				object_store: {
					supportLevel: 'preview',
					notes: ['Provider-backed object store reads are preview.']
				}
			}
		};
		const resolved = resolveSourceCapabilityDescriptor('object_store', caps as any);
		expect(resolved.supportLevel).toBe('preview');
		expect(resolved.notes).toContain('Provider-backed object store reads are preview.');
	});

	it('builds notices for preview and object_store mock mode', () => {
		const descriptor: SourceCapabilityDescriptor = {
			sourceKind: 'object_store',
			supportLevel: 'preview',
			notes: ['Preview note']
		};
		const notices = buildSourceCapabilityNotices(descriptor, { object_store_mode: 'mock' });
		expect(notices.some((n) => n.toLowerCase().includes('preview capability'))).toBe(true);
		expect(notices.some((n) => n.toLowerCase().includes('mock mode'))).toBe(true);
		expect(notices).toContain('Preview note');
	});
});
