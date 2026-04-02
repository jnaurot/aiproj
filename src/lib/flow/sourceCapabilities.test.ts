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

	it('test_source_editor_shows_capability_badge_per_kind', () => {
		const kinds = ['file', 'database', 'api', 'object_store', 'warehouse'] as const;
		for (const kind of kinds) {
			const descriptor = resolveSourceCapabilityDescriptor(kind);
			expect(descriptor.sourceKind).toBe(kind);
			expect(['production', 'preview', 'mock_only']).toContain(descriptor.supportLevel);
		}
	});

	it('test_source_editor_shows_mock_only_notice', () => {
		const descriptor: SourceCapabilityDescriptor = {
			sourceKind: 'api',
			supportLevel: 'mock_only',
			notes: []
		};
		const notices = buildSourceCapabilityNotices(descriptor, {});
		expect(notices.some((n) => n.toLowerCase().includes('mock-only capability'))).toBe(true);
	});

	it('test_source_editor_shows_preview_notice', () => {
		const descriptor: SourceCapabilityDescriptor = {
			sourceKind: 'warehouse',
			supportLevel: 'preview',
			notes: []
		};
		const notices = buildSourceCapabilityNotices(descriptor, {});
		expect(notices.some((n) => n.toLowerCase().includes('preview capability'))).toBe(true);
	});
});
