import { beforeEach, afterEach, describe, expect, it } from 'vitest';
import { get } from 'svelte/store';
import { nodePresetStore } from '$lib/flow/store/nodePresetStore';
import type { PipelineNodeData } from '$lib/flow/types';

class MemoryStorage {
	private data = new Map<string, string>();

	getItem(key: string): string | null {
		return this.data.has(key) ? this.data.get(key)! : null;
	}

	setItem(key: string, value: string): void {
		this.data.set(key, value);
	}

	removeItem(key: string): void {
		this.data.delete(key);
	}

	clear(): void {
		this.data.clear();
	}
}

const storage = new MemoryStorage();
const originalWindow = (globalThis as any).window;

function sourceNodeData(params: Record<string, unknown>): PipelineNodeData {
	return {
		kind: 'source',
		label: 'Source',
		sourceKind: 'file',
		ports: { in: null, out: 'table' },
		params
	} as PipelineNodeData;
}

describe('nodePresetStore', () => {
	beforeEach(() => {
		storage.clear();
		(globalThis as any).window = { localStorage: storage };
		nodePresetStore.reload();
	});

	afterEach(() => {
		(globalThis as any).window = originalWindow;
	});

	it('does not persist authored ports in preset payload', () => {
		const result = nodePresetStore.upsertFromNodeData(sourceNodeData({ file_format: 'csv' }), 'Preset A');
		expect(result.ok).toBe(true);
		const all = get(nodePresetStore);
		expect(all).toHaveLength(1);
		expect((all[0] as any).ports).toBeUndefined();
	});

	it('treats same kind/subtype/params as identical even if incoming ports differ', () => {
		const first = nodePresetStore.upsertFromNodeData(sourceNodeData({ file_format: 'csv' }), 'Preset A');
		expect(first.ok).toBe(true);
		const secondData = {
			...sourceNodeData({ file_format: 'csv' }),
			ports: { in: null, out: 'text' }
		} as PipelineNodeData;
		const second = nodePresetStore.upsertFromNodeData(secondData, 'Preset B');
		expect(second.ok).toBe(false);
		if (!second.ok) expect(second.error).toBe('identical_preset_exists');
	});
});

