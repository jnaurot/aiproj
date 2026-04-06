import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';
import { graphStore } from '$lib/flow/store/graphStore';
import { buildNodeDocLlmContext, buildNodeDocLlmContextSignature } from './nodeDocLlmContext';

describe('nodeDocLlmContext', () => {
	it('extracts source file context including filename', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		graphStore.setSourceKind(sourceId, 'file');
		graphStore.updateNodeConfig(sourceId, {
			params: { file: { name: 'resume.txt' } }
		});
		const state = get(graphStore as any);
		const context = buildNodeDocLlmContext(state as any, sourceId);
		expect(context?.node_kind).toBe('source');
		expect(context?.settings.source_kind).toBe('file');
		expect(Object.keys(context?.settings ?? {}).length).toBeGreaterThan(0);
	});

	it('extracts source api context with url and method', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		graphStore.setSourceKind(sourceId, 'api');
		graphStore.updateNodeConfig(sourceId, {
			params: { url: 'https://example.com/jobs', method: 'POST' }
		});
		const state = get(graphStore as any);
		const context = buildNodeDocLlmContext(state as any, sourceId);
		expect(context?.settings.api_url).toContain('example.com');
		expect(context?.settings.api_method).toBe('POST');
	});

	it('extracts model context with provider and model details', () => {
		graphStore.hardResetGraph();
		const modelId = graphStore.addNode('model', { x: 0, y: 0 });
		graphStore.updateNodeConfig(modelId, {
			params: {
				user_prompt: 'Given a job row and preferences, score match quality.',
				debug: { enabled: true }
			}
		});
		const state = get(graphStore as any);
		const context = buildNodeDocLlmContext(state as any, modelId);
		expect(context?.node_kind).toBe('model');
		expect(String(context?.settings.provider ?? '').length).toBeGreaterThan(0);
		expect(String(context?.settings.user_prompt ?? '')).toContain('score match quality');
		expect((context?.settings as any).debug_enabled).toBeUndefined();
	});

	it('extracts transform context with operation details', () => {
		graphStore.hardResetGraph();
		const transformId = graphStore.addNode('transform', { x: 0, y: 0 });
		graphStore.setTransformKind(transformId, 'json_filter');
		graphStore.updateNodeConfig(transformId, {
			params: { op: 'json_filter' }
		});
		const state = get(graphStore as any);
		const context = buildNodeDocLlmContext(state as any, transformId);
		expect(context?.settings.transform_kind).toContain('json_filter');
	});

	it('changes signature when explanation-relevant settings change', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		graphStore.setSourceKind(sourceId, 'file');
		const state1 = get(graphStore as any);
		const context1 = buildNodeDocLlmContext(state1 as any, sourceId);
		const sig1 = buildNodeDocLlmContextSignature(context1);
		graphStore.setSourceKind(sourceId, 'api');
		graphStore.updateNodeConfig(sourceId, {
			params: { url: 'https://api.example.com' }
		});
		const state2 = get(graphStore as any);
		const context2 = buildNodeDocLlmContext(state2 as any, sourceId);
		const sig2 = buildNodeDocLlmContextSignature(context2);
		expect(sig2).not.toBe(sig1);
	});
});
