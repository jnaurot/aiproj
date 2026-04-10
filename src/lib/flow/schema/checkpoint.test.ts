import { describe, expect, it } from 'vitest';
import {
	CheckpointExecutionHintsSchema,
	CheckpointRecordSchema,
	CheckpointRegistrySchema,
	MemoKeySchema
} from '$lib/flow/schema/checkpoint';

describe('checkpoint schema', () => {
	it('accepts a fully valid checkpoint record', () => {
		const parsed = CheckpointRecordSchema.parse({
			id: '123e4567-e89b-12d3-a456-426614174000',
			name: 'My checkpoint',
			description: 'saved result',
			nodeId: 'n1',
			graphId: 'g1',
			runId: 'r1',
			artifactId: 'a1',
			execKey: 'e1',
			fingerprintAtCreation: 'a'.repeat(64),
			createdAt: '2026-04-10T12:00:00.000Z',
			staleness: 'valid',
			outputs: {
				out: { artifactId: 'ao1', execKey: 'eo1' }
			}
		});
		expect(parsed.name).toBe('My checkpoint');
		expect(parsed.outputs?.out?.artifactId).toBe('ao1');
	});

	it('rejects empty name', () => {
		expect(() =>
			CheckpointRecordSchema.parse({
				id: '123e4567-e89b-12d3-a456-426614174000',
				name: '',
				nodeId: 'n1',
				graphId: 'g1',
				runId: 'r1',
				artifactId: 'a1',
				execKey: 'e1',
				fingerprintAtCreation: 'a'.repeat(64),
				createdAt: '2026-04-10T12:00:00.000Z',
				staleness: 'valid'
			})
		).toThrow();
	});

	it('rejects non-uuid id', () => {
		expect(() =>
			CheckpointRecordSchema.parse({
				id: 'not-a-uuid',
				name: 'ok',
				nodeId: 'n1',
				graphId: 'g1',
				runId: 'r1',
				artifactId: 'a1',
				execKey: 'e1',
				fingerprintAtCreation: 'a'.repeat(64),
				createdAt: '2026-04-10T12:00:00.000Z',
				staleness: 'valid'
			})
		).toThrow();
	});

	it('rejects 63-char memo key', () => {
		expect(() => MemoKeySchema.parse('a'.repeat(63))).toThrow();
	});

	it('rejects non-hex memo key', () => {
		expect(() => MemoKeySchema.parse('z'.repeat(64))).toThrow();
	});

	it('accepts empty registry', () => {
		expect(CheckpointRegistrySchema.parse({})).toEqual({});
	});

	it('roundtrips execution hints with outputs', () => {
		const parsed = CheckpointExecutionHintsSchema.parse({
			checkpoints: {
				n1: {
					artifactId: 'a1',
					execKey: 'e1',
					fingerprintAtCreation: 'b'.repeat(64),
					outputs: {
						summary: { artifactId: 'a2', execKey: 'e2' }
					}
				}
			}
		});
		expect(parsed.checkpoints.n1.outputs?.summary.artifactId).toBe('a2');
	});
});

