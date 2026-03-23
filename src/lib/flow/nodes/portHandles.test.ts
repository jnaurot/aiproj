import { describe, expect, it } from 'vitest';
import { inferPlaneFromHandleId, portHintText, resolveNodeHandles } from './portHandles';

describe('portHandles', () => {
	it('infers plane from handle id prefixes', () => {
		expect(inferPlaneFromHandleId('in')).toBe('work');
		expect(inferPlaneFromHandleId('param_config')).toBe('param');
		expect(inferPlaneFromHandleId('control_in')).toBe('control');
		expect(inferPlaneFromHandleId('ctl_signal')).toBe('control');
	});

	it('derives handles from port declarations with labels and planes', () => {
		const nodeData: any = {
			kind: 'model',
			label: 'Model',
			params: {},
			portDeclarations: {
				in: {
					in_data: { plane: 'work', label: 'Data' },
					param_prompt: { plane: 'param', label: 'Prompt Params' },
					control_in: { plane: 'control', label: 'Control' }
				},
				out: {
					out_main: { plane: 'work', label: 'Main Out' }
				}
			}
		};
		const inputs = resolveNodeHandles(nodeData, 'in', null, { type: 'json' });
		expect(inputs.map((h) => h.id)).toEqual(['in_data', 'param_prompt', 'control_in']);
		expect(inputs.map((h) => h.plane)).toEqual(['work', 'param', 'control']);
		expect(inputs.map((h) => h.label)).toEqual(['Data', 'Prompt Params', 'Control']);
	});

	it('preserves explicit handles and enriches with declared plane', () => {
		const nodeData: any = {
			kind: 'transform',
			label: 'X',
			params: {},
			portDeclarations: {
				out: {
					param_out: { plane: 'param' }
				}
			}
		};
		const out = resolveNodeHandles(nodeData, 'out', [{ id: 'param_out', label: 'Params' }], { type: 'json' });
		expect(out).toHaveLength(1);
		expect(out[0].id).toBe('param_out');
		expect(out[0].label).toBe('Params');
		expect(out[0].plane).toBe('param');
	});

	it('falls back to default in/out for legacy nodes without declarations', () => {
		const legacy: any = { kind: 'source', label: 'Legacy', params: {} };
		const inHandles = resolveNodeHandles(legacy, 'in', null, { type: 'json' });
		const outHandles = resolveNodeHandles(legacy, 'out', null, { type: 'json' });
		expect(inHandles.map((h) => h.id)).toEqual(['in']);
		expect(outHandles.map((h) => h.id)).toEqual(['out']);
		expect(inHandles[0].plane).toBe('work');
		expect(outHandles[0].plane).toBe('work');
	});

	it('maps declaration default to implicit in/out handles for compatibility', () => {
		const nodeData: any = {
			kind: 'model',
			label: 'Model',
			params: {},
			portDeclarations: {
				in: {
					default: { plane: 'param', label: 'Parameters' }
				},
				out: {
					default: { plane: 'work', label: 'Result' }
				}
			}
		};
		const inputs = resolveNodeHandles(nodeData, 'in', null, { type: 'json' });
		const outputs = resolveNodeHandles(nodeData, 'out', null, { type: 'json' });
		expect(inputs.map((h) => h.id)).toEqual(['in']);
		expect(outputs.map((h) => h.id)).toEqual(['out']);
		expect(inputs[0].label).toBe('Parameters');
		expect(inputs[0].plane).toBe('param');
		expect(outputs[0].label).toBe('Result');
		expect(outputs[0].plane).toBe('work');
	});

	it('formats port hover hints with role, label/id, and plane', () => {
		expect(portHintText('in', { id: 'param_context', label: 'Context', plane: 'param' })).toBe(
			'Input: Context (param_context) [param]'
		);
		expect(portHintText('out', { id: 'out', plane: 'work' })).toBe('Output: out [work]');
	});
});
