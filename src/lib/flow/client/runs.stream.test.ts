import { afterEach, describe, expect, it, vi } from 'vitest';

import { streamRunEvents } from './runs';

type Handler = ((event: any) => void) | null;

class MockEventSource {
	static OPEN = 1;
	static CLOSED = 2;
	static last: MockEventSource | null = null;

	public onmessage: Handler = null;
	public onerror: Handler = null;
	public readyState = MockEventSource.OPEN;
	public url: string;

	constructor(url: string) {
		this.url = url;
		MockEventSource.last = this;
	}

	close() {
		this.readyState = MockEventSource.CLOSED;
	}

	emitMessage(payload: unknown) {
		this.onmessage?.({ data: JSON.stringify(payload) });
	}

	emitError(err: unknown = new Error('sse error')) {
		this.onerror?.(err);
	}
}

describe('streamRunEvents terminal handling', () => {
	const previousEventSource = (globalThis as any).EventSource;

	afterEach(() => {
		(globalThis as any).EventSource = previousEventSource;
		MockEventSource.last = null;
		vi.restoreAllMocks();
	});

	it('treats run_paused as terminal and suppresses onError follow-up', () => {
		(globalThis as any).EventSource = MockEventSource as any;
		const onEvent = vi.fn();
		const onError = vi.fn();

		streamRunEvents('run-123', onEvent, onError);
		const es = MockEventSource.last;
		expect(es).toBeTruthy();
		es!.emitMessage({ type: 'run_paused', runId: 'run-123' });
		es!.emitError(new Error('disconnect after pause'));

		expect(onEvent).toHaveBeenCalledTimes(1);
		expect(onError).not.toHaveBeenCalled();
		expect(es!.readyState).toBe(MockEventSource.CLOSED);
	});
});

