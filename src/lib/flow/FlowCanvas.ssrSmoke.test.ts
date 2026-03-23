import { describe, expect, it } from 'vitest';
import { render } from 'svelte/server';

import FlowCanvas from './FlowCanvas.svelte';

describe('FlowCanvas SSR smoke', () => {
	it('renders without ReferenceError for connection handlers', () => {
		expect(() => render(FlowCanvas as any)).not.toThrow();
	});
});
