import { describe, expect, it } from 'vitest';

import ComponentEditor from './ComponentEditor.svelte';

describe('ComponentEditor', () => {
	it('loads component editor module', () => {
		expect(ComponentEditor).toBeTruthy();
	});
});
