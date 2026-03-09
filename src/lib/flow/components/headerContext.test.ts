import { describe, expect, it } from 'vitest';

import { buildHeaderContextLabels } from './headerContext';

describe('buildHeaderContextLabels', () => {
	it('formats graph context labels', () => {
		expect(
			buildHeaderContextLabels({
				editingContext: 'graph',
				graphName: 'ReaderFreJap',
				componentName: 'Loadw2'
			})
		).toEqual({
			scopeLabel: 'Graph ReaderFreJap',
			breadcrumbLabel: 'Graph ReaderFreJap'
		});
	});

	it('formats component context labels with graph breadcrumb', () => {
		expect(
			buildHeaderContextLabels({
				editingContext: 'component',
				graphName: 'ReaderFreJap',
				componentName: 'Loadw2'
			})
		).toEqual({
			scopeLabel: 'Graph ReaderFreJap / Loadw2',
			breadcrumbLabel: 'Graph ReaderFreJap / Loadw2'
		});
	});

	it('applies fallback names when values are blank', () => {
		expect(
			buildHeaderContextLabels({
				editingContext: 'component',
				graphName: '   ',
				componentName: ''
			})
		).toEqual({
			scopeLabel: 'Graph unnamed / unknown',
			breadcrumbLabel: 'Graph unnamed / unknown'
		});
	});
});
