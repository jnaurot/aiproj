export type HeaderContextInput = {
	editingContext: 'graph' | 'component';
	graphName?: string | null;
	componentName?: string | null;
};

export type HeaderContextLabels = {
	scopeLabel: string;
	breadcrumbLabel: string;
};

function normalizeName(value: unknown, fallback: string): string {
	const text = String(value ?? '').trim();
	return text.length > 0 ? text : fallback;
}

export function buildHeaderContextLabels(input: HeaderContextInput): HeaderContextLabels {
	const graphName = normalizeName(input.graphName, 'unnamed');
	const componentName = normalizeName(input.componentName, 'unknown');
	if (input.editingContext === 'component') {
		const componentScope = `Graph ${graphName} / ${componentName}`;
		return {
			scopeLabel: componentScope,
			breadcrumbLabel: componentScope
		};
	}
	return {
		scopeLabel: `Graph ${graphName}`,
		breadcrumbLabel: `Graph ${graphName}`
	};
}
