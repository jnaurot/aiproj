export type ComponentExitDecision = 'save' | 'discard' | 'cancel';

export function parseComponentExitDecision(raw: string | null | undefined): ComponentExitDecision {
	const normalized = String(raw ?? '')
		.trim()
		.toLowerCase();
	if (normalized === '1' || normalized === 'save' || normalized === 's') return 'save';
	if (normalized === '2' || normalized === 'discard' || normalized === 'd') return 'discard';
	return 'cancel';
}

