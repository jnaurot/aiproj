export type NodeDocTooltipSnapshot = {
	open: boolean;
	expanded: boolean;
};

export type NodeDocTooltipState = {
	enter: () => void;
	leave: () => void;
	keydown: (key: string) => void;
	destroy: () => void;
	snapshot: () => NodeDocTooltipSnapshot;
};

export type NodeDocTooltipStateOptions = {
	openDelayMs?: number;
	expandDelayMs?: number;
	schedule?: (fn: () => void, ms: number) => ReturnType<typeof setTimeout>;
	cancel?: (timer: ReturnType<typeof setTimeout>) => void;
	onChange?: (next: NodeDocTooltipSnapshot) => void;
};

export function createNodeDocTooltipState(options: NodeDocTooltipStateOptions = {}): NodeDocTooltipState {
	const openDelayMs = Math.max(0, Number(options.openDelayMs ?? 500));
	const expandDelayMs = Math.max(0, Number(options.expandDelayMs ?? 1200));
	const schedule = options.schedule ?? ((fn, ms) => setTimeout(fn, ms));
	const cancel = options.cancel ?? ((timer) => clearTimeout(timer));
	let openTimer: ReturnType<typeof setTimeout> | null = null;
	let expandTimer: ReturnType<typeof setTimeout> | null = null;
	let state: NodeDocTooltipSnapshot = { open: false, expanded: false };

	const emit = () => {
		options.onChange?.({ ...state });
	};

	const clearTimers = () => {
		if (openTimer) {
			cancel(openTimer);
			openTimer = null;
		}
		if (expandTimer) {
			cancel(expandTimer);
			expandTimer = null;
		}
	};

	const enter = () => {
		clearTimers();
		openTimer = schedule(() => {
			state = { ...state, open: true };
			emit();
		}, openDelayMs);
		expandTimer = schedule(() => {
			state = { open: true, expanded: true };
			emit();
		}, expandDelayMs);
	};

	const leave = () => {
		clearTimers();
		if (!state.open && !state.expanded) return;
		state = { open: false, expanded: false };
		emit();
	};

	const keydown = (keyRaw: string) => {
		const key = String(keyRaw ?? '');
		if (key === 'Escape') {
			leave();
			return;
		}
		if (key === '?' && state.open) {
			if (!state.expanded) {
				state = { ...state, expanded: true };
				emit();
			}
		}
	};

	const destroy = () => {
		clearTimers();
	};

	const snapshot = (): NodeDocTooltipSnapshot => ({ ...state });

	return { enter, leave, keydown, destroy, snapshot };
}
