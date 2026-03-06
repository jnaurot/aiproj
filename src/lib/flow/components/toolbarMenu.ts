export type ToolbarMenuItem = {
	id: string;
	label: string;
	disabled?: boolean;
	danger?: boolean;
};

export function firstEnabledIndex(items: ToolbarMenuItem[]): number {
	return items.findIndex((item) => !item.disabled);
}

export function nextEnabledIndex(
	items: ToolbarMenuItem[],
	current: number,
	direction: 1 | -1
): number {
	if (!items.length) return -1;
	let idx = current;
	for (let i = 0; i < items.length; i += 1) {
		idx = (idx + direction + items.length) % items.length;
		if (!items[idx]?.disabled) return idx;
	}
	return -1;
}
