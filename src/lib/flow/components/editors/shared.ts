export function asString(value: unknown, fallback = ''): string {
	return typeof value === 'string' ? value : fallback;
}

export function asBoolean(value: unknown, fallback: boolean): boolean {
	return typeof value === 'boolean' ? value : fallback;
}

export function asNumber(value: unknown, fallback: number): number {
	return typeof value === 'number' && Number.isFinite(value) ? value : fallback;
}

export function asNumberOrEmpty(value: unknown): string {
	return typeof value === 'number' && Number.isFinite(value) ? String(value) : '';
}

export function parseOptionalInt(raw: string, minimum?: number): number | undefined {
	if (raw.trim() === '') return undefined;
	const parsed = Number.parseInt(raw, 10);
	if (!Number.isFinite(parsed)) return undefined;
	if (minimum !== undefined && parsed < minimum) return minimum;
	return parsed;
}

export function parseOptionalFloat(raw: string, minimum?: number, maximum?: number): number | undefined {
	if (raw.trim() === '') return undefined;
	const parsed = Number.parseFloat(raw);
	if (!Number.isFinite(parsed)) return undefined;
	if (minimum !== undefined && parsed < minimum) return minimum;
	if (maximum !== undefined && parsed > maximum) return maximum;
	return parsed;
}

export function stringifyJson(value: unknown, fallback = '{}'): string {
	try {
		return JSON.stringify(value ?? JSON.parse(fallback), null, 2);
	} catch {
		return fallback;
	}
}

export function tryParseJson(text: string): unknown | undefined {
	try {
		return JSON.parse(text);
	} catch {
		return undefined;
	}
}

export function uniqueStrings(values: string[]): string[] {
	return Array.from(
		new Set(
			values
				.map((value) => value.trim())
				.filter((value) => value.length > 0)
		)
	);
}
