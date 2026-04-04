const localDateTimeFormatter = new Intl.DateTimeFormat(undefined, {
	year: 'numeric',
	month: '2-digit',
	day: '2-digit',
	hour: 'numeric',
	minute: '2-digit',
	second: '2-digit'
});

export function formatUserLocalTime(value: unknown): string {
	const raw = String(value ?? '').trim();
	if (!raw) return '-';
	const parsed = Date.parse(raw);
	if (Number.isNaN(parsed)) return raw;
	return localDateTimeFormatter.format(new Date(parsed));
}
