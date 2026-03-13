// const DEFAULT_BACKEND_URL = 'http://localhost:8000';
const DEFAULT_BACKEND_URL = '/api';
const IS_TEST = String((import.meta as any)?.env?.MODE || '').trim().toLowerCase() === 'test';

function normalizeBase(url: string): string {
	const raw = String(url || '').trim();
	const base = raw || DEFAULT_BACKEND_URL;
	return base.replace(/\/+$/, '');
}

function requireAbsolutePath(path: string): string {
	const p = String(path || '').trim();
	if (!p) return '/';
	if (p.startsWith('/')) return p;
	return `/${p}`;
}

export function backendUrl(path: string): string {
	const requested = requireAbsolutePath(path);
	if (IS_TEST) return requested;
	const backendPath =
		requested === '/api'
			? '/'
			: requested.startsWith('/api/')
				? requested.slice(4)
				: requested;
	const base = normalizeBase(String(import.meta.env.VITE_BACKEND_URL || ''));
	return `${base}${backendPath}`;
}
