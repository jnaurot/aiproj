import { TOOL_BUILTIN_PROFILES } from './toolBuiltinProfiles';

const PACKAGE_SPEC_RE = /^[A-Za-z0-9][A-Za-z0-9._-]*(?:[<>=!~].+)?$/;

function basePackageName(spec: string): string {
	return String(spec || '')
		.split(/[<>=!~]/, 1)[0]
		.trim()
		.toLowerCase()
		replaceAll('_', '-');
}

export const TOOL_BUILTIN_ALLOWED_PACKAGE_NAMES = new Set(
	TOOL_BUILTIN_PROFILES.filter((p) => p.id !== 'custom')
		.flatMap((p) => p.packages)
		.map((pkg) => basePackageName(pkg))
		.filter((name) => name.length > 0)
);

export type CustomPackageValidation = {
	packages: string[];
	errors: string[];
	duplicates: string[];
	blocked: string[];
};

export function validateCustomPackageDraft(rawText: string): CustomPackageValidation {
	const text = String(rawText ?? '').replaceAll(',', '\n');
	const errors: string[] = [];
	const duplicates: string[] = [];
	const blocked: string[] = [];
	const out: string[] = [];
	const seen = new Set<string>();
	for (const [idx, raw] of text.split('\n').entries()) {
		const spec = raw.trim();
		if (!spec) continue;
		if (!PACKAGE_SPEC_RE.test(spec)) {
			errors.push(`line ${idx + 1}: invalid package spec "${spec}"`);
			continue;
		}
		const normalized = spec.toLowerCase();
		if (seen.has(normalized)) {
			duplicates.push(spec);
			continue;
		}
		const base = basePackageName(spec);
		if (!TOOL_BUILTIN_ALLOWED_PACKAGE_NAMES.has(base)) {
			blocked.push(spec);
			continue;
		}
		seen.add(normalized);
		out.push(spec);
	}
	if (duplicates.length > 0) {
		errors.push(`duplicate entries removed: ${Array.from(new Set(duplicates)).join(', ')}`);
	}
	if (blocked.length > 0) {
		errors.push(`blocked package(s): ${Array.from(new Set(blocked)).join(', ')}`);
	}
	return {
		packages: out,
		errors,
		duplicates: Array.from(new Set(duplicates)),
		blocked: Array.from(new Set(blocked))
	};
}

