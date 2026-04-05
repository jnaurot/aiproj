import type { PublishedProfileDiff } from '$lib/flow/components/exposureProfiles';

export type MigrationDiagnostic = {
	code: 'HANDLE_REMOVED' | 'HANDLE_RETYPED';
	handle_id: string;
	message: string;
	severity: 'error' | 'warning';
};

export function buildMigrationDiagnostics(diff: PublishedProfileDiff): MigrationDiagnostic[] {
	const out: MigrationDiagnostic[] = [];
	for (const handleId of diff.removed) {
		out.push({
			code: 'HANDLE_REMOVED',
			handle_id: handleId,
			message: `Published handle removed: ${handleId}`,
			severity: 'error'
		});
	}
	for (const item of diff.retyped) {
		out.push({
			code: 'HANDLE_RETYPED',
			handle_id: item.handle_id,
			message: `Published handle retyped: ${item.handle_id} (${item.before_type} -> ${item.after_type})`,
			severity: 'error'
		});
	}
	return out;
}

