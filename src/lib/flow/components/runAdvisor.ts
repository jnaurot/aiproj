import type { RunActivityStatus } from '$lib/flow/store/statusModel';
import type { RunMonitorNodeRow } from './runMonitorModel';

export type AdvisorySeverity = 'error' | 'warning' | 'info';
export type AdvisoryConfidence = 'low' | 'medium' | 'high';

export type AdvisoryItem = {
	id: string;
	ruleId: string;
	severity: AdvisorySeverity;
	title: string;
	nodeIds: string[];
	evidence: string[];
	explanation: string;
	actions: string[];
	confidence: AdvisoryConfidence;
	createdAt: string;
};

export type RunAdvisoryInput = {
	runStatus: RunActivityStatus;
	rows: RunMonitorNodeRow[];
	logs: unknown[];
	now?: string;
};

function normalizeLogLine(entry: unknown): string {
	if (typeof entry === 'string') return entry;
	if (!entry || typeof entry !== 'object') return '';
	const obj = entry as Record<string, unknown>;
	const msg = String(obj.message ?? obj.msg ?? obj.text ?? obj.line ?? '').trim();
	if (msg) return msg;
	try {
		return JSON.stringify(obj);
	} catch {
		return '';
	}
}

function hashEvidence(lines: string[]): string {
	const joined = lines.join('|').slice(0, 1000);
	let hash = 0;
	for (let i = 0; i < joined.length; i += 1) {
		hash = (hash * 31 + joined.charCodeAt(i)) >>> 0;
	}
	return hash.toString(16);
}

function makeItem(ruleId: string, partial: Omit<AdvisoryItem, 'id' | 'ruleId' | 'createdAt'>, now: string): AdvisoryItem {
	const nodeIds = [...new Set(partial.nodeIds.map((id) => String(id ?? '').trim()).filter(Boolean))].sort();
	const evidence = [...new Set(partial.evidence.map((line) => String(line ?? '').trim()).filter(Boolean))];
	const id = `${ruleId}:${nodeIds.join(',')}:${hashEvidence(evidence)}`;
	return {
		id,
		ruleId,
		createdAt: now,
		...partial,
		nodeIds,
		evidence
	};
}

export function buildRunAdvisory(input: RunAdvisoryInput): AdvisoryItem[] {
	const now = String(input.now ?? new Date().toISOString());
	const rows = Array.isArray(input.rows) ? input.rows : [];
	const logs = (Array.isArray(input.logs) ? input.logs : []).map(normalizeLogLine).filter(Boolean);
	const items: AdvisoryItem[] = [];

	const add = (ruleId: string, partial: Omit<AdvisoryItem, 'id' | 'ruleId' | 'createdAt'>): void => {
		items.push(makeItem(ruleId, partial, now));
	};

	const unresolvedComponentLogs = logs.filter(
		(line) => line.includes('COMPONENT_OUTPUT_NOT_RESOLVED') || line.includes('COMPONENT_OUTPUT_HANDLE_UNRESOLVED')
	);
	if (unresolvedComponentLogs.length > 0) {
		const involved = rows
			.filter((row) => /component/i.test(String(row.label ?? '')))
			.map((row) => row.nodeId);
		add('COMPONENT_OUTPUT_RESOLUTION', {
			severity: 'error',
			title: 'Component output mapping is unresolved',
			nodeIds: involved,
			evidence: unresolvedComponentLogs.slice(0, 3),
			explanation:
				'A required component output could not be resolved to an internal artifact or published output edge.',
			actions: [
				'Open the component revision and verify published output handle mapping.',
				'Confirm downstream edges use the intended source handle names.',
				'Re-run component internals to refresh output lineage.'
			],
			confidence: 'high'
		});
	}

	for (const row of rows) {
		if (
			row.lifecycle === 'waiting' &&
			row.pendingInputCount === 0 &&
			row.inflight === 0 &&
			String(row.terminalReasonCode ?? '').trim().length === 0 &&
			String(row.blockedReasonCode ?? '').trim().toUpperCase() === 'WAITING_REQUIRED_INPUT'
		) {
			add('WAITING_WITHOUT_WORK', {
				severity: 'warning',
				title: 'Node is waiting with no queued/inflight work',
				nodeIds: [row.nodeId],
				evidence: [
					`node=${row.nodeId} lifecycle=${row.lifecycle} pending=${row.pendingInputCount} inflight=${row.inflight} blocker=${row.blockedReasonCode}`
				],
				explanation:
					'The node is waiting for required input but currently has no queued work. This can indicate missing upstream closure or unmet required input gate.',
				actions: [
					'Check upstream node completion and input edge closure for this handle.',
					'Inspect required input handles and processing policy expectations.',
					'Review recent control-plane events for upstream_closed and node_terminal.'
				],
				confidence: 'medium'
			});
		}
	}

	const modelFailureLogs = logs.filter((line) => line.includes('MODEL_EXECUTION_FAILED') || line.includes('Ollama request failed'));
	if (modelFailureLogs.length > 0) {
		const failedRows = rows.filter((row) => row.lifecycle === 'failed' || modelFailureLogs.some((line) => line.includes(row.nodeId)));
		add('MODEL_PROVIDER_FAILURE', {
			severity: 'warning',
			title: 'Model provider failures detected',
			nodeIds: failedRows.map((row) => row.nodeId),
			evidence: modelFailureLogs.slice(0, 4),
			explanation:
				'One or more model requests failed at provider level. Retries may have been exhausted or requests timed out.',
			actions: [
				'Increase provider timeout for this run profile if requests are timing out.',
				'Reduce concurrent provider demand if queue/lease contention is high.',
				'Validate model endpoint availability and network path.'
			],
			confidence: 'medium'
		});
	}

	const dedup = new Map<string, AdvisoryItem>();
	for (const item of items) {
		if (!dedup.has(item.id)) dedup.set(item.id, item);
	}
	return [...dedup.values()];
}
