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

type ParityClosureSnapshot = {
	upstreamTotal: number;
	upstreamClosed: number;
	upstreamOpen: number;
	upstreamUnknown: number;
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

function parseKeyNumber(line: string, key: string): number | null {
	const match = line.match(new RegExp(`${key}=(-?\\d+)`));
	if (!match) return null;
	const value = Number(match[1]);
	return Number.isFinite(value) ? value : null;
}

function parseKeyString(line: string, key: string): string {
	const match = line.match(new RegExp(`${key}=([^\\s]+)`));
	return match ? String(match[1] ?? '').trim() : '';
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
	const parityByNode = new Map<string, ParityClosureSnapshot>();

	for (const line of logs) {
		if (!line.includes('[status-parity-')) continue;
		const nodeId = parseKeyString(line, 'node');
		if (!nodeId) continue;
		const upstreamTotal = parseKeyNumber(line, 'upstream_work_total');
		const upstreamClosed = parseKeyNumber(line, 'upstream_work_closed');
		const upstreamOpen = parseKeyNumber(line, 'upstream_work_open');
		const upstreamUnknown = parseKeyNumber(line, 'upstream_work_unknown');
		if (upstreamTotal == null || upstreamClosed == null || upstreamOpen == null || upstreamUnknown == null) continue;
		parityByNode.set(nodeId, { upstreamTotal, upstreamClosed, upstreamOpen, upstreamUnknown });
	}

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
			const closure = parityByNode.get(String(row.nodeId ?? '').trim());
			const allImmediateUpstreamClosed = Boolean(
				closure &&
				closure.upstreamTotal >= 0 &&
				closure.upstreamClosed === closure.upstreamTotal &&
				closure.upstreamOpen === 0
			);
			if (allImmediateUpstreamClosed) {
				add('WAITING_WITHOUT_WORK', {
					severity: 'warning',
					title: 'Node is waiting with no queued/inflight work',
					nodeIds: [row.nodeId],
					evidence: [
						`node=${row.nodeId} lifecycle=${row.lifecycle} pending=${row.pendingInputCount} inflight=${row.inflight} blocker=${row.blockedReasonCode}`,
						`upstream_work_total=${closure?.upstreamTotal ?? -1} upstream_work_closed=${closure?.upstreamClosed ?? -1} upstream_work_open=${closure?.upstreamOpen ?? -1} upstream_work_unknown=${closure?.upstreamUnknown ?? -1}`
					],
					explanation:
						'The node is waiting for required input with no queued/inflight work, and all immediate upstream work edges are already closed. This suggests a terminalization gap.',
					actions: [
						'Check control-plane node_terminal sequencing for this node.',
						'Inspect blockedByNode and scheduler snapshot transitions around this node.',
						'Review recent upstream_closed and node_terminal control events.'
					],
					confidence: 'high'
				});
			}
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
