import { backendUrl } from '$lib/flow/client/backend';
import {
	sanitizeNodeDocGeneratedExplanation,
	type NodeDocGeneratedExplanation
} from '$lib/flow/schema/nodeDocs';
import type { NodeDocLlmContext } from './nodeDocLlmContext';

export type NodeDocLlmTelemetry = {
	status: 'success' | 'failed';
	fallbackReason?: 'timeout' | 'network' | 'invalid_response' | 'unknown';
	latencyMs: number;
	cacheHit?: boolean;
};

export type NodeDocLlmServiceResult = {
	explanation: NodeDocGeneratedExplanation | null;
	telemetry: NodeDocLlmTelemetry;
};

export type NodeDocLlmServiceOptions = {
	timeoutMs?: number;
	retries?: number;
	provider?: string;
	model?: string;
	onTelemetry?: (telemetry: NodeDocLlmTelemetry) => void;
};

function nowMs(): number {
	return Date.now();
}

async function postOnce(
	context: NodeDocLlmContext,
	signatureKey: string,
	options: Required<Pick<NodeDocLlmServiceOptions, 'timeoutMs' | 'provider' | 'model'>>
): Promise<NodeDocGeneratedExplanation | null> {
	const controller = new AbortController();
	const timeoutHandle = setTimeout(() => controller.abort(), options.timeoutMs);
	try {
		const response = await fetch(backendUrl('/api/models/node-doc-explain'), {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			signal: controller.signal,
			body: JSON.stringify({
				context,
				signatureKey,
				provider: options.provider,
				model: options.model
			})
		});
		if (!response.ok) return null;
		const payload = await response.json();
		return sanitizeNodeDocGeneratedExplanation(payload);
	} finally {
		clearTimeout(timeoutHandle);
	}
}

export async function generateNodeDocLlmExplanation(
	context: NodeDocLlmContext,
	signatureKey: string,
	options: NodeDocLlmServiceOptions = {}
): Promise<NodeDocLlmServiceResult> {
	const retries = Math.max(0, Number(options.retries ?? 1));
	const timeoutMs = Math.max(400, Number(options.timeoutMs ?? 2500));
	const provider = String(options.provider ?? 'ollama').trim() || 'ollama';
	const model = String(options.model ?? 'glm-4.7-flash:latest').trim() || 'glm-4.7-flash:latest';
	const startedAt = nowMs();
	let lastReason: NodeDocLlmTelemetry['fallbackReason'] = 'unknown';
	for (let attempt = 0; attempt <= retries; attempt += 1) {
		try {
			const explanation = await postOnce(context, signatureKey, { timeoutMs, provider, model });
			if (explanation) {
				const telemetry: NodeDocLlmTelemetry = {
					status: 'success',
					latencyMs: nowMs() - startedAt
				};
				options.onTelemetry?.(telemetry);
				return { explanation, telemetry };
			}
			lastReason = 'invalid_response';
		} catch (error: any) {
			const message = String(error?.name ?? error?.message ?? '').toLowerCase();
			lastReason = message.includes('abort') ? 'timeout' : 'network';
		}
	}
	const telemetry: NodeDocLlmTelemetry = {
		status: 'failed',
		fallbackReason: lastReason,
		latencyMs: nowMs() - startedAt
	};
	options.onTelemetry?.(telemetry);
	return { explanation: null, telemetry };
}

