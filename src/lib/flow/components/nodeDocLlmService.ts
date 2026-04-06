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
	/**
	 * Optional client-side timeout override in milliseconds.
	 * When omitted, request lifetime is controlled by backend timeout policy.
	 */
	timeoutMs?: number;
	retries?: number;
	provider?: string;
	model?: string;
	onTelemetry?: (telemetry: NodeDocLlmTelemetry) => void;
};

export type NodeDocLlmFeedbackRequest = {
	context: NodeDocLlmContext;
	signatureKey: string;
	generatedSummary: string;
	verdict: 'good' | 'bad';
	correctedSummary?: string;
};

export type NodeDocLlmFeedbackResult = {
	ok: boolean;
	stored: boolean;
	entry_id: string;
	kind: string;
	subtype: string;
	suggestion_file: string;
	suggested_fields: string[];
	notes: string[];
};

function nowMs(): number {
	return Date.now();
}

async function postOnce(
	context: NodeDocLlmContext,
	signatureKey: string,
	options: Pick<NodeDocLlmServiceOptions, 'timeoutMs' | 'provider' | 'model'>
): Promise<NodeDocGeneratedExplanation | null> {
	const timeoutMs = Number(options.timeoutMs);
	const useClientTimeout = Number.isFinite(timeoutMs) && timeoutMs > 0;
	const controller = useClientTimeout ? new AbortController() : null;
	const timeoutHandle = useClientTimeout ? setTimeout(() => controller?.abort(), timeoutMs) : null;
	try {
		const response = await fetch(backendUrl('/api/models/node-doc-explain'), {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			...(controller ? { signal: controller.signal } : {}),
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
		if (timeoutHandle) clearTimeout(timeoutHandle);
	}
}

export async function generateNodeDocLlmExplanation(
	context: NodeDocLlmContext,
	signatureKey: string,
	options: NodeDocLlmServiceOptions = {}
): Promise<NodeDocLlmServiceResult> {
	const retries = Math.max(0, Number(options.retries ?? 1));
	const timeoutMsRaw = Number(options.timeoutMs);
	const timeoutMs = Number.isFinite(timeoutMsRaw) && timeoutMsRaw > 0 ? timeoutMsRaw : undefined;
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

function sanitizeFeedbackResult(input: unknown): NodeDocLlmFeedbackResult | null {
	if (!input || typeof input !== 'object') return null;
	const rec = input as Record<string, unknown>;
	const suggestedFields = Array.isArray(rec.suggested_fields)
		? rec.suggested_fields.map((value) => String(value ?? '').trim()).filter(Boolean)
		: [];
	const notes = Array.isArray(rec.notes)
		? rec.notes.map((value) => String(value ?? '').trim()).filter(Boolean)
		: [];
	const ok = Boolean(rec.ok);
	const stored = Boolean(rec.stored);
	const entryId = String(rec.entry_id ?? '').trim();
	const kind = String(rec.kind ?? '').trim();
	const subtype = String(rec.subtype ?? '').trim();
	const suggestionFile = String(rec.suggestion_file ?? '').trim();
	if (!entryId || !kind || !suggestionFile) return null;
	return {
		ok,
		stored,
		entry_id: entryId,
		kind,
		subtype,
		suggestion_file: suggestionFile,
		suggested_fields: suggestedFields,
		notes
	};
}

export async function submitNodeDocLlmFeedback(
	request: NodeDocLlmFeedbackRequest
): Promise<NodeDocLlmFeedbackResult | null> {
	const body = {
		context: request.context,
		signatureKey: String(request.signatureKey ?? '').trim(),
		generatedSummary: String(request.generatedSummary ?? '').trim(),
		verdict: request.verdict === 'bad' ? 'bad' : 'good',
		correctedSummary: String(request.correctedSummary ?? '').trim()
	};
	const response = await fetch(backendUrl('/api/models/node-doc-feedback'), {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify(body)
	});
	if (!response.ok) return null;
	const payload = await response.json();
	return sanitizeFeedbackResult(payload);
}
