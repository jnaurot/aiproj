import type { SourceKind } from '$lib/flow/types/paramsMap';

export type SourceEditorTelemetryEvent =
	| {
			type: 'section_toggle';
			sourceKind: SourceKind;
			nodeId: string;
			sectionId: string;
			open: boolean;
	  }
	| {
			type: 'validation';
			sourceKind: SourceKind;
			nodeId: string;
			controlId: string;
			severity: 'info' | 'warning' | 'error';
			action: 'shown' | 'resolved';
	  }
	| {
			type: 'auto_adjustment';
			sourceKind: SourceKind;
			nodeId: string;
			change: string;
			redactedContext: Record<string, unknown>;
	  };

const SECRET_KEY_PATTERN = /(token|secret|password|key|connection_string|auth)/i;

export function redactTelemetryContext(
	payload: Record<string, unknown>
): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	for (const [key, value] of Object.entries(payload)) {
		if (SECRET_KEY_PATTERN.test(key)) {
			out[key] = '[REDACTED]';
			continue;
		}
		out[key] = value;
	}
	return out;
}

export function makeSectionToggleEvent(
	sourceKind: SourceKind,
	nodeId: string,
	sectionId: string,
	open: boolean
): SourceEditorTelemetryEvent {
	return { type: 'section_toggle', sourceKind, nodeId, sectionId, open };
}

export function makeValidationEvent(
	sourceKind: SourceKind,
	nodeId: string,
	controlId: string,
	severity: 'info' | 'warning' | 'error',
	action: 'shown' | 'resolved'
): SourceEditorTelemetryEvent {
	return { type: 'validation', sourceKind, nodeId, controlId, severity, action };
}

export function makeAutoAdjustmentEvent(
	sourceKind: SourceKind,
	nodeId: string,
	change: string,
	context: Record<string, unknown>
): SourceEditorTelemetryEvent {
	return {
		type: 'auto_adjustment',
		sourceKind,
		nodeId,
		change,
		redactedContext: redactTelemetryContext(context)
	};
}

