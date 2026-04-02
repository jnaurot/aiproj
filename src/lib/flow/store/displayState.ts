import type { NodeStatus } from '$lib/flow/types';
import {
	normalizeRuntimeStatus,
	projectNodeStatus,
	type NodeBindingProjectionInput,
	type RuntimeNodeStatus
} from './statusModel';
export type { NodeBindingProjectionInput, RuntimeNodeStatus };
export { normalizeRuntimeStatus };

export function projectNodeDisplayState(
	binding: NodeBindingProjectionInput | null | undefined,
	runtimeStatus?: unknown
): NodeStatus {
	return projectNodeStatus(binding, runtimeStatus).display;
}
