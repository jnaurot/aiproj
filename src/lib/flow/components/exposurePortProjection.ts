import type { ComponentExposureHandle } from '$lib/flow/client/components';

export function projectConnectableHandles(exposureRegistry: ComponentExposureHandle[]): ComponentExposureHandle[] {
	const rows = Array.isArray(exposureRegistry) ? exposureRegistry : [];
	return rows.filter((row) => Boolean(row.published));
}

export function projectDebugVisibleHandles(exposureRegistry: ComponentExposureHandle[]): ComponentExposureHandle[] {
	const rows = Array.isArray(exposureRegistry) ? exposureRegistry : [];
	return rows.filter((row) => Boolean(row.published) || Boolean(row.debug_visible));
}

