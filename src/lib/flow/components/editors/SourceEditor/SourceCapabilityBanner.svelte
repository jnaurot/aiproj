<script lang="ts">
	import { buildSourceCapabilityNotices, resolveSourceCapabilityDescriptor } from '$lib/flow/sourceCapabilities';
	import type { SourceKind } from '$lib/flow/types/paramsMap';

	export let sourceKind: SourceKind = 'file';
	export let params: Record<string, unknown> = {};

	$: descriptor = resolveSourceCapabilityDescriptor(sourceKind);
	$: notices = buildSourceCapabilityNotices(descriptor, params);
</script>

<div class="sourceCapability">
	<div class="row">
		<span class="label">Capability</span>
		<span class={`badge ${descriptor.supportLevel}`}>{descriptor.supportLevel}</span>
	</div>
	{#if notices.length > 0}
		<ul class="noticeList">
			{#each notices as notice}
				<li>{notice}</li>
			{/each}
		</ul>
	{/if}
</div>

<style>
	.sourceCapability {
		display: grid;
		gap: 6px;
		padding: 8px;
		border: 1px solid var(--color-control-border);
		border-radius: 10px;
		background: var(--color-control-bg);
	}

	.row {
		display: flex;
		align-items: center;
		gap: 8px;
	}

	.label {
		font-size: 12px;
		opacity: 0.85;
	}

	.badge {
		font-size: 11px;
		padding: 2px 8px;
		border-radius: 999px;
		border: 1px solid var(--color-control-border);
	}

	.badge.production {
		color: #22c55e;
	}

	.badge.preview {
		color: #f59e0b;
	}

	.badge.mock_only {
		color: #f97316;
	}

	.noticeList {
		margin: 0;
		padding-left: 16px;
		font-size: 12px;
		opacity: 0.9;
		display: grid;
		gap: 2px;
	}
</style>
