<script lang="ts">
	import type { EffectiveConfigLine } from './sourceEffectiveConfig';

	export let lines: EffectiveConfigLine[] = [];
	export let recentAdjustments: string[] = [];
	export let title = 'Effective config';
	export let adjustmentsTitle = 'Auto-adjustments';
</script>

<div class="effectivePreview">
	<div class="effectiveTitle">{title}</div>
	<div class="effectiveList">
		{#if lines.length === 0}
			<div class="effectiveEmpty">No effective config available.</div>
		{:else}
			{#each lines as line}
				<div class="effectiveRow">
					<span class="effectiveKey">{line.key}</span>
					<span class="effectiveValue">{line.value}</span>
				</div>
			{/each}
		{/if}
	</div>
	{#if recentAdjustments.length > 0}
		<div class="adjustments">
			<div class="adjustmentsTitle">{adjustmentsTitle}</div>
			<ul>
				{#each recentAdjustments as adjustment}
					<li>{adjustment}</li>
				{/each}
			</ul>
		</div>
	{/if}
</div>

<style>
	.effectivePreview {
		margin-top: 8px;
		padding: 8px;
		border: 1px solid var(--color-control-border, #253149);
		border-radius: 8px;
		background: var(--color-panel-muted-bg, rgba(11, 18, 32, 0.45));
		font-size: 12px;
	}

	.effectiveTitle {
		font-weight: 600;
		margin-bottom: 6px;
	}

	.effectiveList {
		display: grid;
		gap: 4px;
	}

	.effectiveRow {
		display: grid;
		grid-template-columns: max-content minmax(0, 1fr);
		gap: 8px;
	}

	.effectiveKey {
		opacity: 0.72;
	}

	.effectiveValue {
		word-break: break-word;
		overflow-wrap: anywhere;
	}

	.effectiveEmpty {
		opacity: 0.72;
	}

	.adjustments {
		margin-top: 8px;
		border-top: 1px dashed var(--color-control-border, #253149);
		padding-top: 8px;
	}

	.adjustmentsTitle {
		font-weight: 600;
		margin-bottom: 4px;
	}

	.adjustments ul {
		margin: 0;
		padding-left: 16px;
		display: grid;
		gap: 2px;
	}
</style>

