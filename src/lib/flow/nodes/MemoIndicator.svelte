<script lang="ts">
	export let memoState:
		| {
				decision: 'reuse' | 'compute' | 'skip_non_memoizable';
		  }
		| undefined
		| null = undefined;
</script>

{#if memoState?.decision === 'reuse'}
	<span class="memoBadge memoReuse" title="Result reused from memo cache">cached</span>
{:else if memoState?.decision === 'compute'}
	<span class="memoBadge memoCompute" title="Freshly computed">computed</span>
{/if}

<style>
	.memoBadge {
		display: inline-flex;
		align-items: center;
		font-size: 10px;
		line-height: 1;
		font-weight: 700;
		letter-spacing: 0.02em;
		text-transform: lowercase;
		padding: 2px 6px;
		border-radius: 999px;
		border: 1px solid transparent;
	}

	.memoReuse {
		background: rgba(34, 197, 94, 0.15);
		border-color: rgba(34, 197, 94, 0.35);
		color: #86efac;
	}

	.memoCompute {
		background: rgba(148, 163, 184, 0.16);
		border-color: rgba(148, 163, 184, 0.35);
		color: #cbd5e1;
	}
</style>
