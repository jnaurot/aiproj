<script lang="ts">
	export let availableColumns: string[] = [];
	export let upstreamShape: Array<number | string> | null = null;
	export let dtype: string | null = null;
	export let sampleRate: number | null = null;
</script>

<div class="rounded-md border border-blue-500/30 bg-blue-950/20 p-2 text-xs text-blue-200">
	<div class="font-medium text-blue-100">Configuration Oracle</div>
	{#if availableColumns.length > 0}
		<div class="mt-1">Columns: {availableColumns.join(', ')}</div>
	{/if}
	{#if upstreamShape}
		<div class="mt-1">Shape: [{upstreamShape.join(', ')}]</div>
	{/if}
	{#if dtype}
		<div class="mt-1">DType: {dtype}</div>
	{/if}
	{#if sampleRate}
		<div class="mt-1">Sample Rate: {sampleRate}</div>
	{/if}
</div>

