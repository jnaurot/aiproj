<script lang="ts">
	export let value: any;
	export let name: string | null = null;
	export let depth = 0;

	$: isObj = value !== null && typeof value === 'object';
	$: isArr = Array.isArray(value);
	$: keys = isObj ? (isArr ? value.map((_: any, i: number) => String(i)) : Object.keys(value)) : [];

	function short(v: any): string {
		if (v === null) return 'null';
		if (typeof v === 'string') return `"${v.length > 80 ? `${v.slice(0, 80)}...` : v}"`;
		if (typeof v === 'object') return isArr ? `Array(${v.length})` : `Object(${Object.keys(v).length})`;
		return String(v);
	}
</script>

{#if isObj}
	<details open={depth < 1}>
		<summary>
			{#if name}<b>{name}:</b> {/if}{short(value)}
		</summary>
		<div class="children">
			{#each keys as k}
				<svelte:self name={k} value={isArr ? value[Number(k)] : value[k]} depth={depth + 1} />
			{/each}
		</div>
	</details>
{:else}
	<div class="leaf">{#if name}<b>{name}:</b> {/if}{short(value)}</div>
{/if}

<style>
	.children {
		margin-left: 14px;
		border-left: 1px solid #e5e7eb;
		padding-left: 8px;
	}
	.leaf {
		margin-left: 14px;
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
		font-size: 12px;
	}
	summary {
		cursor: pointer;
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
		font-size: 12px;
	}
</style>
