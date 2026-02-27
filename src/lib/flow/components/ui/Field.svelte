<script lang="ts">
	export let label: string = '';
	export let className: string = '';
</script>

<div class={`field ${className}`.trim()}>
	{#if label}
		<div class="k">{label}</div>
	{/if}
	<div class="v">
		<slot />
	</div>
</div>

<style>
	.field {
		display: grid;
		grid-template-columns: 100px minmax(0, 1fr);
		gap: 8px;
		align-items: start;
		margin-bottom: 10px;
	}
	.k {
		font-size: 14px;
		opacity: 0.85;
		padding-top: 8px;
	}
</style>
