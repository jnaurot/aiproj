<script lang="ts">
	import { onMount, afterUpdate } from 'svelte';

	export let label: string = '';
	export let className: string = '';
	export let stacked: boolean = false;

	// DEBUG: DOM telemetry
	let vEl: HTMLDivElement | null = null;
	let hasAnyTextarea = false;
	let hasDirectTextarea = false;

	function recompute() {
		if (!vEl) return;

		hasAnyTextarea = !!vEl.querySelector('textarea');

		// direct child textarea only
		hasDirectTextarea = Array.from(vEl.children).some(
			(el) => el.tagName.toLowerCase() === 'textarea'
		);
	}

	onMount(recompute);
	afterUpdate(recompute);

</script>

<!-- <div class={`field ${stacked ? 'stacked' : ''} ${className}`.trim()}>
	{#if label}
		<div class="k">{label}</div>
	{/if}
	<div class="v">
		<slot />
	</div>
</div> -->

<div
	class={`field ${stacked ? 'stacked' : ''} ${className}`.trim()}
	data-has-any-textarea={hasAnyTextarea ? '1' : '0'}
	data-has-direct-textarea={hasDirectTextarea ? '1' : '0'}
>
	{#if label}
		<div class="k">{label}</div>
	{/if}

	<div class="v" bind:this={vEl}>
		<slot />
	</div>
</div>

<!-- <style>
	.field {
		display: grid;
		grid-template-columns: 140px minmax(0, 1fr);
		gap: 8px;
		align-items: start;
		margin-bottom: 10px;
	}

	/* NEW: explicit stacked mode (label above control) */
	.field.stacked {
		grid-template-columns: 1fr;
		gap: 6px;
	}

	/* Existing behavior: For textarea editors, place control under label */
	/* NEW: only if the textarea is directly under .v */
	.field:has(> .v > :global(textarea)) {
		grid-template-columns: 1fr;
		gap: 6px;
	}

	.field:has(> .v > :global(textarea)) .k {
		padding-top: 0;
	}

	.k {
		font-size: 14px;
		opacity: 0.85;
		padding-top: 8px;
	}

	/* In stacked mode, don't push label down */
	.field.stacked .k {
		padding-top: 0;
	}

	/* Existing textarea tweak stays */
	.field:has(textarea) .k {
		padding-top: 0;
	}

	/* Optional: when stacked, indent the value block slightly (your "few spaces") */
	.field.stacked .v {
		padding-left: 12px;
	}
</style> -->

<style>
	.field {
		display: grid;
		grid-template-columns: 140px minmax(0, 1fr);
		gap: 8px;
		align-items: start;
		margin-bottom: 10px;
	}

	.field.stacked {
		grid-template-columns: 1fr;
		gap: 6px;
	}

	.field[data-has-any-textarea='1'] {
		grid-template-columns: 1fr;
		gap: 6px;
	}

	.v {
		min-width: 0;
	}

	.k {
		font-size: 14px;
		opacity: 0.85;
		padding-top: 8px;
		min-width: 0;
	}

	.field.stacked .k {
		padding-top: 0;
	}

	.field[data-has-any-textarea='1'] .k {
		padding-top: 0;
	}

	.field.stacked .v {
		padding-left: 12px;
	}
</style>
