<script lang="ts">
	export let label: string = '';
	export let className: string = '';
	export let stacked: boolean = false; // NEW
</script>

<div class={`field ${stacked ? 'stacked' : ''} ${className}`.trim()}>
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

	/* NEW: explicit stacked mode (label above control) */
	.field.stacked {
		grid-template-columns: 1fr;
		gap: 6px;
	}

	/* Existing behavior: For textarea editors, place control under label */
	.field:has(textarea) {
		grid-template-columns: 1fr;
		gap: 6px;
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
</style>