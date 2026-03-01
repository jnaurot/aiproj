<script lang="ts">
	export let title: string;
	export let open: boolean | undefined = undefined;
	export let onToggle: ((open: boolean) => void) | undefined = undefined;
	export let variant: 'primary' | 'sub' = 'primary';
	export let summaryRight: string = '';
	export let badge: 'dirty' | null = null;
	export let disabled = false;

	const panelId = `disc_${Math.random().toString(36).slice(2, 10)}`;
	let localOpen = Boolean(open ?? false);

	$: if (typeof open === 'boolean') {
		localOpen = open;
	}

	function toggle(): void {
		if (disabled) return;
		const next = !localOpen;
		localOpen = next;
		onToggle?.(next);
	}
</script>

<div class={`disclosure ${variant} ${disabled ? 'disabled' : ''}`}>
	<button
		type="button"
		class="header"
		aria-expanded={localOpen}
		aria-controls={panelId}
		on:click={toggle}
		disabled={disabled}
	>
		<span class={`chev ${localOpen ? 'open' : ''}`}>▸</span>
		<span class="title">{title}</span>
		{#if badge === 'dirty'}
			<span class="badge">dirty</span>
		{/if}
		<span class="spacer"></span>
		{#if summaryRight}
			<span class="summary">{summaryRight}</span>
		{/if}
	</button>

	{#if localOpen}
		<div id={panelId} class="body">
			<slot />
		</div>
	{/if}
</div>

<style>
	.disclosure {
		border: 1px solid #27314a;
		border-radius: 10px;
		background: #0b1220;
	}

	.disclosure.sub {
		border-style: dashed;
		border-color: #2d3a56;
		background: rgba(11, 18, 32, 0.6);
	}

	.header {
		width: 100%;
		display: flex;
		align-items: center;
		gap: 8px;
		padding: 8px 10px;
		background: transparent;
		border: 0;
		color: inherit;
		text-align: left;
		cursor: pointer;
	}

	.disclosure.disabled .header {
		cursor: not-allowed;
		opacity: 0.7;
	}

	.chev {
		display: inline-block;
		transition: transform 120ms ease;
		font-size: 11px;
		opacity: 0.85;
	}

	.chev.open {
		transform: rotate(90deg);
	}

	.title {
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.02em;
	}

	.badge {
		font-size: 10px;
		line-height: 1;
		padding: 3px 6px;
		border-radius: 999px;
		border: 1px solid #f59e0b;
		color: #fbbf24;
		background: rgba(245, 158, 11, 0.1);
		text-transform: uppercase;
	}

	.spacer {
		flex: 1;
		min-width: 0;
	}

	.summary {
		font-size: 11px;
		opacity: 0.8;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
		max-width: 60%;
	}

	.body {
		padding: 8px 10px 10px;
		border-top: 1px solid #1f2a40;
		display: flex;
		flex-direction: column;
		gap: 10px;
		min-width: 0;
	}
</style>
