<script lang="ts">
	export type ThemedSelectOption = {
		value: string;
		label: string;
		disabled?: boolean;
	};

	export let value = '';
	export let options: ThemedSelectOption[] = [];
	export let ariaLabel = 'Select option';
	export let onValueChange: (next: string) => void = () => {};

	let open = false;
	let rootEl: HTMLDivElement | null = null;

	$: selected = options.find((option) => option.value === value) ?? options[0] ?? { value: '', label: '' };

	function toggleOpen(): void {
		open = !open;
	}

	function selectOption(option: ThemedSelectOption): void {
		if (option.disabled) return;
		if (option.value !== value) onValueChange(option.value);
		open = false;
	}

	function handleFocusOut(event: FocusEvent): void {
		const next = event.relatedTarget as Node | null;
		if (!rootEl || (next && rootEl.contains(next))) return;
		open = false;
	}
</script>

<div class="themedSelect" bind:this={rootEl} on:focusout={handleFocusOut}>
	<button type="button" class="trigger" aria-label={ariaLabel} aria-haspopup="listbox" aria-expanded={open} on:click={toggleOpen}>
		<span>{selected?.label ?? ''}</span>
		<span class="caret">⌄</span>
	</button>
	{#if open}
		<div class="menu" role="listbox" tabindex="-1">
			{#each options as option (option.value)}
				<button
					type="button"
					role="option"
					aria-selected={option.value === value}
					class:selected={option.value === value}
					class:disabled={Boolean(option.disabled)}
					disabled={Boolean(option.disabled)}
					on:click={() => selectOption(option)}
				>
					{option.label}
				</button>
			{/each}
		</div>
	{/if}
</div>

<style>
	.themedSelect {
		position: relative;
		width: 100%;
		min-width: 0;
	}

	.trigger {
		width: 100%;
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
		padding: 6px 8px;
		font-size: 13px;
		border-radius: 8px;
		border: 1px solid var(--color-control-border);
		background: var(--color-control-bg);
		color: var(--color-control-text);
	}

	.caret {
		opacity: 0.85;
	}

	.menu {
		position: absolute;
		z-index: 60;
		top: calc(100% + 2px);
		left: 0;
		right: 0;
		max-height: 240px;
		overflow: auto;
		border-radius: 8px;
		border: 1px solid var(--color-control-border);
		background: var(--color-control-option-bg);
		box-shadow: 0 8px 18px rgba(0, 0, 0, 0.35);
	}

	.menu button {
		width: 100%;
		text-align: left;
		padding: 8px;
		font-size: 13px;
		background: transparent;
		color: var(--color-control-option-text);
		border: 0;
	}

	.menu button:hover {
		background: var(--color-control-option-hover-bg);
	}

	.menu button.selected {
		background: var(--color-control-option-selected-bg);
	}

	.menu button.disabled {
		color: var(--color-control-disabled-text);
	}
</style>
