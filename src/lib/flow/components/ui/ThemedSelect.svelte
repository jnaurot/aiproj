<script lang="ts">
	import { createEventDispatcher, tick } from 'svelte';

	export type ThemedSelectOption = {
		value: string;
		label: string;
		disabled?: boolean;
	};

	export let value = '';
	export let options: ThemedSelectOption[] = [];
	export let ariaLabel = 'Select option';
	export let disabled = false;
	export let onValueChange: (next: string) => void = () => {};

	let open = false;
	let rootEl: HTMLDivElement | null = null;
	let triggerEl: HTMLButtonElement | null = null;
	let optionEls: Array<HTMLButtonElement | null> = [];

	const dispatch = createEventDispatcher<{ change: string }>();

	$: selected = options.find((option) => option.value === value) ?? options[0] ?? { value: '', label: '' };

	function toggleOpen(): void {
		if (disabled) return;
		open = !open;
		if (open) {
			void tick().then(() => focusSelectedOrFirst());
		}
	}

	function selectOption(option: ThemedSelectOption): void {
		if (disabled || option.disabled) return;
		if (option.value !== value) {
			onValueChange(option.value);
			dispatch('change', option.value);
		}
		open = false;
		triggerEl?.focus();
	}

	function handleFocusOut(event: FocusEvent): void {
		const next = event.relatedTarget as Node | null;
		if (!rootEl || (next && rootEl.contains(next))) return;
		open = false;
	}

	function enabledOptionIndexes(): number[] {
		const indexes: number[] = [];
		for (let i = 0; i < options.length; i += 1) {
			if (!options[i]?.disabled) indexes.push(i);
		}
		return indexes;
	}

	function focusOptionAt(index: number): void {
		const el = optionEls[index] ?? null;
		el?.focus();
	}

	function focusSelectedOrFirst(): void {
		const enabled = enabledOptionIndexes();
		if (enabled.length === 0) return;
		const selectedIndex = options.findIndex((option) => option.value === value && !option.disabled);
		const target = selectedIndex >= 0 ? selectedIndex : enabled[0];
		focusOptionAt(target);
	}

	function focusNextOption(currentIndex: number): void {
		const enabled = enabledOptionIndexes();
		if (enabled.length === 0) return;
		const currentPos = enabled.indexOf(currentIndex);
		const nextPos = currentPos < 0 ? 0 : Math.min(currentPos + 1, enabled.length - 1);
		focusOptionAt(enabled[nextPos]);
	}

	function focusPrevOption(currentIndex: number): void {
		const enabled = enabledOptionIndexes();
		if (enabled.length === 0) return;
		const currentPos = enabled.indexOf(currentIndex);
		const prevPos = currentPos < 0 ? enabled.length - 1 : Math.max(currentPos - 1, 0);
		focusOptionAt(enabled[prevPos]);
	}

	function handleTriggerKeydown(event: KeyboardEvent): void {
		if (disabled) return;
		if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
			event.preventDefault();
			if (!open) {
				open = true;
				void tick().then(() => focusSelectedOrFirst());
			}
			return;
		}
		if (event.key === 'Enter' || event.key === ' ') {
			event.preventDefault();
			toggleOpen();
			return;
		}
		if (event.key === 'Escape') {
			event.preventDefault();
			open = false;
		}
	}

	function handleOptionKeydown(index: number, event: KeyboardEvent): void {
		if (event.key === 'ArrowDown') {
			event.preventDefault();
			focusNextOption(index);
			return;
		}
		if (event.key === 'ArrowUp') {
			event.preventDefault();
			focusPrevOption(index);
			return;
		}
		if (event.key === 'Enter' || event.key === ' ') {
			event.preventDefault();
			const option = options[index];
			if (option) selectOption(option);
			return;
		}
		if (event.key === 'Escape') {
			event.preventDefault();
			open = false;
			triggerEl?.focus();
			return;
		}
		if (event.key === 'Tab') {
			open = false;
		}
	}
</script>

<div class="themedSelect" bind:this={rootEl} on:focusout={handleFocusOut}>
	<button
		type="button"
		class="trigger"
		aria-label={ariaLabel}
		aria-haspopup="listbox"
		aria-expanded={open}
		disabled={disabled}
		bind:this={triggerEl}
		on:click={toggleOpen}
		on:keydown={handleTriggerKeydown}
	>
		<span>{selected?.label ?? ''}</span>
		<span class="caret">⌄</span>
	</button>
	{#if open}
		<div class="menu" role="listbox" tabindex="-1">
			{#each options as option, idx (option.value)}
				<button
					type="button"
					role="option"
					aria-selected={option.value === value}
					class:selected={option.value === value}
					class:disabled={Boolean(option.disabled)}
					disabled={Boolean(option.disabled)}
					bind:this={optionEls[idx]}
					on:click={() => selectOption(option)}
					on:keydown={(event) => handleOptionKeydown(idx, event)}
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

	.trigger:disabled {
		cursor: not-allowed;
		color: var(--color-control-disabled-text);
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

	.menu button:hover,
	.menu button:focus-visible {
		background: var(--color-control-option-hover-bg);
		outline: none;
	}

	.menu button.selected {
		background: var(--color-control-option-selected-bg);
	}

	.menu button.disabled {
		color: var(--color-control-disabled-text);
	}
</style>
