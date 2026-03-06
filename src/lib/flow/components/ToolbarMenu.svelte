<script lang="ts">
	import { onDestroy } from 'svelte';
	import type { ToolbarMenuItem } from './toolbarMenu';
	import { firstEnabledIndex, nextEnabledIndex } from './toolbarMenu';

	export let label = 'Menu';
	export let items: ToolbarMenuItem[] = [];
	export let disabled = false;
	export let compact = false;
	export let align: 'left' | 'right' = 'left';
	export let buttonClass = '';
	export let menuAriaLabel: string | undefined = undefined;
	export let onSelect: (id: string) => void = () => {};

	let open = false;
	let menuEl: HTMLUListElement | null = null;
	let rootEl: HTMLDivElement | null = null;
	let activeIndex = -1;

	function closeMenu() {
		open = false;
		activeIndex = -1;
	}

	function openMenu() {
		if (disabled || !items.length) return;
		open = true;
		activeIndex = firstEnabledIndex(items);
	}

	function toggleMenu() {
		if (open) {
			closeMenu();
			return;
		}
		openMenu();
	}

	function selectItem(item: ToolbarMenuItem) {
		if (item.disabled) return;
		onSelect(item.id);
		closeMenu();
	}

	function onButtonKeyDown(event: KeyboardEvent) {
		if (disabled) return;
		if (event.key === 'ArrowDown' || event.key === 'Enter' || event.key === ' ') {
			event.preventDefault();
			openMenu();
		}
		if (event.key === 'Escape') {
			event.preventDefault();
			closeMenu();
		}
	}

	function onMenuKeyDown(event: KeyboardEvent) {
		if (!open) return;
		if (event.key === 'Escape') {
			event.preventDefault();
			closeMenu();
			return;
		}
		if (event.key === 'ArrowDown') {
			event.preventDefault();
			activeIndex = nextEnabledIndex(items, Math.max(activeIndex, 0), 1);
			return;
		}
		if (event.key === 'ArrowUp') {
			event.preventDefault();
			activeIndex = nextEnabledIndex(items, activeIndex < 0 ? 0 : activeIndex, -1);
			return;
		}
		if (event.key === 'Home') {
			event.preventDefault();
			activeIndex = firstEnabledIndex(items);
			return;
		}
		if (event.key === 'End') {
			event.preventDefault();
			activeIndex = nextEnabledIndex(items, 0, -1);
			return;
		}
		if (event.key === 'Enter' || event.key === ' ') {
			event.preventDefault();
			const item = items[activeIndex];
			if (item) selectItem(item);
		}
	}

	function onWindowPointerDown(event: PointerEvent) {
		if (!open || !rootEl) return;
		const target = event.target as Node | null;
		if (target && rootEl.contains(target)) return;
		closeMenu();
	}

	$: if (open) {
		queueMicrotask(() => {
			const activeItem = menuEl?.querySelector<HTMLButtonElement>(`button[data-idx="${activeIndex}"]`);
			activeItem?.focus();
		});
	}

	onDestroy(() => {
		closeMenu();
	});
</script>

<svelte:window on:pointerdown={onWindowPointerDown} />

<div class="menuRoot" bind:this={rootEl}>
	<button
		type="button"
		class={`menuButton ${compact ? 'compact' : ''} ${buttonClass}`}
		aria-haspopup="menu"
		aria-expanded={open}
		aria-label={menuAriaLabel ?? label}
		{disabled}
		on:click={toggleMenu}
		on:keydown={onButtonKeyDown}
	>
		{label} <span class="caret" aria-hidden="true">▼</span>
	</button>
	{#if open}
		<ul
			class={`menuList ${align === 'right' ? 'alignRight' : 'alignLeft'}`}
			role="menu"
			aria-label={menuAriaLabel ?? label}
			tabindex="-1"
			bind:this={menuEl}
			on:keydown={onMenuKeyDown}
		>
			{#each items as item, idx (item.id)}
				<li role="none">
					<button
						type="button"
						role="menuitem"
						data-idx={idx}
						disabled={item.disabled}
						class={`menuItem ${item.danger ? 'danger' : ''}`}
						on:click={() => selectItem(item)}
					>
						{item.label}
					</button>
				</li>
			{/each}
		</ul>
	{/if}
</div>

<style>
	.menuRoot {
		position: relative;
		display: inline-flex;
	}

	.menuButton {
		border: 1px solid #283044;
		background: #111522;
		color: #e6e6e6;
		padding: 8px 10px;
		border-radius: 10px;
		cursor: pointer;
		font-weight: 600;
	}

	.menuButton.compact {
		padding: 6px 8px;
		font-size: 12px;
		font-weight: 500;
	}

	.menuButton.runSecondary {
		background: #0f1522;
		border-color: #324464;
	}

	.menuButton:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}

	.menuButton:focus-visible {
		outline: 2px solid #4b8cff;
		outline-offset: 2px;
	}

	.caret {
		opacity: 0.85;
		font-size: 10px;
	}

	.menuList {
		position: absolute;
		top: calc(100% + 6px);
		min-width: 190px;
		margin: 0;
		padding: 6px;
		list-style: none;
		border: 1px solid #2a3550;
		background: #0f1626;
		border-radius: 10px;
		z-index: 30;
		display: grid;
		gap: 4px;
		box-shadow: 0 10px 26px rgba(0, 0, 0, 0.35);
	}

	.alignLeft {
		left: 0;
	}

	.alignRight {
		right: 0;
	}

	.menuItem {
		width: 100%;
		text-align: left;
		border: 1px solid transparent;
		background: transparent;
		color: #e6e6e6;
		padding: 6px 8px;
		border-radius: 8px;
		cursor: pointer;
		font-size: 13px;
	}

	.menuItem:hover:not(:disabled),
	.menuItem:focus-visible {
		background: #1c2538;
		border-color: #2f3d5a;
		outline: none;
	}

	.menuItem:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}

	.menuItem.danger {
		color: #ff9c9c;
	}
</style>
