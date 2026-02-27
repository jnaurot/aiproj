<script lang="ts">
	import type { HTMLInputTypeAttribute } from 'svelte/elements';

	export let value: string | number = '';
	export let checked: boolean = false;
	export let placeholder: string = '';
	export let type: HTMLInputTypeAttribute = 'text';
	export let multiline: boolean = false;
	export let rows: number = 4;
	export let disabled: boolean = false;
	export let readonly: boolean = false;
	export let min: string | number | undefined = undefined;
	export let max: string | number | undefined = undefined;
	export let step: string | number | undefined = undefined;
	export let accept: string | undefined = undefined;
	export let className: string = '';
	export let onInput: (e: Event) => void = () => {};
	export let onBlur: (e: Event) => void = () => {};
	export let onChange: (e: Event) => void = () => {};
</script>

{#if multiline}
	<textarea
		class={className}
		{placeholder}
		{rows}
		{disabled}
		{readonly}
		on:input={onInput}
		on:blur={onBlur}
	>{String(value ?? '')}</textarea>
{:else if type === 'checkbox'}
	<input
		class={`checkbox ${className}`.trim()}
		{type}
		{checked}
		{disabled}
		{readonly}
		on:change={onChange}
		on:blur={onBlur}
	/>
{:else}
	<input
		class={className}
		{type}
		value={value}
		{placeholder}
		{disabled}
		{readonly}
		{min}
		{max}
		{step}
		{accept}
		on:input={onInput}
		on:blur={onBlur}
		on:change={onChange}
	/>
{/if}

<style>
	input,
	textarea {
		width: 100%;
		box-sizing: border-box;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		background: rgba(0, 0, 0, 0.2);
		color: inherit;
		padding: 8px 10px;
		font-size: 14px;
		min-height: 40px;
	}

	textarea {
		resize: vertical;
		line-height: 1.35;
	}

	.checkbox {
		width: auto;
		min-height: 0;
		padding: 0;
	}
</style>
