<script lang="ts">
	export let value: string = '';
	export let placeholder: string = '';
	export let onInput: (e: Event) => void = () => {};
	export let onBlur: (e: Event) => void = () => {};
</script>

<input
	value={value}
	placeholder={placeholder}
	on:input={onInput}
	on:blur={onBlur}
/>

<style>
	input {
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
</style>
