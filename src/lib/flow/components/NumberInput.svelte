<script lang="ts">
	export let label = '';
	export let value: number | undefined;
	export let placeholder = '';
	export let min: number | undefined = undefined;
	export let onChange: (next: number | undefined) => void;

	function handle(e: Event) {
		const raw = (e.currentTarget as HTMLInputElement).value;
		if (raw.trim() === '') return onChange(undefined);
		const n = Number(raw);
		if (Number.isNaN(n)) return; // ignore invalid typing
		onChange(n);
	}
</script>

<label>{label}</label>
<input type="number" value={value ?? ''} {placeholder} {min} on:input={handle} />

<style>
	input {
		width: 100%;
		border-radius: 8px;
		padding: 6px 8px;
		background: #0b0c10;
		color: #e6e6e6;
		border: 1px solid #283044;
		margin-top: 6px;
	}
	label {
		display: block;
		margin-top: 10px;
		opacity: 0.85;
	}
</style>
