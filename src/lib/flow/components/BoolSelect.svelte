<script lang="ts">
	export let label = '';
	export let value: boolean = false;
	export let onChange: (next: boolean) => void;

	function handle(e: Event) {
		const v = (e.currentTarget as HTMLSelectElement).value === 'true';
		onChange(v);
	}
</script>

<label>{label}</label>
<select value={String(value)} on:change={handle}>
	<option value="true">true</option>
	<option value="false">false</option>
</select>

<style>
	select {
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
