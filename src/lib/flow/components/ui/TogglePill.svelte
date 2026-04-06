<script lang="ts">
	export let value: boolean = false;
	export let disabled: boolean = false;
</script>

<button
	type="button"
	class="runtimeEnvTogglePill"
	class:on={value}
	class:off={!value}
	{disabled}
	on:click={() => (value = !value)}
>
	{value ? 'On' : 'Off'}
</button>
