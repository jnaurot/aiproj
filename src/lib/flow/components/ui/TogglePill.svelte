<script lang="ts">
	import { createEventDispatcher } from 'svelte';

	export let value: boolean = false;
	export let disabled: boolean = false;

	const dispatch = createEventDispatcher<{ toggle: { value: boolean } }>();

	function onToggleClick(): void {
		if (disabled) return;
		const next = !value;
		value = next;
		dispatch('toggle', { value: next });
	}
</script>

<button
	type="button"
	class="runtimeEnvTogglePill"
	class:on={value}
	class:off={!value}
	{disabled}
	on:click={onToggleClick}
>
	{value ? 'On' : 'Off'}
</button>
