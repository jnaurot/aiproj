<script lang="ts">
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let value: string[] = [];
	export let onChange: (next: string[]) => void;
	export let schema: string[] = [];
	export let placeholder: string = 'Add column';

	let draft = '';
	const suggestionListId = `col-token-${Math.random().toString(36).slice(2, 10)}`;

	$: normalizedValue = uniqueStrings(value.map((item) => String(item ?? '').trim()).filter(Boolean));
	$: suggestions = uniqueStrings(schema.map((item) => String(item ?? '').trim()).filter(Boolean)).filter(
		(item) => !normalizedValue.includes(item)
	);

	function addToken(raw: string): void {
		const token = String(raw ?? '').trim();
		if (!token) return;
		draft = '';
		onChange(uniqueStrings([...normalizedValue, token]));
	}

	function removeToken(token: string): void {
		onChange(normalizedValue.filter((item) => item !== token));
	}
</script>

<div class="tokenInput">
	<div class="tokenRow">
		<Input
			value={draft}
			placeholder={placeholder}
			list={suggestionListId}
			onInput={(e) => (draft = (e.currentTarget as HTMLInputElement).value)}
			onKeydown={(e) => {
				if ((e as KeyboardEvent).key === 'Enter' || (e as KeyboardEvent).key === ',') {
					e.preventDefault();
					addToken(draft);
				}
			}}
		/>
		<button class="small" type="button" on:click={() => addToken(draft)}>Add</button>
	</div>
	{#if suggestions.length > 0}
		<datalist id={suggestionListId}>
			{#each suggestions as suggestion (suggestion)}
				<option value={suggestion}></option>
			{/each}
		</datalist>
	{/if}
	{#if normalizedValue.length > 0}
		<div class="chips">
			{#each normalizedValue as token (token)}
				<button type="button" class="chip" on:click={() => removeToken(token)}>{token} x</button>
			{/each}
		</div>
	{/if}
</div>

<style>
	.tokenRow {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		gap: 8px;
	}

	.chips {
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
		margin-top: 8px;
	}

	.chip {
		border: 1px solid rgba(148, 163, 184, 0.4);
		background: rgba(15, 23, 42, 0.45);
		color: #e2e8f0;
		border-radius: 999px;
		padding: 2px 10px;
		font-size: 12px;
		cursor: pointer;
	}

	.small {
		padding: 6px 10px;
		font-size: 12px;
		border-radius: 8px;
		border: 1px solid var(--ni-control-border, rgba(255, 255, 255, 0.15));
		background: transparent;
		color: inherit;
	}
</style>
