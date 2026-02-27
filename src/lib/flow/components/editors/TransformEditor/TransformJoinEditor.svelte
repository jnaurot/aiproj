<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformJoinParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	type JoinHow = TransformJoinParams['how'];

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformJoinParams>;
	export let onDraft: (patch: Partial<TransformJoinParams>) => void;
	export let onCommit: (patch: Partial<TransformJoinParams>) => void;

	const joinModes: JoinHow[] = ['inner', 'left', 'right', 'full'];
	const defaults: TransformJoinParams = {
		withNodeId: '',
		how: 'inner',
		on: [{ left: 'id', right: 'id' }]
	};

	$: void selectedNode?.id;
	$: withNodeId = typeof params?.withNodeId === 'string' ? params.withNodeId : defaults.withNodeId;
	$: how = params?.how ?? defaults.how;
	$: clauses = params?.on?.length ? params.on : defaults.on;
	$: normalizedClauses = clauses
		.map((clause) => ({ left: clause.left.trim(), right: clause.right.trim() }))
		.filter((clause) => clause.left.length > 0 && clause.right.length > 0);
	$: onSql =
		normalizedClauses.map((clause) => `input.${clause.left} = other.${clause.right}`).join(' AND ') ||
		'(missing join keys)';
</script>

<Section title="Join">
	<div class="hint">Join <code>input</code> with another node output (<code>other</code>).</div>

	<Field label="withNodeId">
		<Input
			value={withNodeId}
			placeholder="e.g. n_abc123"
			onInput={(event) => onDraft({ withNodeId: (event.currentTarget as HTMLInputElement).value })}
			onBlur={() => onCommit({ withNodeId: withNodeId.trim(), how, on: normalizedClauses })}
		/>
	</Field>

	<Field label="how">
		<select
			value={how}
			on:change={(event) => {
				const value = (event.currentTarget as HTMLSelectElement).value as JoinHow;
				onDraft({ how: value });
				onCommit({ withNodeId: withNodeId.trim(), how: value, on: normalizedClauses });
			}}
		>
			{#each joinModes as mode}
				<option value={mode}>{mode}</option>
			{/each}
		</select>
	</Field>

	<div class="subTitle">ON conditions</div>
	{#each clauses as clause, index}
		<div class="row">
			<Input
				value={clause.left}
				placeholder="left key"
				onInput={(event) =>
					onDraft({
						on: clauses.map((item, current) =>
							current === index ? { ...item, left: (event.currentTarget as HTMLInputElement).value } : item
						)
					})}
				onBlur={() => onCommit({ withNodeId: withNodeId.trim(), how, on: normalizedClauses })}
			/>
			<Input
				value={clause.right}
				placeholder="right key"
				onInput={(event) =>
					onDraft({
						on: clauses.map((item, current) =>
							current === index ? { ...item, right: (event.currentTarget as HTMLInputElement).value } : item
						)
					})}
				onBlur={() => onCommit({ withNodeId: withNodeId.trim(), how, on: normalizedClauses })}
			/>
			<button class="small danger" type="button" on:click={() => onDraft({ on: clauses.filter((_, current) => current !== index) })}>
				x
			</button>
		</div>
	{/each}

	<div class="actions">
		<button class="small" type="button" on:click={() => onDraft({ on: [...clauses, { left: '', right: '' }] })}>+ Add ON</button>
		<button
			class="small ghost"
			type="button"
			on:click={() => {
				onDraft(defaults);
				onCommit(defaults);
			}}
		>
			Reset defaults
		</button>
	</div>

	{#if withNodeId.trim().length === 0 || normalizedClauses.length === 0}
		<div class="warn">Join requires a target node id and at least one valid ON clause.</div>
	{/if}

	<div class="preview">
		<div class="subTitle">Preview</div>
		<pre>SELECT *
FROM input {how.toUpperCase()} JOIN other
ON {onSql}</pre>
	</div>
</Section>

<style>
	.row {
		display: grid;
		grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) auto;
		gap: 8px;
		align-items: center;
		margin: 8px 0;
	}

	.subTitle {
		margin-top: 10px;
		font-size: 13px;
		font-weight: 600;
	}

	.hint,
	.warn {
		font-size: 12px;
		margin-top: 6px;
	}

	.hint {
		opacity: 0.75;
	}

	.warn {
		color: #fca5a5;
	}

	.actions {
		display: flex;
		gap: 8px;
		justify-content: flex-end;
		margin-top: 8px;
		flex-wrap: wrap;
	}

	.preview {
		margin-top: 12px;
	}

	pre {
		white-space: pre-wrap;
		word-break: break-word;
		padding: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		border-radius: 10px;
		font-size: 12px;
	}

	button.small {
		padding: 6px 10px;
		font-size: 12px;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.06);
		color: inherit;
		cursor: pointer;
	}

	button.ghost {
		background: transparent;
	}

	button.danger {
		border-color: rgba(239, 68, 68, 0.5);
		background: rgba(239, 68, 68, 0.14);
	}

	code {
		font-family: ui-monospace, Menlo, Consolas, monospace;
		font-size: 12px;
	}
</style>
