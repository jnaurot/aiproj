<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { TransformNodeData } from '$lib/flow/types/transform';
	import type { TransformJoinParams } from '$lib/flow/schema/transform';

	// NOTE: ports/enabled/notes handled upstream. This editor only edits join params.
	export let selectedNode: Node<TransformNodeData>;
	export let params: TransformJoinParams;
	export let onDraft: (patch: Partial<TransformJoinParams>) => void;
	export let onCommit: (patch: Partial<TransformJoinParams>) => void;

	type JoinHow = 'inner' | 'left' | 'right' | 'full';
	type OnClause = { left: string; right: string };

	const DEFAULTS: TransformJoinParams = {
		withNodeId: '',
		how: 'inner',
		on: [{ left: 'id', right: 'id' }]
	};

	$: withNodeId = params?.withNodeId ?? DEFAULTS.withNodeId;
	$: how = (params?.how ?? DEFAULTS.how) as JoinHow;
	$: onClauses = (params?.on?.length ? params.on : DEFAULTS.on) as OnClause[];

	function normOn(list: OnClause[]) {
		const cleaned = list
			.map((x) => ({ left: (x.left ?? '').trim(), right: (x.right ?? '').trim() }))
			.filter((x) => x.left.length > 0 && x.right.length > 0);
		return cleaned;
	}

	function draftWithNodeId(v: string) {
		onDraft({ withNodeId: v });
	}

	function draftHow(v: JoinHow) {
		onDraft({ how: v });
	}

	function draftOnLeft(i: number, v: string) {
		const next = onClauses.map((x, idx) => (idx === i ? { ...x, left: v } : x));
		onDraft({ on: normOn(next) as any });
	}

	function draftOnRight(i: number, v: string) {
		const next = onClauses.map((x, idx) => (idx === i ? { ...x, right: v } : x));
		onDraft({ on: normOn(next) as any });
	}

	function addOn() {
		const next = [...onClauses, { left: '', right: '' }];
		onDraft({ on: normOn(next) as any });
	}

	function removeOn(i: number) {
		const next = onClauses.filter((_, idx) => idx !== i);
		onDraft({ on: normOn(next) as any });
	}

	function resetDefaults() {
		onDraft({ ...DEFAULTS });
		onCommit({ ...DEFAULTS });
	}

	function commit() {
		onCommit({
			withNodeId: (withNodeId ?? '').trim(),
			how,
			on: normOn(onClauses) as any
		});
	}

	$: valid = (withNodeId ?? '').trim().length > 0 && normOn(onClauses).length > 0;

	$: onSql =
		normOn(onClauses)
			.map((x) => `input.${x.left} = other.${x.right}`)
			.join(' AND ') || '(missing join keys)';
</script>

<div class="section">
	<div class="sectionTitle">Join</div>

	<div class="hint">
		Join <code>input</code> with another node’s output (<code>other</code>). The backend resolves
		<code>withNodeId</code> from the run context.
	</div>

	<div class="field">
		<div class="k">withNodeId</div>
		<div class="v">
			<input
				value={withNodeId}
				placeholder="e.g. n_abc123... (pick from UI later)"
				on:input={(e) => draftWithNodeId((e.currentTarget as HTMLInputElement).value)}
				on:blur={commit}
			/>
		</div>
	</div>

	<div class="field">
		<div class="k">how</div>
		<div class="v">
			<select
				value={how}
				on:change={(e) => {
					draftHow((e.currentTarget as HTMLSelectElement).value as JoinHow);
					commit();
				}}
			>
				<option value="inner">inner</option>
				<option value="left">left</option>
				<option value="right">right</option>
				<option value="full">full</option>
			</select>
		</div>
	</div>

	<div class="subTitle">ON conditions</div>

	{#each onClauses as c, i (i)}
		<div class="row">
			<div class="field grow">
				<div class="k">left</div>
				<div class="v">
					<input
						value={c.left}
						placeholder="e.g. id"
						on:input={(e) => draftOnLeft(i, (e.currentTarget as HTMLInputElement).value)}
						on:blur={commit}
					/>
				</div>
			</div>

			<div class="field grow">
				<div class="k">right</div>
				<div class="v">
					<input
						value={c.right}
						placeholder="e.g. id"
						on:input={(e) => draftOnRight(i, (e.currentTarget as HTMLInputElement).value)}
						on:blur={commit}
					/>
				</div>
			</div>

			<button class="danger small" title="Remove ON condition" on:click={() => removeOn(i)}>✕</button>
		</div>
	{/each}

	<div class="actions">
		<button class="small" on:click={addOn}>+ Add ON</button>
		<button class="small ghost" on:click={resetDefaults}>Reset defaults</button>
	</div>

	{#if !valid}
		<div class="warn">
			Join requires:
			<ul>
				<li><code>withNodeId</code> (non-empty)</li>
				<li>At least one ON clause with both <code>left</code> and <code>right</code></li>
			</ul>
		</div>
	{/if}

	<div class="preview">
		<div class="label">Preview</div>
		<pre>
SELECT *
FROM input {how.toUpperCase()} JOIN other
ON {onSql}
		</pre>
	</div>
</div>

<style>
	.section {
		margin-top: 8px;
	}

	.sectionTitle {
		font-weight: 600;
		margin-bottom: 6px;
	}

	.subTitle {
		margin-top: 10px;
		font-weight: 600;
		font-size: 13px;
		opacity: 0.95;
	}

	.hint {
		font-size: 12px;
		opacity: 0.8;
		margin-bottom: 10px;
	}

	.row {
		display: flex;
		gap: 8px;
		align-items: center;
		margin-bottom: 8px;
	}

	.field {
		display: flex;
		gap: 6px;
		align-items: center;
		margin-bottom: 8px;
	}

	.grow {
		flex: 1;
	}

	.k {
		width: 80px;
		font-size: 12px;
		opacity: 0.8;
	}

	.v {
		width: 100%;
	}

	input,
	select {
		width: 100%;
		padding: 4px 6px;
		border-radius: 4px;
		border: 1px solid #ccc;
		box-sizing: border-box;
	}

	.actions {
		display: flex;
		gap: 8px;
		margin-top: 4px;
	}

	button.small {
		padding: 4px 8px;
		font-size: 12px;
	}

	button.ghost {
		background: transparent;
		border: 1px solid #ccc;
		border-radius: 4px;
		cursor: pointer;
	}

	button.danger {
		background: #f44336;
		color: white;
		border: none;
		border-radius: 4px;
		cursor: pointer;
		padding: 4px 8px;
	}

	.warn {
		margin-top: 8px;
		font-size: 12px;
		color: #b00020;
		white-space: pre-wrap;
	}

	.warn ul {
		margin: 6px 0 0 16px;
		padding: 0;
	}

	.preview {
		margin-top: 10px;
	}

	.label {
		font-weight: 700;
		margin-bottom: 6px;
	}

	pre {
		white-space: pre-wrap;
		word-break: break-word;
		padding: 8px;
		border: 1px solid #ddd;
		border-radius: 6px;
		font-size: 12px;
		opacity: 0.95;
	}
</style>
