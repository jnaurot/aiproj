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
		Join <code>input</code> with another nodeâ€™s output (<code>other</code>). The backend resolves
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

			<button class="danger small" title="Remove ON condition" on:click={() => removeOn(i)}>âœ•</button>
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
		border: 1px solid rgba(255, 255, 255, 0.08);
		border-radius: 12px;
		padding: 12px;
		background: rgba(255, 255, 255, 0.03);
		margin-top: 8px;
	}

	.sectionTitle {
		font-weight: 650;
		font-size: 14px;
		margin-bottom: 10px;
		opacity: 0.9;
	}

	.subTitle {
		margin-top: 10px;
		font-weight: 600;
		font-size: 13px;
		opacity: 0.95;
	}

	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-bottom: 10px;
		line-height: 1.35;
	}

	.row {
		display: flex;
		gap: 8px;
		align-items: flex-start;
		margin-bottom: 8px;
	}

	.line {
		display: flex;
		gap: 8px;
		align-items: center;
		margin-bottom: 8px;
	}

	.field {
		display: grid;
		grid-template-columns: 100px minmax(0, 1fr);
		align-items: start;
		gap: 8px;
		margin-bottom: 10px;
	}

	.field.grow {
		flex: 1;
	}

	.field.dir {
		grid-template-columns: 70px minmax(0, 1fr);
	}

	.k,
	.label {
		font-size: 14px;
		opacity: 0.85;
		padding-top: 8px;
		font-weight: 400;
	}

	.v {
		min-width: 0;
		width: 100%;
	}

	.colInput {
		flex: 1;
	}

	.arrow {
		opacity: 0.75;
		padding-top: 8px;
	}

	.toggle {
		display: inline-flex;
		gap: 8px;
		align-items: center;
	}

	input,
	select,
	textarea,
	.readonly,
	.code {
		width: 100%;
		box-sizing: border-box;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		background: rgba(0, 0, 0, 0.2);
		color: inherit;
		padding: 8px 10px;
		font-size: 14px;
		outline: none;
		min-height: 40px;
	}

	textarea,
	.code {
		resize: vertical;
		line-height: 1.35;
		min-height: 96px;
	}

	input[type='checkbox'] {
		width: auto;
		min-height: 0;
		padding: 0;
	}

	input:focus,
	select:focus,
	textarea:focus,
	.code:focus,
	.readonly:focus {
		border-color: rgba(255, 255, 255, 0.25);
	}

	.actions,
	.snips {
		margin-top: 8px;
		display: flex;
		gap: 8px;
		justify-content: flex-end;
		flex-wrap: wrap;
	}

	.snipsTitle {
		font-size: 12px;
		opacity: 0.8;
		align-self: center;
	}

	.snipRow {
		display: flex;
		gap: 8px;
		width: 100%;
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
		color: #fecaca;
	}

	.warn {
		margin-top: 8px;
		font-size: 12px;
		color: #fca5a5;
		white-space: pre-wrap;
	}

	.warn ul {
		margin: 6px 0 0 16px;
		padding: 0;
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
		opacity: 0.95;
	}

	code {
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
		font-size: 12px;
	}
</style>

