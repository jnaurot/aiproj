<script lang="ts">
	type Primitive = string | number | boolean | null;
	type ValueType = 'string' | 'number' | 'boolean' | 'null' | 'json';

	export let label: string = '';
	export let value: Record<string, any> = {};
	export let allowTypes: boolean = false;
	export let defaultType: ValueType = 'string';
	export let stacked: boolean = false;

	// parent passes: onChange={(next) => ...}
	export let onChange: (next: Record<string, any>) => void = () => {};

	type Row = { id: string; k: string; t: ValueType; v: string };

	function inferType(v: any): ValueType {
		if (v === null) return 'null';
		if (typeof v === 'boolean') return 'boolean';
		if (typeof v === 'number') return 'number';
		if (typeof v === 'string') return 'string';
		return 'json';
	}

	function toRows(obj: Record<string, any>): Row[] {
		return Object.entries(obj ?? {}).map(([k, v]) => {
			const t = inferType(v);
			return {
				id: crypto.randomUUID(),
				k,
				t,
				v: t === 'json' ? safeStringify(v) : String(v)
			};
		});
	}

	function safeStringify(v: any) {
		try {
			return JSON.stringify(v, null, 2);
		} catch {
			return String(v);
		}
	}

	function parseValue(t: ValueType, raw: string): any {
		if (!allowTypes) return raw; // headers: always strings

		switch (t) {
			case 'string':
				return raw;
			case 'number': {
				const n = Number(raw);
				return Number.isFinite(n) ? n : 0;
			}
			case 'boolean':
				return raw === 'true';
			case 'null':
				return null;
			case 'json':
				try {
					return raw.trim() ? JSON.parse(raw) : {};
				} catch {
					// keep last raw if invalid; parent validation can reject if you want
					return {};
				}
		}
	}

	let rows: Row[] = toRows(value);
	let lastValueRef: unknown = value;
	let dirty = false;

	function markDirty() {
		dirty = true;
	}

	function acceptExternal(nextValue: unknown) {
		rows = toRows(nextValue);
		lastValueRef = nextValue;
		dirty = false;
	}

	// Only accept external changes when we are not dirty.
	$: if (!dirty && value !== lastValueRef) {
		acceptExternal(value);
	}

	function emit(nextRows: Row[]) {
		const obj: Record<string, any> = {};
		for (const r of nextRows) {
			const key = r.k.trim();
			if (!key) continue;
			obj[key] = parseValue(allowTypes ? r.t : 'string', r.v);
		}
		onChange(obj);
	}

	function addRow() {
		markDirty();
		const next: Row[] = [...rows, { id: crypto.randomUUID(), k: '', t: defaultType, v: '' }];
		rows = next;
		emit(next);
	}
	function removeRow(i: number) {
		markDirty();
		const next = rows.filter((_, idx) => idx !== i);
		rows = next;
		emit(next);
	}

	function setKey(id: string, k: string) {
		markDirty();
		const next = rows.map((r) => (r.id === id ? { ...r, k } : r));
		rows = next;
		emit(next);
	}

	function setType(id: string, t: ValueType) {
		markDirty();
		const next = rows.map((r) => (r.id === id ? { ...r, t } : r));
		rows = next;
		emit(next);
	}

	function setVal(id: string, v: string) {
		markDirty();
		const next = rows.map((r) => (r.id === id ? { ...r, v } : r));
		rows = next;
		emit(next);
	}
</script>

{#if label}
	<div class="kvLabel">{label}</div>
{/if}

<div class="kvTable">
	{#each rows as r, i (r.id)}
		<div class={`kvRow ${stacked ? 'stacked' : allowTypes ? 'withTypes' : 'noTypes'}`}>
			<input
				class="kvKey"
				placeholder="key"
				value={r.k}
				on:input={(e) => setKey(r.id, (e.currentTarget as HTMLInputElement).value)}
			/>

			{#if allowTypes}
				<select
					class="kvType"
					value={r.t}
					on:change={(e) => setType(r.id, (e.currentTarget as HTMLSelectElement).value as any)}
				>
					<option value="string">string</option>
					<option value="number">number</option>
					<option value="boolean">boolean</option>
					<option value="null">null</option>
					<option value="json">json</option>
				</select>
			{/if}

			{#if allowTypes && r.t === 'json'}
				<textarea
					class="kvVal kvJson"
					rows="3"
					placeholder={'{"a": 1}'}
					on:input={(e) => setVal(r.id, (e.currentTarget as HTMLTextAreaElement).value)}
					>{r.v}</textarea
				>
			{:else}
				<input
					class="kvVal"
					placeholder="value"
					value={r.v}
					on:input={(e) => setVal(r.id, (e.currentTarget as HTMLInputElement).value)}
				/>
			{/if}

			{#if stacked}
				<div class="kvRowActions">
					{#if i === rows.length - 1}
						<button class="kvAddInline" type="button" on:click={addRow}>+</button>
					{:else}
						<span class="kvActionsSpacer" aria-hidden="true"></span>
					{/if}
					<button class="kvDel" type="button" on:click={() => removeRow(i)}>×</button>
				</div>
			{:else}
				<button class="kvDel" type="button" on:click={() => removeRow(i)}>×</button>
			{/if}
		</div>
	{/each}

	{#if !stacked || rows.length === 0}
		<button class="kvAdd" type="button" on:click={addRow}>+</button>
	{/if}
</div>

<style>
	:global(.inspector) {
		--kv-control-bg: #ffffff;
		--kv-control-text: #1f2937;
		--kv-control-border: #b9c5da;
		--kv-btn-bg: #eef3ff;
		--kv-btn-text: #1f2937;
		--kv-option-bg: #ffffff;
		--kv-option-text: #1f2937;
	}

	@media (prefers-color-scheme: dark) {
		:global(.inspector) {
			--kv-control-bg: #0b0c10;
			--kv-control-text: #e6e6e6;
			--kv-control-border: #283044;
			--kv-btn-bg: #111522;
			--kv-btn-text: #e6e6e6;
			--kv-option-bg: #0b0c10;
			--kv-option-text: #e6e6e6;
		}
	}

	.kvLabel {
		margin-top: 8px;
		opacity: 0.85;
		font-size: 12px;
	}
	.kvTable {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.kvRow {
		display: grid;
		gap: 8px;
		align-items: start;
		min-width: 0; /* critical in nested flex/grid */
	}

	.kvRow.noTypes {
		grid-template-columns: 160px minmax(0, 1fr) auto;
	}

	.kvRow.withTypes {
		grid-template-columns: 160px 110px minmax(0, 1fr) auto;
	}

	.kvRow.stacked {
		display: flex;
		flex-direction: column;
		align-items: stretch;
		gap: 6px;
	}

	.kvKey,
	.kvVal,
	.kvType,
	.kvJson {
		border: 1px solid var(--kv-control-border);
		background: var(--kv-control-bg);
		color: var(--kv-control-text);
		border-radius: 8px;
		padding: 6px 8px;
		font-size: 12px;
	}

	.kvType option {
		background: var(--kv-option-bg);
		color: var(--kv-option-text);
	}

	.kvJson {
		font-family: ui-monospace, Menlo, Consolas, monospace;
	}

	.kvType {
		width: 110px;
	}

	.kvDel {
		border: 1px solid var(--kv-control-border);
		background: var(--kv-btn-bg);
		color: var(--kv-btn-text);
		border-radius: 8px;
		padding: 6px 10px;
		cursor: pointer;
	}

	.kvAdd,
	.kvAddInline {
		align-self: flex-start;
		border: 1px solid var(--kv-control-border);
		background: var(--kv-btn-bg);
		color: var(--kv-btn-text);
		border-radius: 10px;
		padding: 6px 10px;
		cursor: pointer;
		font-weight: 600;
	}

	.kvRow .kvKey {
		width: 160px;
	}

	.kvRow .kvType {
		width: 110px;
	}

	.kvRow .kvVal {
		width: 100%;
		min-width: 0;
	}
	.kvRow input,
	.kvRow select,
	.kvRow textarea {
		min-width: 0;
	}

	.kvRow.stacked .kvKey,
	.kvRow.stacked .kvVal,
	.kvRow.stacked .kvType,
	.kvRow.stacked .kvJson {
		width: 100%;
		min-width: 0;
		box-sizing: border-box;
	}

	.kvRow.stacked .kvDel {
		align-self: auto;
	}

	.kvRowActions {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
	}

	.kvActionsSpacer {
		display: inline-block;
		min-width: 1px;
	}
</style>
