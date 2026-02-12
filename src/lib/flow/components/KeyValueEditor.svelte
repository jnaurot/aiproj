<script lang="ts">
	type Primitive = string | number | boolean | null;
	type ValueType = "string" | "number" | "boolean" | "null" | "json";

	export let label: string = "";
	export let value: Record<string, any> = {};
	export let allowTypes: boolean = false;
	export let defaultType: ValueType = "string";

	// parent passes: onChange={(next) => ...}
	export let onChange: (next: Record<string, any>) => void = () => {};

	type Row = { k: string; t: ValueType; v: string };

	function inferType(v: any): ValueType {
		if (v === null) return "null";
		if (typeof v === "boolean") return "boolean";
		if (typeof v === "number") return "number";
		if (typeof v === "string") return "string";
		return "json";
	}

	function toRows(obj: Record<string, any>): Row[] {
		return Object.entries(obj ?? {}).map(([k, v]) => {
			const t = inferType(v);
			return {
				k,
				t,
				v: t === "json" ? safeStringify(v) : String(v)
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
			case "string":
				return raw;
			case "number": {
				const n = Number(raw);
				return Number.isFinite(n) ? n : 0;
			}
			case "boolean":
				return raw === "true";
			case "null":
				return null;
			case "json":
				try {
					return raw.trim() ? JSON.parse(raw) : {};
				} catch {
					// keep last raw if invalid; parent validation can reject if you want
					return {};
				}
		}
	}

	let rows: Row[] = [];

	// keep local rows synced with external value
	$: rows = toRows(value);

	function emit(nextRows: Row[]) {
		const obj: Record<string, any> = {};
		for (const r of nextRows) {
			const key = r.k.trim();
			if (!key) continue;
			obj[key] = parseValue(allowTypes ? r.t : "string", r.v);
		}
		onChange(obj);
	}

	function addRow() {
		const next = [...rows, { k: "", t: defaultType, v: "" }];
		rows = next;
		emit(next);
	}

	function removeRow(i: number) {
		const next = rows.filter((_, idx) => idx !== i);
		rows = next;
		emit(next);
	}

	function setKey(i: number, k: string) {
		const next = rows.map((r, idx) => (idx === i ? { ...r, k } : r));
		rows = next;
		emit(next);
	}

	function setType(i: number, t: ValueType) {
		const next = rows.map((r, idx) => (idx === i ? { ...r, t } : r));
		rows = next;
		emit(next);
	}

	function setVal(i: number, v: string) {
		const next = rows.map((r, idx) => (idx === i ? { ...r, v } : r));
		rows = next;
		emit(next);
	}
</script>

{#if label}
	<div class="kvLabel">{label}</div>
{/if}

<div class="kvTable">
	{#each rows as r, i (i)}
		<div class="kvRow">
			<input
				class="kvKey"
				placeholder="key"
				value={r.k}
				on:input={(e) => setKey(i, (e.currentTarget as HTMLInputElement).value)}
			/>

			{#if allowTypes}
				<select class="kvType" value={r.t} on:change={(e) => setType(i, (e.currentTarget as HTMLSelectElement).value as any)}>
					<option value="string">string</option>
					<option value="number">number</option>
					<option value="boolean">boolean</option>
					<option value="null">null</option>
					<option value="json">json</option>
				</select>
			{/if}

			{#if allowTypes && r.t === "json"}
				<textarea
					class="kvVal kvJson"
					rows="3"
					placeholder='&#123;"a": 1&#125;'
					on:input={(e) => setVal(i, (e.currentTarget as HTMLTextAreaElement).value)}
				>{r.v}</textarea>
			{:else}
				<input
					class="kvVal"
					placeholder="value"
					value={r.v}
					on:input={(e) => setVal(i, (e.currentTarget as HTMLInputElement).value)}
				/>
			{/if}

			<button class="kvDel" type="button" on:click={() => removeRow(i)}>×</button>
		</div>
	{/each}

	<button class="kvAdd" type="button" on:click={addRow}>+ add</button>
</div>

<style>
	.kvLabel { margin-top: 8px; opacity: 0.85; font-size: 12px; }
	.kvTable { display: flex; flex-direction: column; gap: 8px; }

	.kvRow {
		display: grid;
		grid-template-columns: 1.2fr auto 1.6fr auto;
		gap: 8px;
		align-items: start;
	}

	.kvKey, .kvVal, .kvType, .kvJson {
		border: 1px solid #283044;
		background: #0b0c10;
		color: #e6e6e6;
		border-radius: 8px;
		padding: 6px 8px;
		font-size: 12px;
	}

	.kvJson { font-family: ui-monospace, Menlo, Consolas, monospace; }

	.kvType { width: 110px; }

	.kvDel {
		border: 1px solid #283044;
		background: #111522;
		color: #e6e6e6;
		border-radius: 8px;
		padding: 6px 10px;
		cursor: pointer;
	}

	.kvAdd {
		align-self: flex-start;
		border: 1px solid #283044;
		background: #111522;
		color: #e6e6e6;
		border-radius: 10px;
		padding: 6px 10px;
		cursor: pointer;
		font-weight: 600;
	}
</style>
