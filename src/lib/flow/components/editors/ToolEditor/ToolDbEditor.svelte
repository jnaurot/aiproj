<script lang="ts">
	export let params: Record<string, any>;
	export let onDraft: (patch: Record<string, any>) => void;
	export let onCommit: (patch: Record<string, any>) => void;
	$: db = params?.db ?? { connectionRef: '', sql: '', params: {} };
	$: dbParamsText = JSON.stringify(db?.params ?? {}, null, 2);
	function commitParams(text: string) {
		try {
			onCommit({ db: { ...db, params: JSON.parse(text) } });
		} catch {}
	}
</script>

<div class="section">
	<div class="sectionTitle">DB</div>
	<div class="field">
		<div class="k">connectionRef</div>
		<div class="v">
			<input
				value={db?.connectionRef ?? ''}
				on:input={(e) =>
					onDraft({ db: { ...db, connectionRef: (e.currentTarget as HTMLInputElement).value } })}
				on:blur={(e) =>
					onCommit({ db: { ...db, connectionRef: (e.currentTarget as HTMLInputElement).value } })}
			/>
		</div>
	</div>
	<div class="field">
		<div class="k">sql</div>
		<div class="v">
			<textarea
				rows="8"
				value={db?.sql ?? ''}
				on:input={(e) =>
					onDraft({ db: { ...db, sql: (e.currentTarget as HTMLTextAreaElement).value } })}
				on:blur={(e) =>
					onCommit({ db: { ...db, sql: (e.currentTarget as HTMLTextAreaElement).value } })}
			></textarea>
		</div>
	</div>
	<div class="field">
		<div class="k">params</div>
		<div class="v">
			<textarea
				rows="6"
				value={dbParamsText}
				on:blur={(e) => commitParams((e.currentTarget as HTMLTextAreaElement).value)}
			></textarea>
		</div>
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
	.field {
		display: grid;
		grid-template-columns: 100px minmax(0, 1fr);
		gap: 8px;
		align-items: start;
		margin-bottom: 10px;
	}
	.k {
		font-size: 14px;
		opacity: 0.85;
		padding-top: 8px;
	}
	input,
	textarea {
		width: 100%;
		box-sizing: border-box;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		background: rgba(0, 0, 0, 0.2);
		color: inherit;
		padding: 8px 10px;
		font-size: 14px;
		min-height: 40px;
	}
	textarea {
		resize: vertical;
		min-height: 96px;
	}
</style>
