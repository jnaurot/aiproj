<script lang="ts">
	export let params: Record<string, any>;
	export let onDraft: (patch: Record<string, any>) => void;
	export let onCommit: (patch: Record<string, any>) => void;
	$: js = params?.js ?? { code: '' };
</script>

<div class="section">
	<div class="sectionTitle">JavaScript</div>
	<div class="field">
		<div class="k">code</div>
		<div class="v">
			<textarea
				rows="10"
				value={js?.code ?? ''}
				on:input={(e) =>
					onDraft({ js: { ...js, code: (e.currentTarget as HTMLTextAreaElement).value } })}
				on:blur={(e) =>
					onCommit({ js: { ...js, code: (e.currentTarget as HTMLTextAreaElement).value } })}
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
	textarea {
		width: 100%;
		box-sizing: border-box;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		background: rgba(0, 0, 0, 0.2);
		color: inherit;
		padding: 8px 10px;
		font-size: 14px;
		min-height: 120px;
		resize: vertical;
	}
</style>
