<script lang="ts">
	export let params: Record<string, any>;
	export let onDraft: (patch: Record<string, any>) => void;
	export let onCommit: (patch: Record<string, any>) => void;

	$: http = params?.http ?? { url: 'https://', method: 'GET', headers: {}, query: {}, body: null };
	$: headersText = JSON.stringify(http?.headers ?? {}, null, 2);
	$: queryText = JSON.stringify(http?.query ?? {}, null, 2);
	$: bodyText = JSON.stringify(http?.body ?? null, null, 2);

	function commitJson(key: 'headers' | 'query' | 'body', text: string) {
		try {
			const parsed = JSON.parse(text);
			onCommit({ http: { ...http, [key]: parsed } });
		} catch {
			// ignore invalid JSON until corrected
		}
	}
</script>

<div class="section">
	<div class="sectionTitle">HTTP</div>
	<div class="field">
		<div class="k">url</div>
		<div class="v">
			<input
				value={http?.url ?? ''}
				on:input={(e) =>
					onDraft({ http: { ...http, url: (e.currentTarget as HTMLInputElement).value } })}
				on:blur={(e) =>
					onCommit({ http: { ...http, url: (e.currentTarget as HTMLInputElement).value } })}
			/>
		</div>
	</div>
	<div class="field">
		<div class="k">method</div>
		<div class="v">
			<select
				value={http?.method ?? 'GET'}
				on:change={(e) => {
					const v = (e.currentTarget as HTMLSelectElement).value;
					onDraft({ http: { ...http, method: v } });
					onCommit({ http: { ...http, method: v } });
				}}
				><option value="GET">GET</option><option value="POST">POST</option><option value="PUT"
					>PUT</option
				><option value="PATCH">PATCH</option><option value="DELETE">DELETE</option></select
			>
		</div>
	</div>
	<div class="field">
		<div class="k">headers</div>
		<div class="v">
			<textarea
				rows="4"
				value={headersText}
				on:blur={(e) => commitJson('headers', (e.currentTarget as HTMLTextAreaElement).value)}
			></textarea>
		</div>
	</div>
	<div class="field">
		<div class="k">query</div>
		<div class="v">
			<textarea
				rows="4"
				value={queryText}
				on:blur={(e) => commitJson('query', (e.currentTarget as HTMLTextAreaElement).value)}
			></textarea>
		</div>
	</div>
	<div class="field">
		<div class="k">body</div>
		<div class="v">
			<textarea
				rows="6"
				value={bodyText}
				on:blur={(e) => commitJson('body', (e.currentTarget as HTMLTextAreaElement).value)}
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
	select,
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
