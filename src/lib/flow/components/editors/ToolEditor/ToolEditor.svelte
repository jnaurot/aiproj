<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { ToolParams } from '$lib/flow/schema/tool';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { asNumberOrEmpty, asString, parseOptionalInt } from '$lib/flow/components/editors/shared';

	type ToolPatch = Partial<ToolParams>;
	type RetryPolicy = NonNullable<ToolParams['retry']>;
	type ToolPermissions = NonNullable<ToolParams['permissions']>;

	const defaultRetry: RetryPolicy = {
		max_attempts: 1,
		backoff_ms: 0,
		on: ['timeout', '429', '5xx']
	};

	const defaultPermissions: ToolPermissions = {
		net: false,
		fs: false,
		env: false,
		subprocess: false
	};

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<ToolParams>;
	export let onDraft: (patch: ToolPatch) => void;
	export let onCommit: (patch: ToolPatch) => void;

	$: void selectedNode?.id;
	$: name = asString(params?.name, '');
	$: toolVersion = asString(params?.toolVersion, 'v1');
	$: sideEffectMode = (params?.side_effect_mode ?? 'pure') as 'pure' | 'idempotent' | 'effectful';
	$: armed = Boolean(params?.armed ?? false);
	$: connectionRef = asString(params?.connectionRef, '');
	$: timeoutMs = asNumberOrEmpty(params?.timeoutMs);
	$: retry = { ...defaultRetry, ...(params?.retry ?? {}) } as RetryPolicy;
	$: maxAttempts = asNumberOrEmpty(retry?.max_attempts ?? 1);
	$: backoffMs = asNumberOrEmpty(retry?.backoff_ms ?? 0);
	$: permissions = { ...defaultPermissions, ...(params?.permissions ?? {}) } as ToolPermissions;
	$: canNet = Boolean(permissions?.net ?? false);
	$: canFs = Boolean(permissions?.fs ?? false);
	$: canEnv = Boolean(permissions?.env ?? false);
	$: canSubprocess = Boolean(permissions?.subprocess ?? false);
</script>

<Section title="Tool Config">
	<Field label="name">
		<Input
			value={name}
			placeholder="http.request"
			onInput={(event) => onDraft({ name: (event.currentTarget as HTMLInputElement).value })}
			onBlur={(event) => onCommit({ name: (event.currentTarget as HTMLInputElement).value })}
		/>
	</Field>

	<Field label="toolVersion">
		<Input
			value={toolVersion}
			placeholder="v1"
			onInput={(event) => onDraft({ toolVersion: (event.currentTarget as HTMLInputElement).value })}
			onBlur={(event) => onCommit({ toolVersion: (event.currentTarget as HTMLInputElement).value })}
		/>
	</Field>

	<Field label="side_effect_mode">
		<select
			value={sideEffectMode}
			on:change={(event) => {
				const value = (event.currentTarget as HTMLSelectElement).value as ToolParams['side_effect_mode'];
				onDraft({ side_effect_mode: value });
				onCommit({ side_effect_mode: value });
			}}
		>
			<option value="pure">pure</option>
			<option value="idempotent">idempotent</option>
			<option value="effectful">effectful</option>
		</select>
	</Field>

	<Field label="armed">
		<Input
			type="checkbox"
			checked={armed}
			onChange={(event) => {
				const checked = (event.currentTarget as HTMLInputElement).checked;
				onDraft({ armed: checked });
				onCommit({ armed: checked });
			}}
		/>
	</Field>

	<Field label="connectionRef">
		<Input
			value={connectionRef}
			placeholder="conn:default"
			onInput={(event) => {
				const value = (event.currentTarget as HTMLInputElement).value.trim();
				onDraft({ connectionRef: value === '' ? undefined : value });
			}}
			onBlur={(event) => {
				const value = (event.currentTarget as HTMLInputElement).value.trim();
				onCommit({ connectionRef: value === '' ? undefined : value });
			}}
		/>
	</Field>

	<Field label="timeoutMs">
		<Input
			type="number"
			min="1"
			step="1"
			value={timeoutMs}
			onInput={(event) => onDraft({ timeoutMs: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}
			onBlur={(event) => onCommit({ timeoutMs: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) })}
		/>
	</Field>

	<Field label="retry.max_attempts">
		<Input
			type="number"
			min="1"
			step="1"
			value={maxAttempts}
			onInput={(event) =>
				onDraft({ retry: { ...retry, max_attempts: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) ?? 1 } })}
			onBlur={(event) =>
				onCommit({ retry: { ...retry, max_attempts: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 1) ?? 1 } })}
		/>
	</Field>

	<Field label="retry.backoff_ms">
		<Input
			type="number"
			min="0"
			step="1"
			value={backoffMs}
			onInput={(event) =>
				onDraft({ retry: { ...retry, backoff_ms: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 0) ?? 0 } })}
			onBlur={(event) =>
				onCommit({ retry: { ...retry, backoff_ms: parseOptionalInt((event.currentTarget as HTMLInputElement).value, 0) ?? 0 } })}
		/>
	</Field>

	<Field label="perm.net">
		<Input
			type="checkbox"
			checked={canNet}
			onChange={(event) => {
				const checked = (event.currentTarget as HTMLInputElement).checked;
				const next = { ...permissions, net: checked };
				onDraft({ permissions: next });
				onCommit({ permissions: next });
			}}
		/>
	</Field>

	<Field label="perm.fs">
		<Input
			type="checkbox"
			checked={canFs}
			onChange={(event) => {
				const checked = (event.currentTarget as HTMLInputElement).checked;
				const next = { ...permissions, fs: checked };
				onDraft({ permissions: next });
				onCommit({ permissions: next });
			}}
		/>
	</Field>

	<Field label="perm.env">
		<Input
			type="checkbox"
			checked={canEnv}
			onChange={(event) => {
				const checked = (event.currentTarget as HTMLInputElement).checked;
				const next = { ...permissions, env: checked };
				onDraft({ permissions: next });
				onCommit({ permissions: next });
			}}
		/>
	</Field>

	<Field label="perm.subprocess">
		<Input
			type="checkbox"
			checked={canSubprocess}
			onChange={(event) => {
				const checked = (event.currentTarget as HTMLInputElement).checked;
				const next = { ...permissions, subprocess: checked };
				onDraft({ permissions: next });
				onCommit({ permissions: next });
			}}
		/>
	</Field>
</Section>
