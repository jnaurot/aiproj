<script lang="ts">
  import { graphStore } from '$lib/flow/store/graphStore';

  export let edgeId: string;
  export let onClose: () => void;

  // ── Edge and node metadata ────────────────────────────────────────────────
  $: edge = ($graphStore.edges ?? []).find((e) => String(e.id ?? '') === edgeId) ?? null;
  $: sourceNodeId   = String(edge?.source ?? '').trim();
  $: targetNodeId   = String(edge?.target ?? '').trim();
  $: sourceHandleId = String((edge as any)?.sourceHandle ?? 'out').trim() || 'out';
  $: targetHandleId = String((edge as any)?.targetHandle ?? 'in').trim()  || 'in';

  $: sourceNode = ($graphStore.nodes ?? []).find((n) => n.id === sourceNodeId) ?? null;
  $: targetNode = ($graphStore.nodes ?? []).find((n) => n.id === targetNodeId) ?? null;
  $: sourceLabel = String((sourceNode?.data as any)?.label ?? sourceNodeId);
  $: targetLabel = String((targetNode?.data as any)?.label ?? targetNodeId);

  // ── Schema diagnostic ─────────────────────────────────────────────────────
  $: snapshot = (graphStore as any).getEdgeDiagnosticSnapshot?.(edgeId) ?? null;
  $: mismatchMessage =
    snapshot?.contractMessage ||
    snapshot?.schemaPlaneMessage ||
    (snapshot?.effectiveSeverity !== 'clean' ? 'Schema mismatch between source output and target input.' : null);

  // ── Source schema (what the source node outputs on this handle) ───────────
  $: sourceSchemaRaw = (() => {
    const plane = ($graphStore as any)?.schemaPlane;
    if (!plane?.edgeSchemas) return null;
    const edgeSchema = plane.edgeSchemas[edgeId];
    if (!edgeSchema) return null;
    return (edgeSchema as any)?.typedSchema ?? edgeSchema ?? null;
  })();
  $: sourceSchemaText = sourceSchemaRaw ? JSON.stringify(sourceSchemaRaw, null, 2) : '';

  // Source is editable only if the source node has a declared expected output schema.
  // (inferred / model output is read-only)
  $: sourceSchemaEnvelope = (() => {
    const nodeData = sourceNode?.data as any;
    return nodeData?.schema ?? null;
  })();
  $: sourceIsEditable = (() => {
    if (!sourceSchemaEnvelope) return false;
    const declaredSchema =
      sourceSchemaEnvelope?.expectedSchema ??
      sourceSchemaEnvelope?.outputSchema ?? null;
    const src = String((declaredSchema as any)?.source ?? '').trim();
    return src === 'declared';
  })();

  // ── Target expected schema ────────────────────────────────────────────────
  $: targetSchemaEnvelope = (() => {
    const nodeData = targetNode?.data as any;
    return nodeData?.schema ?? null;
  })();
  $: targetExpectedSchemas = (targetSchemaEnvelope as any)?.expectedInputSchemas ?? {};
  $: targetExpectedSchema = targetExpectedSchemas[targetHandleId] ?? null;
  $: targetSchemaRaw = (targetExpectedSchema as any)?.typedSchema ?? null;
  $: targetSchemaText = targetSchemaRaw ? JSON.stringify(targetSchemaRaw, null, 2) : '';

  // Target is editable if declared (not inferred/default)
  $: targetIsEditable = (() => {
    const src = String((targetExpectedSchema as any)?.source ?? '').trim();
    return !src || src === 'declared';
  })();

  // ── Draft edits ───────────────────────────────────────────────────────────
  let sourceDraft: string = '';
  let targetDraft: string = '';
  $: { sourceDraft = sourceSchemaText; }
  $: { targetDraft = targetSchemaText;  }

  let sourceParseError: string | null = null;
  let targetParseError: string | null = null;
  let applySourceResult: { ok: boolean; error?: string } | null = null;
  let applyTargetResult: { ok: boolean; error?: string } | null = null;

  function onSourceDraftInput(e: Event) {
    sourceDraft = (e.target as HTMLTextAreaElement).value;
    sourceParseError = null;
    applySourceResult = null;
    // Live re-validation happens via the reactive snapshot below
  }

  function onTargetDraftInput(e: Event) {
    targetDraft = (e.target as HTMLTextAreaElement).value;
    targetParseError = null;
    applyTargetResult = null;
  }

  function applySource() {
    applySourceResult = null;
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(sourceDraft);
    } catch {
      sourceParseError = 'Invalid JSON';
      return;
    }
    applySourceResult = (graphStore as any).updateNodeSchema?.(
      sourceNodeId,
      sourceHandleId,
      'output',
      parsed
    ) ?? { ok: false, error: 'updateNodeSchema not available' };
  }

  function applyTarget() {
    applyTargetResult = null;
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(targetDraft);
    } catch {
      targetParseError = 'Invalid JSON';
      return;
    }
    applyTargetResult = (graphStore as any).updateNodeSchema?.(
      targetNodeId,
      targetHandleId,
      'input',
      parsed
    ) ?? { ok: false, error: 'updateNodeSchema not available' };
  }

  function handleKeyDown(e: KeyboardEvent) {
    if (e.key === 'Escape') onClose();
  }
</script>

<div
  class="schema-edge-inspector"
  role="dialog"
  aria-label="Schema edge inspector"
  aria-modal="true"
  tabindex="-1"
  on:keydown={handleKeyDown}
>
  <!-- Header -->
  <div class="sei-header">
    <div class="sei-title">
      <span class="sei-edge-label">
        {sourceLabel} <span class="sei-handle">[{sourceHandleId}]</span>
        →
        {targetLabel} <span class="sei-handle">[{targetHandleId}]</span>
      </span>
    </div>
    <button class="sei-close" on:click={onClose} aria-label="Close inspector" title="Close">✕</button>
  </div>

  <!-- Mismatch banner -->
  {#if mismatchMessage && snapshot?.effectiveSeverity !== 'clean'}
    <div class={`sei-banner sei-banner--${snapshot?.effectiveSeverity ?? 'warning'}`}>
      {#if snapshot?.effectiveSeverity === 'error'}⚠{:else}◎{/if}
      {mismatchMessage}
    </div>
  {:else if snapshot?.effectiveSeverity === 'clean'}
    <div class="sei-banner sei-banner--ok">✓ Schemas are compatible</div>
  {/if}

  <!-- Two-column layout -->
  <div class="sei-columns">
    <!-- Source column -->
    <div class="sei-col">
      <div class="sei-col-header">
        <strong>{sourceLabel}</strong>
        <span class="sei-handle-label">{sourceHandleId}</span>
        <span class="sei-role-label">What this node outputs</span>
      </div>
      {#if sourceIsEditable}
        <textarea
          class="sei-schema-editor"
          value={sourceDraft}
          on:input={onSourceDraftInput}
          spellcheck="false"
          aria-label="Source output schema editor"
          rows={8}
        ></textarea>
        {#if sourceParseError}
          <div class="sei-error">{sourceParseError}</div>
        {/if}
        {#if applySourceResult}
          <div class={applySourceResult.ok ? 'sei-success' : 'sei-error'}>
            {applySourceResult.ok ? 'Applied' : (applySourceResult.error ?? 'Error')}
          </div>
        {/if}
        <button class="sei-apply-btn" on:click={applySource}>Apply source</button>
      {:else}
        <pre class="sei-schema-readonly">{sourceSchemaText || '(no schema)'}</pre>
        <p class="sei-readonly-label">Schema inferred from data — not editable</p>
      {/if}
    </div>

    <!-- Target column -->
    <div class="sei-col">
      <div class="sei-col-header">
        <strong>{targetLabel}</strong>
        <span class="sei-handle-label">{targetHandleId}</span>
        <span class="sei-role-label">What this node expects to receive</span>
      </div>
      {#if targetIsEditable}
        <textarea
          class="sei-schema-editor"
          value={targetDraft}
          on:input={onTargetDraftInput}
          spellcheck="false"
          aria-label="Target expected schema editor"
          rows={8}
        ></textarea>
        {#if targetParseError}
          <div class="sei-error">{targetParseError}</div>
        {/if}
        {#if applyTargetResult}
          <div class={applyTargetResult.ok ? 'sei-success' : 'sei-error'}>
            {applyTargetResult.ok ? 'Applied' : (applyTargetResult.error ?? 'Error')}
          </div>
        {/if}
        <button class="sei-apply-btn" on:click={applyTarget}>Apply target</button>
      {:else}
        <pre class="sei-schema-readonly">{targetSchemaText || '(no expected schema declared)'}</pre>
        {#if !targetIsEditable && targetExpectedSchema}
          <p class="sei-readonly-label">Schema inferred — not editable</p>
        {/if}
      {/if}
    </div>
  </div>
</div>

<style>
  .schema-edge-inspector {
    position: fixed;
    right: 1.25rem;
    top: 4rem;
    bottom: 1.25rem;
    width: 680px;
    max-width: calc(100vw - 2.5rem);
    z-index: 50;
    background: rgba(15, 20, 35, 0.97);
    border: 1px solid #374151;
    border-radius: 0.75rem;
    box-shadow: 0 8px 32px rgba(0,0,0,0.6);
    display: flex;
    flex-direction: column;
    font-family: ui-monospace, 'Cascadia Code', 'Fira Code', monospace;
    font-size: 0.8125rem;
    color: #e5e7eb;
    overflow: hidden;
  }

  .sei-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.75rem 1rem;
    border-bottom: 1px solid #374151;
    flex-shrink: 0;
  }

  .sei-title { flex: 1; min-width: 0; }
  .sei-edge-label { font-size: 0.8125rem; color: #d1d5db; }
  .sei-handle {
    color: #9ca3af;
    font-size: 0.75rem;
  }

  .sei-close {
    background: transparent;
    border: none;
    color: #6b7280;
    cursor: pointer;
    font-size: 1rem;
    padding: 0.25rem 0.5rem;
    line-height: 1;
    border-radius: 0.25rem;
  }
  .sei-close:hover { color: #e5e7eb; background: rgba(255,255,255,0.06); }

  .sei-banner {
    padding: 0.5rem 1rem;
    font-size: 0.8125rem;
    flex-shrink: 0;
    border-bottom: 1px solid #374151;
  }
  .sei-banner--error   { color: #f87171; background: rgba(248,113,113,0.08); }
  .sei-banner--warning { color: #fbbf24; background: rgba(251,191,36,0.08);  }
  .sei-banner--ok      { color: #34d399; background: rgba(52,211,153,0.08);  }

  .sei-columns {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0;
    flex: 1;
    overflow: hidden;
  }

  .sei-col {
    padding: 0.875rem 1rem;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    overflow-y: auto;
    border-right: 1px solid #374151;
  }
  .sei-col:last-child { border-right: none; }

  .sei-col-header {
    display: flex;
    flex-direction: column;
    gap: 0.125rem;
    margin-bottom: 0.25rem;
  }
  .sei-col-header strong { color: #f3f4f6; font-size: 0.875rem; }
  .sei-handle-label { color: #9ca3af; font-size: 0.75rem; }
  .sei-role-label {
    color: #6b7280;
    font-size: 0.6875rem;
    font-style: italic;
  }

  .sei-schema-editor {
    flex: 1;
    min-height: 10rem;
    background: rgba(255,255,255,0.04);
    border: 1px solid #374151;
    border-radius: 0.375rem;
    padding: 0.5rem;
    color: #e5e7eb;
    font-family: inherit;
    font-size: 0.75rem;
    resize: vertical;
    line-height: 1.5;
  }
  .sei-schema-editor:focus {
    outline: none;
    border-color: #2E75B6;
  }

  .sei-schema-readonly {
    flex: 1;
    margin: 0;
    padding: 0.5rem;
    background: rgba(255,255,255,0.02);
    border: 1px solid #1f2937;
    border-radius: 0.375rem;
    color: #9ca3af;
    font-family: inherit;
    font-size: 0.75rem;
    line-height: 1.5;
    white-space: pre-wrap;
    overflow-y: auto;
  }

  .sei-readonly-label {
    color: #6b7280;
    font-size: 0.6875rem;
    font-style: italic;
    margin: 0;
  }

  .sei-apply-btn {
    align-self: flex-start;
    background: #2E75B6;
    border: none;
    border-radius: 0.375rem;
    color: #fff;
    cursor: pointer;
    font-family: inherit;
    font-size: 0.75rem;
    font-weight: 600;
    padding: 0.375rem 0.875rem;
    transition: background 0.15s;
  }
  .sei-apply-btn:hover { background: #3b82f6; }

  .sei-error   { color: #f87171; font-size: 0.6875rem; }
  .sei-success { color: #34d399; font-size: 0.6875rem; }
</style>
