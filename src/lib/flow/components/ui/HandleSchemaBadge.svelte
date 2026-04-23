<script lang="ts">
  import type { GraphViewMode } from '$lib/flow/edgeVisualState';

  export let severity: 'error' | 'warning' | null = null;
  export let viewMode: GraphViewMode = 'execution';
  /** Inline style string for absolute positioning within the node container. */
  export let style: string = '';
</script>

{#if viewMode === 'schema' && severity !== null}
  <span
    class={`handle-schema-badge handle-schema-badge--${severity}`}
    {style}
    aria-label={severity === 'error' ? 'Schema error on this handle' : 'Schema warning on this handle'}
    title={severity === 'error' ? 'Schema error' : 'Schema warning'}
  >{severity === 'error' ? '✕' : '⚠'}</span>
{/if}

<style>
  .handle-schema-badge {
    position: absolute;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 14px;
    height: 14px;
    border-radius: 9999px;
    font-size: 9px;
    font-weight: 700;
    line-height: 1;
    pointer-events: none;
    z-index: 10;
    border: 1px solid rgba(0,0,0,0.4);
  }

  .handle-schema-badge--error {
    background: #f87171;
    color: #111827;
  }

  .handle-schema-badge--warning {
    background: #fbbf24;
    color: #111827;
  }
</style>
