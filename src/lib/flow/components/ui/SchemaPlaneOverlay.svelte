<script lang="ts">
  export let enabled: boolean = false;
  export let errorCount: number = 0;
  export let warningCount: number = 0;
  export let onToggle: (() => void) | undefined = undefined;
</script>

{#if enabled}
  <!-- Schema plane status bar — bottom-left corner -->
  <div
    class="schema-plane-overlay"
    role="status"
    aria-label="Schema plane status"
  >
    <!-- Toggle button -->
    {#if onToggle}
      <button
        class="schema-toggle-btn"
        on:click={onToggle}
        title="Exit schema view"
        aria-label="Exit schema view"
      >
        ← Execution View
      </button>
    {/if}

    <!-- Status indicator -->
    <div class="schema-status">
      {#if errorCount > 0}
        <span class="schema-badge schema-error" aria-label="{errorCount} schema error{errorCount !== 1 ? 's' : ''}">
          ⚠ {errorCount} error{errorCount !== 1 ? 's' : ''}
        </span>
      {:else if warningCount > 0}
        <span class="schema-badge schema-warning" aria-label="{warningCount} schema warning{warningCount !== 1 ? 's' : ''}">
          ◎ {warningCount} warning{warningCount !== 1 ? 's' : ''}
        </span>
      {:else}
        <span class="schema-badge schema-valid" aria-label="Schema valid">
          ✓ Schema valid — structure verified
        </span>
      {/if}
    </div>
  </div>
{:else}
  <!-- Collapsed toggle — shows when in execution view -->
  {#if onToggle}
    <button
      class="schema-view-btn"
      on:click={onToggle}
      title="Switch to schema view to inspect types and shapes"
      aria-label="Open schema view"
    >
      Schema View
      {#if errorCount > 0}
        <span class="schema-err-pill">{errorCount}</span>
      {/if}
    </button>
  {/if}
{/if}

<style>
  .schema-plane-overlay {
    position: fixed;
    bottom: 1.5rem;
    left: 1.5rem;
    z-index: 30;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    background: rgba(17, 24, 39, 0.92);
    border: 1px solid #374151;
    border-radius: 0.5rem;
    padding: 0.375rem 0.75rem;
    backdrop-filter: blur(4px);
    box-shadow: 0 2px 8px rgba(0,0,0,0.4);
    font-size: 0.8125rem;
    font-family: ui-monospace, 'Cascadia Code', 'Fira Code', monospace;
    color: #e5e7eb;
    user-select: none;
  }

  .schema-toggle-btn {
    background: transparent;
    border: none;
    color: #9ca3af;
    cursor: pointer;
    font-size: 0.75rem;
    padding: 0 0.375rem;
    border-right: 1px solid #374151;
    margin-right: 0.375rem;
    line-height: 1.5;
  }
  .schema-toggle-btn:hover { color: #e5e7eb; }

  .schema-status { display: flex; align-items: center; }

  .schema-badge {
    display: inline-flex;
    align-items: center;
    gap: 0.25rem;
    font-weight: 500;
    letter-spacing: 0.01em;
  }
  .schema-error  { color: #f87171; }
  .schema-warning { color: #fbbf24; }
  .schema-valid  { color: #34d399; }

  .schema-view-btn {
    position: fixed;
    bottom: 1.5rem;
    left: 1.5rem;
    z-index: 30;
    display: inline-flex;
    align-items: center;
    gap: 0.375rem;
    background: rgba(17, 24, 39, 0.85);
    border: 1px solid #374151;
    border-radius: 0.375rem;
    padding: 0.25rem 0.625rem;
    font-size: 0.75rem;
    font-family: ui-monospace, 'Cascadia Code', 'Fira Code', monospace;
    color: #9ca3af;
    cursor: pointer;
    backdrop-filter: blur(4px);
    transition: color 0.15s, border-color 0.15s;
    user-select: none;
  }
  .schema-view-btn:hover {
    color: #e5e7eb;
    border-color: #2E75B6;
  }

  .schema-err-pill {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    background: #f87171;
    color: #111827;
    border-radius: 9999px;
    font-size: 0.6875rem;
    font-weight: 700;
    min-width: 1.125rem;
    height: 1.125rem;
    padding: 0 0.25rem;
  }
</style>
