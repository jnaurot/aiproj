<script lang="ts">
  import type { Node } from "@xyflow/svelte";
  import type { PipelineNodeData } from "$lib/flow/types";

  export let selectedNode: Node<PipelineNodeData> | null;

  // Text shown in the editor
  export let text: string;
  export let onDraftText: (nextText: string) => void;

  // Called when user wants to apply JSON -> params
  export let onApplyJson: (parsed: unknown) => void;

  function tryApply() {
    if (!selectedNode) return;

    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch (e) {
      // UI-level feedback only (don’t mutate graph here)
      alert(`Invalid JSON: ${String(e)}`);
      return;
    }

    onApplyJson(parsed);
  }

  function format() {
    try {
      const parsed = JSON.parse(text);
      onDraftText(JSON.stringify(parsed, null, 2));
    } catch {
      // ignore formatting if invalid JSON
    }
  }
</script>

{#if selectedNode}
  <div class="section">
    <div class="sectionTitle">Params JSON</div>

    <textarea
      rows="10"
      value={text}
      on:input={(e) => onDraftText((e.currentTarget as HTMLTextAreaElement).value)}
    />

    <div style="display:flex; gap:8px; margin-top:8px;">
      <button type="button" on:click={format}>Format</button>
      <button type="button" on:click={tryApply}>Apply JSON</button>
    </div>
  </div>
{/if}
