<script lang="ts">
  import { Handle, Position } from "@xyflow/svelte";
  import type { PipelineNodeData } from "$lib/flow/types";
  import { graphStore } from "$lib/flow/store/graphStore";
  import { displayStatusFromBinding } from "$lib/flow/store/runScope";

  // xyflow passes these props into node components
  export let id: string;
  export let data: PipelineNodeData;

  // xyflow also passes some optional props (safe to accept)
  export let selected: boolean = false;

  // Status is derived from bindings; node.data.status is not authoritative.
  $: binding = $graphStore.nodeBindings?.[id];
  $: status = displayStatusFromBinding(binding as any);
  $: kind = data?.kind ?? "node";
  $: label = data?.label ?? "Node";

  // ✅ normalize ports (treat missing as null)
  $: inPort  = data?.ports?.in ?? null;
  $: outPort = data?.ports?.out ?? null;
</script>

{#if inPort !== null}
  <Handle type="target" position={Position.Left} id="in" />
{/if}

{#if outPort !== null}
  <Handle type="source" position={Position.Right} id="out" />
{/if}

<div class={`node ${selected ? "selected" : ""} st-${status}`}>
  <div class="title">
    <span class="label">{label}</span>
    <span class="badge">{kind}</span>
  </div>

  <slot />

  <div class="footer">
    <span class="status">{status}</span>
  </div>
</div>

<style>
  .node {
    width: 220px;
    border-radius: 12px;
    border: 1px solid #2a2a2a;
    background: #0f1115;
    color: #e6e6e6;
    padding: 10px;
    box-shadow: 0 8px 18px rgba(0, 0, 0, 0.35);
  }

  .node.selected {
    outline: 2px solid #4b8cff;
  }

  .title {
    display: flex;
    align-items: center;
    justify-content: space-between;
    font-weight: 600;
    margin-bottom: 8px;
    gap: 10px;
  }

  .label {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .badge {
    font-size: 12px;
    opacity: 0.8;
    border: 1px solid #283044;
    border-radius: 999px;
    padding: 2px 8px;
  }

  .footer {
    margin-top: 8px;
    font-size: 12px;
    opacity: 0.85;
  }

  /* status coloring */
  .st-idle .status { color: #e6e6e6; }
  .st-stale .status { color: #f2cc60; }
  .st-running .status { color: #8ab4ff; }
  .st-succeeded .status { color: #7ee787; }
  .st-failed .status { color: #ff7b72; }
  .st-canceled .status { color: #f2cc60; }
</style>
