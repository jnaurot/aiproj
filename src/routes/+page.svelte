<script lang="ts">
  import { SvelteFlowProvider } from "@xyflow/svelte";
  import FlowCanvas from "$lib/flow/FlowCanvas.svelte";
</script>

<SvelteFlowProvider>
  <FlowCanvas />
</SvelteFlowProvider>
