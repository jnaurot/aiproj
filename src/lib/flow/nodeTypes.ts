import SourceNode from "./nodes/SourceNode.svelte";
import TransformNode from "./nodes/TransformNode.svelte";
import ModelNode from "./nodes/ModelNode.svelte";
import ToolNode from "./nodes/ToolNode.svelte";
import ComponentNode from "./nodes/ComponentNode.svelte";

export const nodeTypes = {
  source: SourceNode,
  transform: TransformNode,
	model: ModelNode,
	llm: ModelNode,
	tool: ToolNode,
	component: ComponentNode,
} as const;
