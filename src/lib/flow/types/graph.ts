// src/lib/flow/types/graph.ts
// import type { Node, Edge, XYPosition } from "@xyflow/svelte";
import type { Node, Edge } from "@xyflow/svelte";
import type { PipelineNodeData, PipelineEdgeData } from "$lib/flow/types";
import type { PortType } from "$lib/flow/types/base";

export type ViewportDTO = {
  x: number;
  y: number;
  zoom: number;
};

export type PipelineGraphDTO = {
  version: 1;
  nodes: Array<Node<PipelineNodeData & Record<string, unknown>>>; // allow extra keys
  edges: Array<Edge<PipelineEdgeData & Record<string, unknown>>>; // allow extra keys
  viewport?: ViewportDTO;
  meta?: {
    createdAt?: string;
    updatedAt?: string;
    name?: string;
  };
};

export type ComponentTypedPrimitive =
	| "table"
	| "json"
	| "text"
	| "binary"
	| "embeddings"
	| "unknown";

export type ComponentTypedFieldDTO = {
	name: string;
	type: ComponentTypedPrimitive;
	nativeType?: string;
	nullable?: boolean;
};

export type ComponentTypedSchemaDTO = {
	type: ComponentTypedPrimitive;
	fields?: ComponentTypedFieldDTO[];
};

export type ComponentApiPortDTO = {
	name: string;
	portType: PortType;
	required?: boolean;
	typedSchema: ComponentTypedSchemaDTO;
};

export type ComponentApiContractDTO = {
	inputs: ComponentApiPortDTO[];
	outputs: ComponentApiPortDTO[];
};
