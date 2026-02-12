// src/lib/flow/types/graph.ts
// import type { Node, Edge, XYPosition } from "@xyflow/svelte";
import type { Node, Edge } from "@xyflow/svelte";
import type { PipelineNodeData, PipelineEdgeData } from "$lib/flow/types";

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
