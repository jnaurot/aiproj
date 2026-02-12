// src/lib/flow/store/persist.ts
import type { PipelineGraphDTO } from "$lib/flow/types";

const KEY = "flow:graph:v1";

function hasLocalStorage(): boolean {
  return typeof window !== "undefined" && typeof window.localStorage !== "undefined";
}

export const emptyGraph: PipelineGraphDTO = {
  version: 1,
  nodes: [],
  edges: []
};

export function saveGraphToLocalStorage(graph: PipelineGraphDTO) {
  if (!hasLocalStorage()) return;
  try {
    window.localStorage.setItem(KEY, JSON.stringify(graph));
  } catch (e) {
    console.warn("Failed to save graph to localStorage", e);
  }
}

export function loadGraphFromLocalStorage(
  fallback: PipelineGraphDTO = emptyGraph
): PipelineGraphDTO {
  if (!hasLocalStorage()) return fallback;

  try {
    const raw = window.localStorage.getItem(KEY);
    if (!raw) return fallback;

    const parsed = JSON.parse(raw) as Partial<PipelineGraphDTO>;

    // minimal validation
    if (parsed?.version !== 1) return fallback;
    if (!Array.isArray(parsed.nodes) || !Array.isArray(parsed.edges)) return fallback;

    // build a correct DTO (required fields always present)
    const dto: PipelineGraphDTO = {
      version: 1,
      nodes: parsed.nodes,
      edges: parsed.edges
    };

    // optional fields
    if (parsed.viewport) dto.viewport = parsed.viewport;
    if (parsed.meta) dto.meta = parsed.meta;

    return dto;
  } catch (e) {
    console.warn("Failed to load graph from localStorage", e);
    return fallback;
  }
}
