// src/lib/flow/store/persist.ts
import type { PipelineGraphDTO } from "$lib/flow/types";

const KEY = "flow:graph:v1";
const SCHEMA_VERSION = 1;

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
    const payload = {
      schemaVersion: SCHEMA_VERSION,
      updatedAt: new Date().toISOString(),
      graphId: String((graph?.meta as any)?.graphId ?? ""),
      graph
    };
    window.localStorage.setItem(KEY, JSON.stringify(payload));
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

    const parsed = JSON.parse(raw) as any;
    const graphCandidate: Partial<PipelineGraphDTO> =
      parsed && typeof parsed === "object" && parsed.graph && typeof parsed.graph === "object"
        ? (parsed.graph as Partial<PipelineGraphDTO>)
        : (parsed as Partial<PipelineGraphDTO>);

    // minimal validation
    if (graphCandidate?.version !== 1) return fallback;
    if (!Array.isArray(graphCandidate.nodes) || !Array.isArray(graphCandidate.edges)) return fallback;

    // build a correct DTO (required fields always present)
    const dto: PipelineGraphDTO = {
      version: 1,
      nodes: graphCandidate.nodes,
      edges: graphCandidate.edges
    };

    // optional fields
    if (graphCandidate.viewport) dto.viewport = graphCandidate.viewport;
    if (graphCandidate.meta) dto.meta = graphCandidate.meta;

    return dto;
  } catch (e) {
    console.warn("Failed to load graph from localStorage", e);
    return fallback;
  }
}

export function hasGraphDraft(): boolean {
  if (!hasLocalStorage()) return false;
  return Boolean(window.localStorage.getItem(KEY));
}

export function clearGraphDraft(): void {
  if (!hasLocalStorage()) return;
  try {
    window.localStorage.removeItem(KEY);
  } catch (e) {
    console.warn("Failed to clear graph draft from localStorage", e);
  }
}

export function getGraphDraftInfo(): { updatedAt?: string | null; graphId?: string | null } {
  if (!hasLocalStorage()) return {};
  try {
    const raw = window.localStorage.getItem(KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw) as any;
    if (!parsed || typeof parsed !== "object" || !parsed.graph) return {};
    return {
      updatedAt: typeof parsed.updatedAt === "string" ? parsed.updatedAt : null,
      graphId: typeof parsed.graphId === "string" ? parsed.graphId : null
    };
  } catch {
    return {};
  }
}
