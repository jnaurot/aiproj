// src/lib/flow/types/nodes.ts
import type { BaseNodeData } from "./base";
import type { SourceKind, SourceParamsByKind } from "./paramsMap";

export type SourceNodeData<K extends SourceKind = SourceKind> =
    BaseNodeData<"source", SourceParamsByKind[K]> & {
        sourceKind: K; // ✅ structural discriminator
    };

// Later:
// export type ToolNodeData<T extends ToolKind = ToolKind> =
//   BaseNodeData<"tool", ToolParamsByKind[T]> & { ToolKind: T };

