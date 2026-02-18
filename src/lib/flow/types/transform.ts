// src/lib/flow/types/transform.ts
import type { BaseNodeData } from "./base";
import type { TransformKind, TransformParamsByKind } from "./paramsMap";

export type TransformNodeData<K extends TransformKind = TransformKind> =
    BaseNodeData<"transform", TransformParamsByKind[K]> & {
        transformKind: K; // ✅ structural discriminator
    };
