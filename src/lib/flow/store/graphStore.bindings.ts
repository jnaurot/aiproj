// src/lib/flow/store/graphStore.bindings.ts
//
// Binding pair logic and related utilities.
// BindingPair is the minimal type needed by graphStore.types.ts to avoid
// circular imports.

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/** Represents the execKey / artifactId pair stored on a node binding. */
export type BindingPair = {
	execKey: string | null;
	artifactId: string | null;
};
