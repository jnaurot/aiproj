import { z } from "zod";

export type NormalizeResult<T> =
  | { ok: true; value: T }
  | { ok: false; error: string };

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === "object" && !Array.isArray(v);
}

/**
 * Deep merge:
 * - objects merge recursively
 * - arrays are replaced
 * - primitives replace
 * - `undefined` means “no change”
 */
function deepMerge(base: unknown, patch: unknown): unknown {
  if (patch === undefined) return base;
  if (!isPlainObject(base) || !isPlainObject(patch)) return patch;

  const out: Record<string, unknown> = { ...base };
  for (const [k, v] of Object.entries(patch)) {
    const bv = (out as any)[k];
    (out as any)[k] = isPlainObject(bv) && isPlainObject(v) ? deepMerge(bv, v) : v;
  }
  return out;
}

/**
 * Schema-authoritative normalization:
 * defaults → existing → patch, then parse, then return parsed.data only (hard strip).
 *
 * NOTE: `existing` is whatever is currently persisted.
 *       `patch` is what the user submitted (on Apply/select).
 */
export function normalizeWithDefaults<T>(
  schema: z.ZodType<T>,
  defaults: T,
  existing: unknown,
  patch: unknown
): NormalizeResult<T> {
  console.log("schema: "+JSON.stringify(schema))
  console.log("patch: "+JSON.stringify(patch))
  const merged = deepMerge(deepMerge(defaults, existing ?? {}), patch ?? {});

  const res = schema.safeParse(merged);
  if (!res.success) {
    const msg = res.error.issues
      .map((i) => {
        const path = i.path?.length ? i.path.join(".") : "(root)";
        return `${path}: ${i.message}`;
      })
      .join("; ");
    return { ok: false, error: msg };
  }

  // Hard strip happens here: only parsed data survives.
  return { ok: true, value: res.data };
}
