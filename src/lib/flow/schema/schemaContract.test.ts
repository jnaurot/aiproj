import { describe, expect, it } from "vitest";

import { NodeSchemaEnvelopeSchema } from "./schemaContract";

describe("NodeSchemaEnvelopeSchema", () => {
	it("strips unknown keys and applies observation defaults", () => {
		const parsed = NodeSchemaEnvelopeSchema.parse({
			inferredSchema: {
				source: "sample",
				typedSchema: { type: "table", fields: [{ name: "id", type: "json", nullable: false }] },
				extra: "drop-me"
			},
			extraEnvelopeKey: true
		});

		expect(parsed).toEqual({
			inferredSchema: {
				source: "sample",
				state: "unknown",
				typedSchema: { type: "table", fields: [{ name: "id", type: "json", nullable: false }] }
			}
		});
	});

	it("supports expected and observed schema channels", () => {
		const parsed = NodeSchemaEnvelopeSchema.parse({
			expectedSchema: {
				source: "declared",
				state: "fresh",
				typedSchema: { type: "json", fields: [] },
				schemaFingerprint: "fp_expected"
			},
			observedSchema: {
				source: "runtime",
				state: "partial",
				typedSchema: { type: "json", fields: [] },
				updatedAt: "2026-03-10T00:00:00Z"
			}
		});

		expect(parsed.expectedSchema?.source).toBe("declared");
		expect(parsed.expectedSchema?.schemaFingerprint).toBe("fp_expected");
		expect(parsed.observedSchema?.source).toBe("runtime");
		expect(parsed.observedSchema?.state).toBe("partial");
	});
});

