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

	it("supports expected per-handle input/output and observed schema channels", () => {
		const parsed = NodeSchemaEnvelopeSchema.parse({
			expectedInputSchemas: {
				in: {
					source: "declared",
					state: "fresh",
					typedSchema: { type: "text", fields: [] }
				}
			},
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

		expect(parsed.expectedInputSchemas?.in?.source).toBe("declared");
		expect(parsed.expectedInputSchemas?.in?.typedSchema?.type).toBe("text");
		expect(parsed.expectedSchema?.source).toBe("declared");
		expect(parsed.expectedSchema?.schemaFingerprint).toBe("fp_expected");
		expect(parsed.observedSchema?.source).toBe("runtime");
		expect(parsed.observedSchema?.state).toBe("partial");
	});

	it("supports split input contracts by affinity", () => {
		const parsed = NodeSchemaEnvelopeSchema.parse({
			workInputs: {
				defaultSchema: {
					source: "declared",
					typedSchema: { type: "json", fields: [] }
				},
				handles: {
					in: {
						source: "declared",
						typedSchema: { type: "json", fields: [] }
					}
				}
			},
			paramInputs: {
				handles: {
					param_filters: {
						source: "declared",
						typedSchema: { type: "json", fields: [] }
					}
				}
			},
			controlInputs: {
				handles: {
					control_in: {
						source: "declared",
						typedSchema: { type: "text", fields: [] }
					}
				}
			}
		});

		expect(parsed.workInputs?.defaultSchema?.typedSchema?.type).toBe("json");
		expect(parsed.workInputs?.handles?.in?.typedSchema?.type).toBe("json");
		expect(parsed.paramInputs?.handles?.param_filters?.typedSchema?.type).toBe("json");
		expect(parsed.controlInputs?.handles?.control_in?.typedSchema?.type).toBe("text");
	});
});

