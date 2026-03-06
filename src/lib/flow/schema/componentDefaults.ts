import type { ComponentParams } from "$lib/flow/schema/component";

export const defaultComponentParams: ComponentParams = {
	componentRef: {
		componentId: "component_example",
		revisionId: "rev_1",
		apiVersion: "v1"
	},
	bindings: {
		inputs: {},
		config: {}
	},
	config: {},
	api: {
		inputs: [],
		outputs: []
	}
};

export const defaultComponentNodeData = {
	kind: "component" as const,
	componentKind: "graph_component" as const,
	label: "Component",
	params: defaultComponentParams,
	status: "idle" as const,
	ports: { in: "json" as const, out: "json" as const }
};

