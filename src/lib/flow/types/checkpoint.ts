/**
 * A Merkle-style fingerprint encoding (nodeKind + serialized params + inputArtifactIds).
 * Produced by the backend before each node execution.
 */
export type MemoKey = string; // sha256 hex, 64 chars

/**
 * The result of a memoization cache lookup as reported in run trace events.
 */
export type MemoLookupResult =
	| { hit: true; artifactId: string; execKey: string; memoKey: MemoKey }
	| { hit: false; memoKey: MemoKey };

/**
 * Staleness status of a checkpoint relative to the current run context.
 */
export type CheckpointStaleness =
	| 'valid'
	| 'stale'
	| 'artifact_missing'
	| 'unknown';

/**
 * A named, explicit, user-created snapshot of a node's execution output.
 */
export type CheckpointRecord = {
	id: string;
	name: string;
	description?: string;
	nodeId: string;
	graphId: string;
	runId: string;
	artifactId: string;
	execKey: string;
	fingerprintAtCreation: MemoKey;
	createdAt: string;
	staleness: CheckpointStaleness;
	outputs?: Record<
		string,
		{
			artifactId: string;
			execKey?: string;
		}
	>;
};

/**
 * Per-graph registry mapping nodeId -> active checkpoint record.
 */
export type CheckpointRegistry = Record<string, CheckpointRecord>;

/**
 * Payload shape sent in run request replacing legacy pin hints.
 */
export type CheckpointExecutionHints = {
	checkpoints: Record<
		string,
		{
			artifactId: string;
			execKey: string;
			fingerprintAtCreation: MemoKey;
			outputs?: Record<string, { artifactId: string; execKey?: string }>;
		}
	>;
};

