# AI Project Flow Editor - Agent Guide

## Overview

This is a flow-based pipeline editor built with SvelteKit 2, Svelte 5, TypeScript, and Tailwind CSS 4. It provides a visual interface for creating and executing data processing pipelines with nodes representing sources, transformations, LLM operations, and tools.

## Tech Stack

- **Framework**: SvelteKit 2, Svelte 5
- **Language**: TypeScript
- **Styling**: Tailwind CSS 4
- **Build Tool**: Vite
- **Validation**: Zod
- **Visual Editor**: @xyflow/svelte
- **Code Formatting**: Prettier

## Development Commands

```bash
npm run dev          # Start development server
npm run build        # Build for production
npm run preview      # Preview production build
npm run lint         # Check code formatting (Prettier)
npm run format       # Format code with Prettier
```

## Architecture

### Node Types

The system supports four node types, each with specific parameters and port types:

1. **Source Nodes** (`source`)
   - Input: None (source of data)
   - Output: `table`, `text`, `json`, `binary`
   - Kinds: `file`, `database`, `api`
   - Example: File reader, database query, API fetcher

2. **Transform Nodes** (`transform`)
   - Input: `table`, `text`, `json`, `binary`
   - Output: `table`, `text`, `json`, `binary`
   - Kinds: `filter`, `select`, `rename`, `derive`, `aggregate`, `join`, `sort`, `limit`, `dedupe`, `sql`, `python`, `js`
   - Example: Data filtering, column selection, aggregation, custom code execution

3. **LLM Nodes** (`llm`)
   - Input: `text`, `chat`, `embeddings`
   - Output: `text`, `json`, `markdown`
   - Kinds: `ollama`, `openai_compat`
   - Example: Text generation, summarization, embeddings

4. **Tool Nodes** (`tool`)
   - Input: `table`, `text`, `json`, `binary`
   - Output: `json`, `text`, `binary`
   - Providers: `mcp`, `http`, `function`, `python`, `shell`, `db`, `builtin`
   - Example: HTTP requests, database queries, custom functions

### Port Types

All port types are defined in `src/lib/flow/types/base.ts`:
- `table` - Tabular data (DataFrame)
- `text` - Text content
- `json` - JSON data
- `binary` - Binary data
- `chat` - Chat messages
- `embeddings` - Vector embeddings

### Data Flow

1. **Source nodes** generate data
2. **Transform nodes** process data
3. **LLM nodes** generate text/embeddings
4. **Tool nodes** perform external operations
5. Data flows through edges based on port types

## Code Patterns

### Zod Schema Validation

All schemas use Zod with `.strip()` to reject unknown keys:

```typescript
import { z } from "zod";

export const TransformFilterParamsSchema = z.object({
  expr: z.string().min(1, "Filter expression cannot be empty"),
}).strip();
```

### Type Inference

Types are inferred from Zod schemas:

```typescript
export type TransformFilterParams = z.infer<typeof TransformFilterParamsSchema>;
```

### Node Data Structure

All node data extends `BaseNodeData`:

```typescript
export type BaseNodeData<K extends NodeKind, P> = {
  kind: K;
  label: string;
  params: P;
  status: NodeStatus;

  lastRunId?: string;
  lastStartedAt?: string;
  lastEndedAt?: string;
  error?: { message: string; code?: string; details?: unknown };

  ports?: { in?: PortType | null; out?: PortType | null };
  meta?: NodeMeta;
};
```

### Discriminated Unions

Node kinds use discriminated unions for type safety:

```typescript
// Source nodes
export type SourceNodeData<K extends SourceKind = SourceKind> =
    BaseNodeData<"source", SourceParamsByKind[K]> & {
        sourceKind: K;
    };

// Transform nodes
export type TransformNodeData<K extends TransformKind = TransformKind> =
    BaseNodeData<"transform", TransformParamsByKind[K]> & {
        transformKind: K;
    };

// LLM nodes
export type LlmNodeData = BaseNodeData<"llm", LlmParams> & {
  llmKind: LlmKind;
};
```

### Common Parameters

Transform nodes share common parameters:

```typescript
const TransformCommonSchema = z
  .object({
    enabled: z.boolean().default(true),
    notes: z.string().optional().default(""),
    cache: z.object({
      enabled: z.boolean().default(false),
      key: z.string().optional()
    }).optional().default({ enabled: false })
  })
  .strip();
```

## File Structure

```
src/lib/flow/
├── types/              # TypeScript type definitions
│   ├── base.ts        # Base types, port types, node status
│   ├── source.ts      # Source node types
│   ├── transform.ts   # Transform node types
│   ├── llm.ts         # LLM node types
│   ├── tool.ts        # Tool node types
│   ├── pipeline.ts    # Pipeline node union
│   ├── graph.ts       # Graph DTO types
│   └── run.ts         # Run event types
├── schema/            # Zod validation schemas
│   ├── base.ts        # Base node schema
│   ├── source.ts      # Source schemas
│   ├── transform.ts   # Transform schemas
│   ├── llm.ts         # LLM schemas
│   └── tool.ts        # Tool schemas
├── components/        # UI components
│   ├── nodes/         # Node components
│   └── editors/       # Editor components
└── lib/               # Utility functions
```

## Node Status

Node status values (defined in `src/lib/flow/types/base.ts`):
- `idle` - Node is ready, not executed
- `stale` - Node needs re-execution
- `running` - Node is currently executing
- `succeeded` - Node execution completed successfully
- `failed` - Node execution failed
- `skipped` - Node was skipped
- `canceled` - Node execution was canceled

## Edge Execution State

Edge execution states (defined in `src/lib/flow/types/base.ts`):
- `idle` - Edge is not executing
- `active` - Edge is currently transferring data
- `done` - Edge execution completed

## Code Style

- **Formatter**: Prettier
- **Config**: `prettier.config.js` (if exists) or use default
- **Tailwind**: Use Tailwind CSS utility classes
- **Comments**: Use JSDoc for exported functions and types

## Best Practices

1. **Always use Zod schemas** for validation - they provide runtime type safety
2. **Use `.strip()`** on all Zod schemas to reject unknown keys
3. **Infer types from Zod** - never manually define types that can be inferred
4. **Use discriminated unions** for node kinds to ensure type safety
5. **Follow the BaseNodeData pattern** for all node types
6. **Keep schemas in `src/lib/flow/schema/`** and types in `src/lib/flow/types/`
7. **Use meaningful port types** to ensure data compatibility
8. **Handle errors gracefully** with proper error objects

## Adding New Node Types

1. Add the kind to the appropriate enum in `src/lib/flow/types/base.ts`
2. Create the Zod schema in `src/lib/flow/schema/`
3. Infer the type from the schema
4. Create the node data type in `src/lib/flow/types/`
5. Add default values in the corresponding `*Defaults.ts` file
6. Create the UI component in `src/lib/flow/components/nodes/`
7. Create the editor component in `src/lib/flow/components/editors/`

## Testing

The project uses SvelteKit's testing utilities. Tests should be placed in `src/lib/flow/__tests__/` or similar directories.

## Notes

- The project uses `@xyflow/svelte` for the visual editor
- SSE (Server-Sent Events) are used for real-time updates via `sveltekit-sse`
- The system supports caching for transform nodes
- All schemas are strict and reject unknown keys