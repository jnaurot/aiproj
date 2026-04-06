# Node Kind Quick Fields

## Source

### File
- Always examine: `data.sourceKind`, `data.params.file.name|file_name|snapshot.name`, `data.params.format|file_format`
- Examine if meaningful: `data.params.delimiter`, `data.params.hasHeader`, `data.params.encoding`
- Usually ignore: layout metadata, run-only cache state, runtime artifact ids
- Description pattern: "Loads `<format>` file `<name>` for downstream use"

### API
- Always examine: `data.sourceKind`, `data.params.url`, `data.params.method`
- Examine if meaningful: auth mode, query params, headers, pagination/retry behavior
- Usually ignore: unchanged default transport knobs
- Description pattern: "Calls `<method>` `<url>` and emits response payloads"

### Database
- Always examine: `data.sourceKind`, `data.params.table|query`, connection reference
- Examine if meaningful: filters, limits, incremental windowing
- Usually ignore: UI-only editor state
- Description pattern: "Reads `<table|query>` from database and emits rows"

### Object Store
- Always examine: `data.sourceKind`, bucket/container, key/prefix
- Examine if meaningful: format decoder, recursive listing behavior
- Usually ignore: unrelated local file settings
- Description pattern: "Fetches object data from `<bucket/prefix>` for downstream processing"

### Warehouse
- Always examine: `data.sourceKind`, warehouse query/table target
- Examine if meaningful: compute profile, materialization mode
- Usually ignore: defaults unrelated to retrieval semantics
- Description pattern: "Queries warehouse data from `<source>` and outputs tabular results"

## Transform

### Select / Project
- Always examine: transform subtype/op, selected fields/columns
- Examine if meaningful: rename/coercion behavior
- Usually ignore: untouched defaults
- Description pattern: "Projects records to `<selected fields>`"

### JSON Filter
- Always examine: rule conditions, pass/reject behavior
- Examine if meaningful: strictness and reject reasons
- Usually ignore: debug-only verbosity
- Description pattern: "Filters JSON items using `<rules>` and routes pass/reject outputs"

### Dedupe
- Always examine: key columns (`by`) or all-column mode
- Examine if meaningful: keep policy (first/last)
- Usually ignore: incidental ordering unless required
- Description pattern: "Removes duplicate rows using `<columns|all columns>`"

### Aggregate
- Always examine: `groupBy`, metric definitions
- Examine if meaningful: aggregation functions (`sum|avg|count|min|max`)
- Usually ignore: display-only sort preferences
- Description pattern: "Aggregates by `<groupBy>` computing `<metrics>`"

### Derive
- Always examine: derived expressions/column formulas
- Examine if meaningful: type coercion/default handling
- Usually ignore: unchanged helper fields
- Description pattern: "Derives new fields `<derived columns>` from existing values"

## Model
### Ollama (Translation)
- Always examine: `node_label`, `settings.user_prompt`, `planes.data_inputs`, `planes.data_input_sources`
- suggested_fields: `user_prompt`, `data_input_sources`, `data_inputs`
- Examine if meaningful: `settings.system_prompt`, `settings.model`, `settings.output_mode`
- Usually ignore: `runtime.pending_input_count`, `runtime.inflight`, `runtime.ready_work`
- Description pattern: "`<node_label>` reads `<input_name>` from `<upstream_node_kind>` and `<task from user_prompt>`"
- Prompt-to-task normalization:
  - "translate to spanish" -> "translate to Spanish"
  - "summarize" -> "summarize text"
- Example output:
  - `Model_Spanish reads summarize from Component and translate to Spanish`


### OpenAI-Compatible
- Always examine: `params.user_prompt`, `params.model`, provider/base URL
- Examine if meaningful: response format/schema constraints
- Usually ignore: unchanged defaults
- Description pattern: "Uses OpenAI-compatible model `<model>` to produce `<output>` for `<task>`"

### General Guidance (all model subtypes)
- Always examine: expected output contract/schema and error policy
- Examine if meaningful: retries/timeouts, thinking/eval settings if enabled
- Usually ignore: internal debug flags for end-user summary text
- Description pattern: "Consumes input item(s), runs inference, emits structured response"

## Tool

### Built-in
- Always examine: tool identity/name and required args
- Examine if meaningful: side-effect scope, timeout/retry, auth context
- Usually ignore: unrelated provider defaults
- Description pattern: "Calls `<tool>` to `<action>` with `<key args>`"

### Custom / External
- Always examine: endpoint/command target, input/output contract
- Examine if meaningful: safety limits and failure behavior
- Usually ignore: UI-only expansion state
- Description pattern: "Invokes external tool `<name>` for `<purpose>`"

## Component

### Graph Component
- Always examine: `componentId`, `revisionId`, exposed API contract (inputs/outputs)
- Examine if meaningful: required exposed outputs (`req`), instance config payload
- Usually ignore: internal nodes not exposed by contract when writing consumer-facing summary
- Description pattern: "Executes component `<name>@<revision>` exposing `<inputs/outputs>`"

### Component Summary Guidance
- Always examine: published contract semantics over internal implementation details
- Examine if meaningful: instance pin/upgrade state and compatibility notes
- Usually ignore: historical revision metadata not active on this instance
- Description pattern: "Wraps a reusable internal graph behind a stable external contract"
