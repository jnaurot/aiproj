# Model Node Taxonomy

This project now treats model execution as a `kind=model` node family with backward-compatible support for legacy `kind=llm`.

## Taxonomy

| modelKind | taskKind options | default payload in/out |
| --- | --- | --- |
| `llm` | `generate`, `classify`, `extract` | `text -> text/json` |
| `vision` | `caption`, `classify`, `extract`, `generate` | `image -> text/json` |
| `audio` | `transcribe`, `extract`, `classify` | `audio -> text/json` |
| `embedding` | `embed` | `text -> embeddings` |
| `reranker` | `rerank` | `text -> json` |
| `multimodal` | `generate`, `classify`, `extract`, `caption`, `transcribe` | `text + media -> text/json` |

## Provider Matrix

| llmKind/provider | text/json | embeddings | image input | audio input |
| --- | --- | --- | --- | --- |
| `openai_compat` | yes | yes (`/v1/embeddings`) | yes | yes |
| `ollama` | yes | no (explicit unsupported error) | text-only path | no (explicit unsupported error) |

## Migration Notes (`llm` -> `model`)

1. Legacy graphs with `data.kind="llm"` still execute.
2. New saves should use `data.kind="model"`.
3. Use migration command for stored graph JSON:

```sh
cd backend
python scripts/migrate_llm_to_model.py --input path/to/graph.json --report path/to/report.json
```

Apply in-place with rollback artifact:

```sh
cd backend
python scripts/migrate_llm_to_model.py \
  --input path/to/graph.json \
  --apply \
  --report path/to/report.json \
  --rollback-dir path/to/rollbacks
```

## Request Policy Highlights

Model nodes support a unified `request_policy` block for:

- retries/timeout/backoff
- circuit breaker and fallback chain
- embedding micro-batching
- deterministic replay profile

## Prompt Revision Pinning

Use `params.promptRevisionId` (frontend) / `params.prompt_revision_id` (backend) to pin immutable prompt revisions and force cache invalidation when revision changes.
