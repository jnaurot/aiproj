# Backend Requirement Profiles

This repo now uses layered requirement profiles so installs can stay small by default and expand only when needed.

## Default install

From `backend/`:

```powershell
python -m pip install -r requirements.txt
```

Default includes:
- `core.txt` (API/runtime + core data stack)
- `compat.txt` (pandas compatibility)
- `testing.txt` (pytest stack)

## Optional install layers

Install only what your workload needs:

```powershell
python -m pip install -r requirements/llm.txt
python -m pip install -r requirements/dl.txt
python -m pip install -r requirements/finetune.txt
python -m pip install -r requirements/observability.txt
```

## Profiles

- `core.txt`: FastAPI runtime, `polars`, `numpy`, `scipy`, `scikit-learn`, `pyarrow`, core clients.
- `compat.txt`: `pandas` for existing pandas-centric executors and legacy compatibility.
- `llm.txt`: tokenization/fuzzy matching/sentence-transformers helpers.
- `dl.txt`: `torch` runtime.
- `finetune.txt`: `transformers`, `datasets`, `accelerate`, `peft`, `trl`, optional `bitsandbytes`.
- `observability.txt`: `structlog`.
- `testing.txt`: pytest and test utilities.

## Notes

- `bitsandbytes` can be platform-sensitive (especially on Windows). Keep it optional.
- The repository currently uses pandas in runtime codepaths, so `compat.txt` remains in default install.
