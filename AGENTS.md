# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Project-Specific Conventions

### Code Style
- **Tabs**: Use tabs (not spaces) - `.prettierrc` enforces `"useTabs": true`
- **Tailwind**: Use Tailwind CSS utility classes with `prettier-plugin-tailwindcss`

### Backend-Specific
- **Python Backend**: Located in `backend/` directory with FastAPI
- **Test Running**: Backend tests use pytest, run from backend directory: `cd backend && python -m pytest`
- **Port Capabilities**: Defined in `shared/port_capabilities.v1.json` for frontend-backend compatibility

### Non-Obvious Patterns
- **Zod `.strip()`**: All Zod schemas MUST use `.strip()` to reject unknown keys
- **Type Inference**: Always infer types from Zod schemas using `z.infer<typeof Schema>`
- **Discriminated Unions**: Node kinds use discriminated unions for type safety
- **BaseNodeData Pattern**: All node data types extend `BaseNodeData` with kind-specific params

### Critical Gotchas
- **Prettier Config**: Uses tabs, not spaces - this is non-standard for most projects
- **Backend Separation**: Backend is a separate Python project in `backend/` directory
- **Port Type Contracts**: Frontend and backend must agree on port types defined in `shared/port_capabilities.v1.json`
