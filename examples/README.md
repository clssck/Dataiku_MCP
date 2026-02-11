# Examples

This folder holds non-core assets used for demos, local experiments, and output inspection.

## Structure

- `artifacts/`: flow map snapshots and generated summaries.
- `fixtures/`: sample downloaded payloads/data files used for manual checks.
- `scripts/`: local ad-hoc scripts for API probing and MCP smoke testing.
- `output/`: generated script outputs (ignored by git except `.gitkeep`).

## Running example scripts

From repo root:

```bash
npx tsx examples/scripts/test.ts
npx tsx examples/scripts/test-api.ts
```

Scripts read `.env` from repo root.
