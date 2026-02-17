# Dataiku MCP Server

MCP server for Dataiku DSS REST APIs, focused on flow analysis and reliable day-to-day operations (projects, datasets, recipes, jobs, scenarios, folders, variables, connections, and code environments).

## What You Get

- Deterministic normalized flow maps (`project.map`) with recipe subtypes and connectivity.
- Summary-first outputs with explicit raw/detail toggles where needed.
- Broad test coverage (unit + live integration + optional destructive integration suite).
- Strong error taxonomy in responses: `not_found`, `forbidden`, `validation`, `transient`, `unknown` with retry hints.

## Tool Coverage

- `project`: `list`, `get`, `metadata`, `flow`, `map`
- `dataset`: `list`, `get`, `schema`, `preview`, `metadata`, `download`, `create`, `update`, `delete`
- `recipe`: `list`, `get`, `create`, `update`, `delete`, `download`
- `job`: `list`, `get`, `log`, `build`, `buildAndWait`, `wait`, `abort`
- `scenario`: `list`, `run`, `status`, `get`, `create`, `update`, `delete`
- `managed_folder`: `list`, `get`, `contents`, `download`, `upload`, `delete_file`
- `variable`: `get`, `set`
- `connection`: `infer`
- `code_env`: `list`, `get`

## Prerequisites

- Node.js 20+
- npm
- Dataiku DSS URL + API key

## Quick Start

```bash
npm ci
npm run build
```

Run as a local CLI after build:

```bash
node dist/index.js
```

Use directly from npm (after publish):

```bash
npx -y dataiku-mcp
```

## Local Build And Testing

Recommended local workflow from repo root:

```bash
# install deps
npm ci

# static checks
npm run check

# unit tests
npm test

# build distribution
npm run build

# run MCP server locally (dev)
npm start
```

Optional live DSS integration tests:

```bash
# requires DATAIKU_URL, DATAIKU_API_KEY, DATAIKU_PROJECT_KEY in .env
npm run test:integration

# includes destructive actions (create/update/delete)
DATAIKU_MCP_DESTRUCTIVE_TESTS=1 npm run test:integration
```

## Repository Layout

- `src/`: MCP server and tool implementations.
- `tests/`: unit + integration test suites.
- `examples/`: demos, fixtures, artifacts, and ad-hoc local scripts.
- `bin/`: package executable entrypoint.
- `dist/`: compiled output (generated).

Create a local env file:

```bash
cp .env.example .env
# then edit .env
```

Run directly in dev:

```bash
npm start
```

Example scripts and sample outputs are kept under `examples/` to avoid root-level clutter.

## Environment Variables

- `DATAIKU_URL`: DSS base URL
- `DATAIKU_API_KEY`: DSS API key
- `DATAIKU_PROJECT_KEY` (optional): default project key
- `DATAIKU_REQUEST_TIMEOUT_MS` (optional): per-attempt request timeout in milliseconds (default: `30000`)
- `DATAIKU_RETRY_MAX_ATTEMPTS` (optional): max attempts for retry-enabled requests (`GET` only, default: `4`, cap: `10`)
- `DATAIKU_DEBUG_LATENCY` (optional): set to `1`/`true` to include per-tool timing diagnostics in `structuredContent.debug.latency` (off by default)

## MCP Client Setup Guide

Use this server command in clients (npm package):

```json
{
  "command": "npx",
  "args": ["-y", "dataiku-mcp"],
  "env": {
    "DATAIKU_URL": "https://your-dss-instance.app.dataiku.io",
    "DATAIKU_API_KEY": "your_api_key",
    "DATAIKU_PROJECT_KEY": "YOUR_PROJECT_KEY"
  }
}
```

Windows note: if your MCP client launches commands without a shell, use `npx.cmd`:

```json
{
  "command": "npx.cmd",
  "args": ["-y", "dataiku-mcp"],
  "env": {
    "DATAIKU_URL": "https://your-dss-instance.app.dataiku.io",
    "DATAIKU_API_KEY": "your_api_key",
    "DATAIKU_PROJECT_KEY": "YOUR_PROJECT_KEY"
  }
}
```

You can also run TypeScript directly during development:

```json
{
  "command": "npx",
  "args": ["tsx", "/absolute/path/to/Dataiku_MCP/src/index.ts"],
  "env": {
    "DATAIKU_URL": "https://your-dss-instance.app.dataiku.io",
    "DATAIKU_API_KEY": "your_api_key",
    "DATAIKU_PROJECT_KEY": "YOUR_PROJECT_KEY"
  }
}
```

### Claude Desktop

1. Open Claude Desktop -> `Settings` -> `Developer` -> `Edit Config`.
2. Add this under `mcpServers` in `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "dataiku": {
      "command": "npx",
      "args": ["-y", "dataiku-mcp"],
      "env": {
        "DATAIKU_URL": "https://your-dss-instance.app.dataiku.io",
        "DATAIKU_API_KEY": "your_api_key",
        "DATAIKU_PROJECT_KEY": "YOUR_PROJECT_KEY"
      }
    }
  }
}
```

### Cursor

Cursor supports both project-scoped and global MCP config:

- Project: `.cursor/mcp.json`
- Global: `~/.cursor/mcp.json`

Example:

```json
{
  "mcpServers": {
    "dataiku": {
      "command": "npx",
      "args": ["-y", "dataiku-mcp"],
      "env": {
        "DATAIKU_URL": "https://your-dss-instance.app.dataiku.io",
        "DATAIKU_API_KEY": "your_api_key",
        "DATAIKU_PROJECT_KEY": "YOUR_PROJECT_KEY"
      }
    }
  }
}
```

### Cline (VS Code extension)

1. Open Cline -> MCP Servers -> Configure MCP Servers.
2. Add this server block in `cline_mcp_settings.json`:

```json
{
  "mcpServers": {
    "dataiku": {
      "command": "npx",
      "args": ["-y", "dataiku-mcp"],
      "env": {
        "DATAIKU_URL": "https://your-dss-instance.app.dataiku.io",
        "DATAIKU_API_KEY": "your_api_key",
        "DATAIKU_PROJECT_KEY": "YOUR_PROJECT_KEY"
      }
    }
  }
}
```

### Codex / project-level MCP config

This repo already includes a project-scoped MCP file at `.mcp.json`.
The checked-in `.mcp.json` uses `node node_modules/tsx/dist/cli.mjs src/index.ts` for cross-platform startup (including Windows); run `npm ci` first.

## NPM Release Workflow

This repo includes a manual GitHub Actions release workflow:

- Workflow file: `.github/workflows/release.yml`
- Trigger: `Actions` -> `Release NPM Package` -> `Run workflow`

Inputs:

- `bump`: `patch | minor | major`
- `version`: optional exact version (overrides `bump`)
- `publish`: whether to publish to npm

Required repository configuration:

- GitHub variable: `NPM_RELEASE_ENABLED=true`
- Optional variable: `NPM_PUBLISH_ACCESS=public`
- Trusted publisher configured on npmjs.com for this package/repo/workflow

The workflow will:

1. Install dependencies, run checks/tests, and build.
2. Bump package version and create git tag.
3. Push commit + tag to `main`.
4. Publish to npm with GitHub OIDC trusted publishing (if `publish=true`).
5. Create a GitHub Release with generated notes.

Trusted publishing setup (npm):

1. Open `https://www.npmjs.com/package/dataiku-mcp` -> `Settings` -> `Trusted Publisher`.
2. Choose `GitHub Actions`.
3. Set:
   - Organization or user: `clssck`
   - Repository: `Dataiku_MCP`
   - Workflow filename: `release.yml`
4. Save.

## Official MCP Registry

This repo is configured for MCP Registry publishing:

- Metadata file: `server.json`
- Workflow: `.github/workflows/publish-mcp-registry.yml`
- Required package field: `mcpName` in `package.json`

Server namespace:

- `io.github.clssck/dataiku-mcp`

Publish paths:

1. Manual: run `Publish to MCP Registry` in GitHub Actions.
2. Automatic: run the npm release workflow with `publish=true` (it triggers MCP Registry publish).

Validation notes:

- `server.json.name` must match `package.json.mcpName`.
- `server.json.packages[].identifier` + `version` must reference a real npm publish.

## Recommended Verification Prompt

After adding the server in a client, run:

- `project` with `{ "action": "map", "projectKey": "YOUR_PROJECT_KEY" }` (defaults to `maxNodes=300`, `maxEdges=600`; override as needed)

You should receive a flow summary in text and normalized `nodes`, `edges`, `stats`, `roots`, and `leaves` under `structuredContent.map`.
When truncation limits are applied (default `maxNodes=300`, `maxEdges=600`), `structuredContent.truncation` reports before/after node+edge counts and whether truncation occurred.

## Notes

- `project.map` returns a compact text summary; full normalized graph is in `structuredContent.map`.
- Arrays in normalized map output are deterministically sorted to reduce diff churn.
- `job.wait` and `job.buildAndWait` include `structuredContent.normalizedState` with one of `terminalSuccess | terminalFailure | timeout | nonTerminal` while preserving raw DSS `state`.
- With `DATAIKU_DEBUG_LATENCY=1`, responses include per-tool and per-API-call latency metrics under `structuredContent.debug.latency`.
- List-style responses are token-bounded by default; use `limit`/`offset` (and action-specific caps like `maxNodes`, `maxEdges`, `maxKeys`, `maxPackages`) to page or expand results when needed.
- `dataset.get` and `job.get` are summary-first by default; pass `includeDefinition=true` to include full DSS JSON in `structuredContent.definition`.

## Sources

- MCP local server connection docs: https://modelcontextprotocol.io/docs/develop/connect-local-servers
- Cursor MCP docs: https://cursor.com/docs/context/mcp
- Cline MCP docs: https://docs.cline.bot/mcp/configuring-mcp-servers
