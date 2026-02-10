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
- `job`: `list`, `get`, `log`, `build`, `abort`
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

Create a local env file:

```bash
cp .env.example .env
# then edit .env
```

Run directly in dev:

```bash
npm start
```

Run tests:

```bash
npm run check
npm test
```

Live integration tests (requires `.env`):

```bash
npm run test:integration
```

Optional destructive live suite:

```bash
DATAIKU_MCP_DESTRUCTIVE_TESTS=1 npm run test:integration
```

## Environment Variables

- `DATAIKU_URL`: DSS base URL
- `DATAIKU_API_KEY`: DSS API key
- `DATAIKU_PROJECT_KEY` (optional): default project key

## MCP Client Setup Guide

Use this server command in clients:

```json
{
  "command": "node",
  "args": ["/absolute/path/to/dataiku_mcp_skill/dist/index.js"],
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
  "args": ["tsx", "/absolute/path/to/dataiku_mcp_skill/src/index.ts"],
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
      "command": "node",
      "args": ["/absolute/path/to/dataiku_mcp_skill/dist/index.js"],
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
      "command": "node",
      "args": ["/absolute/path/to/dataiku_mcp_skill/dist/index.js"],
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
      "command": "node",
      "args": ["/absolute/path/to/dataiku_mcp_skill/dist/index.js"],
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
Update the path/env values and use it directly with Codex-compatible tooling.

## Recommended Verification Prompt

After adding the server in a client, run:

- `project` with `{ "action": "map", "projectKey": "YOUR_PROJECT_KEY" }`

You should receive normalized `nodes`, `edges`, `stats`, `roots`, and `leaves`.

## Notes

- `project.map` keeps `includeRaw` off by default for lower token usage.
- Arrays in normalized map output are deterministically sorted to reduce diff churn.

## Sources

- MCP local server connection docs: https://modelcontextprotocol.io/docs/develop/connect-local-servers
- Cursor MCP docs: https://cursor.com/docs/context/mcp
- Cline MCP docs: https://docs.cline.bot/mcp/configuring-mcp-servers
