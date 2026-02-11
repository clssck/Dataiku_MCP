import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { register as registerCodeEnvs } from "./tools/code-envs.js";
import { register as registerConnections } from "./tools/connections.js";
import { register as registerDatasets } from "./tools/datasets.js";
import { register as registerFolders } from "./tools/folders.js";
import { register as registerJobs } from "./tools/jobs.js";
import { register as registerProjects } from "./tools/projects.js";
import { register as registerRecipes } from "./tools/recipes.js";
import { register as registerScenarios } from "./tools/scenarios.js";
import { register as registerVariables } from "./tools/variables.js";

function resolveVersion(): string {
	try {
		const raw = readFileSync(join(process.cwd(), "package.json"), "utf8");
		const parsed = JSON.parse(raw) as { version?: unknown };
		return typeof parsed.version === "string" ? parsed.version : "0.0.0";
	} catch {
		return "0.0.0";
	}
}

export function createServer() {
	const server = new McpServer({
		name: "dataiku",
		version: resolveVersion(),
	});

	registerProjects(server);
	registerDatasets(server);
	registerRecipes(server);
	registerJobs(server);
	registerScenarios(server);
	registerVariables(server);
	registerFolders(server);
	registerConnections(server);
	registerCodeEnvs(server);

	return server;
}

export function createSandboxServer() {
	return createServer();
}
