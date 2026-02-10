import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { register as registerCodeEnvs } from "./tools/code-envs.js";
import { register as registerConnections } from "./tools/connections.js";
import { register as registerDatasets } from "./tools/datasets.js";
import { register as registerFolders } from "./tools/folders.js";
import { register as registerJobs } from "./tools/jobs.js";
import { register as registerProjects } from "./tools/projects.js";
import { register as registerRecipes } from "./tools/recipes.js";
import { register as registerScenarios } from "./tools/scenarios.js";
import { register as registerVariables } from "./tools/variables.js";

const server = new McpServer({
	name: "dataiku",
	version: "1.0.0",
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

const transport = new StdioServerTransport();
await server.connect(transport);
