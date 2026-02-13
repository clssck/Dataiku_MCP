import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { register as registerCodeEnvs } from "../../src/tools/code-envs.js";
import { register as registerConnections } from "../../src/tools/connections.js";
import { register as registerDatasets } from "../../src/tools/datasets.js";
import { register as registerFolders } from "../../src/tools/folders.js";
import { register as registerJobs } from "../../src/tools/jobs.js";
// Register all tools
import { register as registerProjects } from "../../src/tools/projects.js";
import { register as registerRecipes } from "../../src/tools/recipes.js";
import { register as registerScenarios } from "../../src/tools/scenarios.js";
import { register as registerVariables } from "../../src/tools/variables.js";

const HAS_DSS =
   !!process.env.DATAIKU_URL &&
   !!process.env.DATAIKU_API_KEY &&
   !!process.env.DATAIKU_PROJECT_KEY;

describe.skipIf(!HAS_DSS)("Integration: MCP tools against live DSS", () => {
   let client: Client;
   let server: McpServer;

   async function call(tool: string, args: Record<string, unknown>) {
      const result = await client.callTool({ name: tool, arguments: args });
      const text = (result.content as Array<{ type: string; text: string }>)[0]
         ?.text;
      return { text, isError: typeof result.isError === "boolean" ? result.isError : undefined };
   }

   beforeAll(async () => {
      server = new McpServer({ name: "test", version: "0.0.1" });
      registerProjects(server);
      registerDatasets(server);
      registerRecipes(server);
      registerJobs(server);
      registerScenarios(server);
      registerVariables(server);
      registerFolders(server);
      registerConnections(server);
      registerCodeEnvs(server);

      const [clientTransport, serverTransport] =
         InMemoryTransport.createLinkedPair();
      await server.connect(serverTransport);

      client = new Client({ name: "test-client", version: "0.0.1" });
      await client.connect(clientTransport);
   });

   afterAll(async () => {
      await client?.close();
      await server?.close();
   });

   // ─── Project ────────────────────────────────────────────
   describe("project", () => {
      it("list — lists accessible projects", async () => {
         const { text, isError } = await call("project", { action: "list" });
         expect(isError).toBeFalsy();
         expect(text).toContain("•");
      });

      it("get — gets project details", async () => {
         const { text, isError } = await call("project", { action: "get" });
         expect(isError).toBeFalsy();
         expect(text).toContain("Project:");
         expect(text).toContain("Status:");
      });

      it("metadata — gets project metadata", async () => {
         const { text, isError } = await call("project", { action: "metadata" });
         expect(isError).toBeFalsy();
         expect(text).toBeDefined();
         expect(text).toMatch(/Tags:|Label:|Custom fields:/);
      });

      it("flow — gets flow graph", async () => {
         const { text, isError } = await call("project", { action: "flow" });
         expect(isError).toBeFalsy();
         expect(text).toContain("Flow:");
         expect(text).toContain("Pipeline:");
      });

      it("map — gets normalized flow map with connectivity", async () => {
         const { text, isError } = await call("project", { action: "map" });
         expect(isError).toBeFalsy();
         const data = JSON.parse(text);
         expect(Array.isArray(data.nodes)).toBe(true);
         expect(Array.isArray(data.edges)).toBe(true);
         expect(data.stats).toBeDefined();
         expect(typeof data.stats.nodeCount).toBe("number");

         const recipeNode = data.nodes.find(
            (node: { kind?: string }) => node.kind === "recipe",
         );
         if (recipeNode) {
            expect(recipeNode.subtype === undefined || typeof recipeNode.subtype === "string").toBe(
               true,
            );
         }
      });

      it("map — includes raw flow payload when includeRaw=true", async () => {
         const { text, isError } = await call("project", {
            action: "map",
            includeRaw: true,
         });
         expect(isError).toBeFalsy();
         const data = JSON.parse(text);
         expect(data.raw).toBeDefined();
         expect(Array.isArray(data.nodes)).toBe(true);
         expect(Array.isArray(data.edges)).toBe(true);
      });
   });

   // ─── Connection ─────────────────────────────────────────
   describe("connection", () => {
      it("infer — lists connections from datasets", async () => {
         const { text, isError } = await call("connection", { action: "infer" });
         expect(isError).toBeFalsy();
         expect(text).toBeDefined();
      });
   });

   // ─── Variable ───────────────────────────────────────────
   describe("variable", () => {
      it("get — reads project variables", async () => {
         const { text, isError } = await call("variable", { action: "get" });
         expect(isError).toBeFalsy();
         expect(text).toContain("Variables:");
         expect(text).toContain("Standard keys:");
         expect(text).toContain("Local keys:");
      });

      it("set — merges a variable and verifies", async () => {
         const testVal = `test_${Date.now()}`;
         const { isError } = await call("variable", {
            action: "set",
            standard: { _integration_test: testVal },
         });
         expect(isError).toBeFalsy();

         // Verify
         const { text } = await call("variable", { action: "get" });
         expect(text).toContain("_integration_test");
      });
   });

   // ─── Dataset CRUD ───────────────────────────────────────
   describe("dataset", () => {
      const DS_NAME = `_test_ds_${Date.now()}`;

      it("list — lists datasets", async () => {
         const { isError } = await call("dataset", { action: "list" });
         expect(isError).toBeFalsy();
      });

      it("create — creates a dataset", async () => {
         const { text: connText } = await call("connection", { action: "infer" });
         const connMatch = connText.match(/• (\S+)/);
         const connection = connMatch?.[1] ?? "filesystem_managed";

         const { text, isError } = await call("dataset", {
            action: "create",
            datasetName: DS_NAME,
            connection,
         });
         expect(isError).toBeFalsy();
         expect(text).toContain("created");
      });

      it("get — gets dataset definition", async () => {
         const { text, isError } = await call("dataset", {
            action: "get",
            datasetName: DS_NAME,
         });
         expect(isError).toBeFalsy();
         expect(text).toContain(`Dataset: ${DS_NAME}`);
      });

      it("schema — gets dataset schema", async () => {
         const { isError } = await call("dataset", {
            action: "schema",
            datasetName: DS_NAME,
         });
         expect(isError).toBeFalsy();
      });

      it("metadata — gets dataset metadata", async () => {
         const { isError } = await call("dataset", {
            action: "metadata",
            datasetName: DS_NAME,
         });
         expect(isError).toBeFalsy();
      });

      it("delete — deletes the dataset", async () => {
         const { text, isError } = await call("dataset", {
            action: "delete",
            datasetName: DS_NAME,
         });
         expect(isError).toBeFalsy();
         expect(text).toContain("deleted");
      });

      it("preview — previews dataset rows (requires data)", async () => {
         const { text: listText } = await call("dataset", { action: "list" });
         const dsMatch = listText.match(/• (\S+)/);
         if (!dsMatch) return;

         const { text, isError } = await call("dataset", {
            action: "preview",
            datasetName: dsMatch[1],
            limit: 5,
         });
         if (!isError) {
            expect(text).toBeDefined();
         }
      });
   });

   // ─── Recipe CRUD ────────────────────────────────────────
   describe("recipe", () => {
      const RECIPE_OUT = `_test_recipe_out_${Date.now()}`;
      const RECIPE_NAME = `python_${RECIPE_OUT}`;

      it("list — lists recipes", async () => {
         const { isError } = await call("recipe", { action: "list" });
         expect(isError).toBeFalsy();
      });

      it("create — creates a Python recipe", async () => {
         const { text: listText } = await call("dataset", { action: "list" });
         const dsMatch = listText.match(/• (\S+)/);
         if (!dsMatch) return;

         const { text, isError } = await call("recipe", {
            action: "create",
            type: "python",
            inputDatasets: [dsMatch[1]],
            outputDataset: RECIPE_OUT,
            payload:
               "import dataiku\nimport pandas as pd\n\nds = dataiku.Dataset(dataiku.get_flow_variables()['input'])\ndf = ds.get_dataframe()\n\nout = dataiku.Dataset(dataiku.get_flow_variables()['output'])\nout.write_with_schema(df)",
         });
         expect(isError).toBeFalsy();
         expect(text).toContain("created");
      });

      it("get — gets recipe definition", async () => {
         const { text, isError } = await call("recipe", {
            action: "get",
            recipeName: RECIPE_NAME,
         });
         if (isError) return;
         expect(text).toContain(`Recipe: ${RECIPE_NAME}`);
      });

      it("get — includes payload body when includePayload=true", async () => {
         const { text, isError } = await call("recipe", {
            action: "get",
            recipeName: RECIPE_NAME,
            includePayload: true,
            payloadMaxLines: 5,
         });
         if (isError) return;
         expect(text).toContain("Payload:");
         expect(text).toContain("Payload Body:");
      });

      it("delete — deletes recipe and cleanup dataset", async () => {
         const { isError: recipeErr } = await call("recipe", {
            action: "delete",
            recipeName: RECIPE_NAME,
         });
         if (!recipeErr) {
            const { isError: dsErr } = await call("dataset", {
               action: "delete",
               datasetName: RECIPE_OUT,
            });
            expect(dsErr).toBeFalsy();
         }
      });
   });

   // ─── Job ────────────────────────────────────────────────
   describe("job", () => {
      it("list — lists recent jobs", async () => {
         const { isError } = await call("job", { action: "list" });
         expect(isError).toBeFalsy();
      });

      it("get — gets a job by ID (uses most recent)", async () => {
         const { text: listText } = await call("job", { action: "list" });
         const jobMatch = listText.match(/• (\S+)/);
         if (!jobMatch) return;

         const { isError } = await call("job", {
            action: "get",
            jobId: jobMatch[1],
         });
         expect(isError).toBeFalsy();
      });

      it("log — gets job log", async () => {
         const { text: datasetListText } = await call("dataset", { action: "list" });
         const datasetMatch = datasetListText.match(/• (\S+)/);
         if (!datasetMatch) return;

         const { text: buildText, isError: buildError } = await call("job", {
            action: "build",
            datasetName: datasetMatch[1],
            buildMode: "RECURSIVE_MISSING_ONLY_BUILD",
         });
         expect(buildError).toBeFalsy();

         const jobId = buildText.match(/Job started:\s*(\S+)/)?.[1];
         if (!jobId) return;

         await new Promise((resolve) => setTimeout(resolve, 3000));
         const { text, isError } = await call("job", {
            action: "log",
            jobId,
         });
         expect(isError).toBeFalsy();
         expect(text).toBeDefined();
      });
   });

   // ─── Scenario CRUD ──────────────────────────────────────
   describe("scenario", () => {
      const SCENARIO_ID = `_test_sc_${Date.now()}`;

      it("list — lists scenarios", async () => {
         const { isError } = await call("scenario", { action: "list" });
         expect(isError).toBeFalsy();
      });

      it("create — creates a scenario", async () => {
         const { text, isError } = await call("scenario", {
            action: "create",
            scenarioId: SCENARIO_ID,
            name: "Integration Test Scenario",
         });
         expect(isError).toBeFalsy();
         expect(text).toContain("created");
      });

      it("get — gets scenario definition", async () => {
         const { text, isError } = await call("scenario", {
            action: "get",
            scenarioId: SCENARIO_ID,
         });
         expect(isError).toBeFalsy();
         expect(text).toContain(`Scenario: ${SCENARIO_ID}`);
      });

      it("status — gets scenario light status", async () => {
         const { isError } = await call("scenario", {
            action: "status",
            scenarioId: SCENARIO_ID,
         });
         expect(isError).toBeFalsy();
      });

      it("update — updates scenario definition", async () => {
         const { text, isError } = await call("scenario", {
            action: "update",
            scenarioId: SCENARIO_ID,
            data: { name: "Updated Integration Test" },
         });
         expect(isError).toBeFalsy();
         expect(text).toContain("updated");
      });

      it("delete — deletes scenario", async () => {
         const { text, isError } = await call("scenario", {
            action: "delete",
            scenarioId: SCENARIO_ID,
         });
         expect(isError).toBeFalsy();
         expect(text).toContain("deleted");
      });
   });

   // ─── Managed Folder ─────────────────────────────────────
   describe("managed_folder", () => {
      it("list — lists managed folders", async () => {
         const { isError } = await call("managed_folder", { action: "list" });
         expect(isError).toBeFalsy();
      });

      it("get — gets folder definition (if any exist)", async () => {
         const { text: listText } = await call("managed_folder", {
            action: "list",
         });
         const folderMatch = listText.match(/• (\S+)/);
         if (!folderMatch) return;

         const folderId = folderMatch[1].replace(/:$/, "");
         const { isError } = await call("managed_folder", {
            action: "get",
            folderId,
         });
         expect(isError).toBeFalsy();
      });

      it("contents — lists folder contents", async () => {
         const { text: listText } = await call("managed_folder", {
            action: "list",
         });
         const folderMatch = listText.match(/• (\S+)/);
         if (!folderMatch) return;

         const folderId = folderMatch[1].replace(/:$/, "");
         const { isError } = await call("managed_folder", {
            action: "contents",
            folderId,
         });
         expect(isError).toBeFalsy();
      });
   });

   // ─── Code Environment ───────────────────────────────────
   describe("code_env", () => {
      it("list — lists code environments", async () => {
         const { text, isError } = await call("code_env", { action: "list" });
         expect(isError).toBeFalsy();
         expect(text).toContain("•");
      });

      it("get — gets installed packages for a code env", async () => {
         // First list to find a real env name
         const { text: listText } = await call("code_env", { action: "list" });
         const envMatch = listText.match(/• (\S+) \((\w+)/);
         if (!envMatch) return;

         const envName = envMatch[1];
         const envLang = envMatch[2] as "PYTHON" | "R";

         const { text, isError } = await call("code_env", {
            action: "get",
            envLang,
            envName,
         });
         expect(isError).toBeFalsy();
         expect(text).toContain(envName);
      });

      it("get — supports full package output", async () => {
         const { text: listText } = await call("code_env", { action: "list" });
         const envMatch = listText.match(/• (\S+) \((\w+)/);
         if (!envMatch) return;

         const envName = envMatch[1];
         const envLang = envMatch[2] as "PYTHON" | "R";

         const { text, isError } = await call("code_env", {
            action: "get",
            envLang,
            envName,
            full: true,
         });
         expect(isError).toBeFalsy();
         expect(text).toContain(envName);
         expect(text).toMatch(/Installed packages:|Requested packages:/);
      });

      it("get — returns error when envLang/envName missing", async () => {
         const { text, isError } = await call("code_env", { action: "get" });
         expect(isError).toBeTruthy();
         expect(text).toContain("envLang");
      });
   });

   // ─── Error handling ─────────────────────────────────────
   describe("error handling", () => {
      it("returns clean error for non-existent dataset", async () => {
         const { text, isError } = await call("dataset", {
            action: "get",
            datasetName: "DOES_NOT_EXIST_12345",
         });
         expect(isError).toBeTruthy();
         expect(text).not.toContain("at java.lang");
         expect(text).not.toContain("detailedMessage");
      });

      it("returns clean error for non-existent scenario", async () => {
         const { text, isError } = await call("scenario", {
            action: "get",
            scenarioId: "DOES_NOT_EXIST_12345",
         });
         expect(isError).toBeTruthy();
         expect(text).not.toContain("at java.lang");
      });

      it("returns validation error for missing required params", async () => {
         const { text, isError } = await call("dataset", {
            action: "get",
            // datasetName is missing
         });
         expect(isError).toBeTruthy();
         expect(text).toContain("datasetName");
      });

      it("returns validation error for wrong payloadMaxLines type", async () => {
         const { text, isError } = await call("recipe", {
            action: "get",
            recipeName: "any",
            payloadMaxLines: "5",
         });
         expect(isError).toBeTruthy();
         expect(text).toMatch(/payloadMaxLines|number/i);
      });
   });
});
