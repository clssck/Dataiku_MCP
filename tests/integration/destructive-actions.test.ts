import { randomUUID } from "node:crypto";
import { access, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { register as registerCodeEnvs } from "../../src/tools/code-envs.js";
import { register as registerConnections } from "../../src/tools/connections.js";
import { register as registerDatasets } from "../../src/tools/datasets.js";
import { register as registerFolders } from "../../src/tools/folders.js";
import { register as registerJobs } from "../../src/tools/jobs.js";
import { register as registerProjects } from "../../src/tools/projects.js";
import { register as registerRecipes } from "../../src/tools/recipes.js";
import { register as registerScenarios } from "../../src/tools/scenarios.js";
import { register as registerVariables } from "../../src/tools/variables.js";

const HAS_DSS =
  !!process.env.DATAIKU_URL && !!process.env.DATAIKU_API_KEY && !!process.env.DATAIKU_PROJECT_KEY;
const RUN_DESTRUCTIVE = process.env.DATAIKU_MCP_DESTRUCTIVE_TESTS === "1";

describe.skipIf(!(HAS_DSS && RUN_DESTRUCTIVE))(
  "Integration: destructive actions against live DSS",
  () => {
    let client: Client;
    let server: McpServer;
    let workDir = "";
    let connection = "filesystem_managed";
    let folderId: string | undefined;

    const createdDatasets = new Set<string>();
    const createdRecipes = new Set<string>();
    const createdScenarios = new Set<string>();

    const runId = `${Date.now()}_${randomUUID().slice(0, 8)}`;
    const INPUT_DS = `_test_in_${runId}`;
    const OUTPUT_DS = `_test_out_${runId}`;
    const RECIPE_NAME = `python_${OUTPUT_DS}`;
    const SCENARIO_ID = `_test_run_${runId}`;
    const FOLDER_REMOTE_PATH = `_mcp_test/${runId}/smoke.txt`;
    let folderFileUploaded = false;

    async function call(tool: string, args: Record<string, unknown>) {
      const result = await client.callTool({ name: tool, arguments: args });
      const text = (result.content as Array<{ type: string; text: string }>)[0]?.text;
      return { text, isError: typeof result.isError === "boolean" ? result.isError : undefined };
    }

    function sleep(ms: number): Promise<void> {
      return new Promise((resolve) => setTimeout(resolve, ms));
    }

    function isMissingResourceMessage(text: string | undefined): boolean {
      const lower = (text ?? "").toLowerCase();
      return (
        lower.includes("not found") ||
        lower.includes("does not exist") ||
        lower.includes("unknown") ||
        lower.includes("no such") ||
        lower.includes("already")
      );
    }

    async function deleteWithRetry(
      tool: string,
      args: Record<string, unknown>,
      attempts = 3,
    ): Promise<boolean> {
      for (let attempt = 1; attempt <= attempts; attempt += 1) {
        const result = await call(tool, args).catch((error) => ({
          isError: true,
          text: error instanceof Error ? error.message : String(error),
        }));
        if (!result.isError || isMissingResourceMessage(result.text)) {
          return true;
        }
        if (attempt < attempts) {
          await sleep(200 * attempt);
        }
      }
      return false;
    }

    async function cleanupBestEffort(): Promise<string[]> {
      const failures: string[] = [];

      for (const recipeName of [...createdRecipes]) {
        const ok = await deleteWithRetry("recipe", { action: "delete", recipeName });
        if (ok) {
          createdRecipes.delete(recipeName);
        } else {
          failures.push(`recipe:${recipeName}`);
        }
      }

      for (const datasetName of [...createdDatasets]) {
        const ok = await deleteWithRetry("dataset", { action: "delete", datasetName });
        if (ok) {
          createdDatasets.delete(datasetName);
        } else {
          failures.push(`dataset:${datasetName}`);
        }
      }

      for (const scenarioId of [...createdScenarios]) {
        const ok = await deleteWithRetry("scenario", { action: "delete", scenarioId });
        if (ok) {
          createdScenarios.delete(scenarioId);
        } else {
          failures.push(`scenario:${scenarioId}`);
        }
      }

      if (folderId && folderFileUploaded) {
        const ok = await deleteWithRetry("managed_folder", {
          action: "delete_file",
          folderId,
          path: FOLDER_REMOTE_PATH,
        });
        if (ok) {
          folderFileUploaded = false;
        } else {
          failures.push(`managed_folder:${folderId}:${FOLDER_REMOTE_PATH}`);
        }
      }

      return failures;
    }

    beforeAll(async () => {
      server = new McpServer({ name: "test-destructive", version: "0.0.1" });
      registerProjects(server);
      registerDatasets(server);
      registerRecipes(server);
      registerJobs(server);
      registerScenarios(server);
      registerVariables(server);
      registerFolders(server);
      registerConnections(server);
      registerCodeEnvs(server);

      const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
      await server.connect(serverTransport);

      client = new Client({ name: "test-destructive-client", version: "0.0.1" });
      await client.connect(clientTransport);

      workDir = await mkdtemp(join(tmpdir(), "dataiku-mcp-destructive-"));

      const { text: connText } = await call("connection", { action: "infer" });
      const connMatch = connText?.match(/• (\S+)/);
      if (connMatch?.[1]) connection = connMatch[1];

      const { text: folderText } = await call("managed_folder", { action: "list" });
      const folderMatch = folderText?.match(/• (\S+)/);
      if (folderMatch?.[1]) folderId = folderMatch[1].replace(/:$/, "");
    });

    afterAll(async () => {
      await cleanupBestEffort();
      if (workDir) {
        await rm(workDir, { recursive: true, force: true });
      }
      await client?.close();
      await server?.close();
    });

    it("dataset create/update/download paths", async () => {
      const createRes = await call("dataset", {
        action: "create",
        datasetName: INPUT_DS,
        connection,
      });
      expect(createRes.isError).toBeFalsy();
      expect(createRes.text).toContain("created");
      createdDatasets.add(INPUT_DS);

      const updateRes = await call("dataset", {
        action: "update",
        datasetName: INPUT_DS,
        data: { shortDesc: "updated by destructive integration test" },
      });
      expect(updateRes.isError).toBeFalsy();
      expect(updateRes.text).toContain("updated");

      const listRes = await call("dataset", { action: "list" });
      expect(listRes.isError).toBeFalsy();
      const candidateNames = (listRes.text ?? "")
        .split("\n")
        .map((line) => line.match(/^•\s+([^\s(]+)/)?.[1])
        .filter((v): v is string => Boolean(v))
        .slice(0, 10);

      let downloadTarget = INPUT_DS;
      for (const candidate of candidateNames) {
        const previewProbe = await call("dataset", {
          action: "preview",
          datasetName: candidate,
          limit: 1,
        });
        if (!previewProbe.isError) {
          downloadTarget = candidate;
          break;
        }
      }

      const downloadRes = await call("dataset", {
        action: "download",
        datasetName: downloadTarget,
        limit: 100,
        outputDir: workDir,
      });
      expect(downloadRes.isError).toBeFalsy();
      expect(downloadRes.text).toContain("exported");

      const gzPath = join(workDir, `${downloadTarget}.csv.gz`);
      await access(gzPath);
      const st = await stat(gzPath);
      expect(st.size).toBeGreaterThan(0);
    });

    it("recipe create/update/download paths", async () => {
      const createOut = await call("recipe", {
        action: "create",
        type: "python",
        inputDatasets: [INPUT_DS],
        outputDataset: OUTPUT_DS,
        outputConnection: connection,
        payload: "import time\ntime.sleep(20)\n",
      });
      expect(createOut.isError).toBeFalsy();
      expect(createOut.text).toContain("created");
      createdRecipes.add(RECIPE_NAME);
      createdDatasets.add(OUTPUT_DS);

      const updateOut = await call("recipe", {
        action: "update",
        recipeName: RECIPE_NAME,
        data: { payload: "print('updated payload')" },
      });
      expect(updateOut.isError).toBeFalsy();
      expect(updateOut.text).toContain("updated");

      const getOut = await call("recipe", {
        action: "get",
        recipeName: RECIPE_NAME,
        includePayload: true,
        payloadMaxLines: 20,
      });
      expect(getOut.isError).toBeFalsy();
      expect(getOut.text).toContain("Payload Body:");
      expect(getOut.text).toContain("updated payload");

      const recipeFile = join(workDir, `${RECIPE_NAME}.json`);
      const downloadOut = await call("recipe", {
        action: "download",
        recipeName: RECIPE_NAME,
        outputPath: recipeFile,
      });
      expect(downloadOut.isError).toBeFalsy();
      expect(downloadOut.text).toContain("saved");
      const raw = await readFile(recipeFile, "utf-8");
      expect(raw.length).toBeGreaterThan(10);
      expect(raw).toContain("recipe");
    });

    it("job build + abort paths", async () => {
      const build = await call("job", {
        action: "build",
        datasetName: OUTPUT_DS,
        buildMode: "RECURSIVE_FORCED_BUILD",
      });
      expect(build.isError).toBeFalsy();
      expect(build.text).toContain("Job started:");

      const jobId = build.text.match(/Job started:\s*(\S+)/)?.[1];
      expect(jobId).toBeTruthy();
      if (!jobId) return;

      const getRes = await call("job", { action: "get", jobId });
      expect(getRes.isError).toBeFalsy();
      expect(getRes.text).toContain("Job:");

      const abortRes = await call("job", { action: "abort", jobId });
      if (abortRes.isError) {
        expect(abortRes.text).toMatch(/abort|already|cannot|done|finish/i);
      } else {
        expect(abortRes.text).toContain("abort requested");
      }
    });

    it("scenario run path", async () => {
      const create = await call("scenario", {
        action: "create",
        scenarioId: SCENARIO_ID,
        name: "Destructive Scenario Run Test",
      });
      expect(create.isError).toBeFalsy();
      expect(create.text).toContain("created");
      createdScenarios.add(SCENARIO_ID);

      const run = await call("scenario", {
        action: "run",
        scenarioId: SCENARIO_ID,
      });
      expect(run.isError).toBeFalsy();
      expect(run.text).toContain("triggered");
    });

    it("managed folder upload/download/delete_file paths", async () => {
      if (!folderId) return;

      const uploadSrc = join(workDir, "upload-source.txt");
      const downloadDst = join(workDir, "downloaded.txt");
      const content = `destructive-${runId}`;
      await writeFile(uploadSrc, content, "utf-8");

      const uploadRes = await call("managed_folder", {
        action: "upload",
        folderId,
        path: FOLDER_REMOTE_PATH,
        localPath: uploadSrc,
      });
      expect(uploadRes.isError).toBeFalsy();
      expect(uploadRes.text).toContain("Uploaded");

      folderFileUploaded = true;
      const downloadRes = await call("managed_folder", {
        action: "download",
        folderId,
        path: FOLDER_REMOTE_PATH,
        localPath: downloadDst,
      });
      expect(downloadRes.isError).toBeFalsy();
      expect(downloadRes.text).toContain("Downloaded");
      const downloaded = await readFile(downloadDst, "utf-8");
      expect(downloaded).toBe(content);

      const deleteRes = await call("managed_folder", {
        action: "delete_file",
        folderId,
        path: FOLDER_REMOTE_PATH,
      });
      expect(deleteRes.isError).toBeFalsy();
      expect(deleteRes.text).toContain("Deleted");
    });

    it("cleanup is idempotent and leaves no leaked resources", async () => {
      const failures = await cleanupBestEffort();
      expect(failures).toEqual([]);
      expect(createdDatasets.size).toBe(0);
      expect(createdRecipes.size).toBe(0);
      expect(createdScenarios.size).toBe(0);

      const datasetsAfter = await call("dataset", { action: "list", query: runId });
      expect(datasetsAfter.isError).toBeFalsy();
      expect(datasetsAfter.text ?? "").not.toContain(INPUT_DS);
      expect(datasetsAfter.text ?? "").not.toContain(OUTPUT_DS);

      const recipesAfter = await call("recipe", { action: "list", query: runId });
      expect(recipesAfter.isError).toBeFalsy();
      expect(recipesAfter.text ?? "").not.toContain(RECIPE_NAME);

      const scenariosAfter = await call("scenario", { action: "list", query: runId });
      expect(scenariosAfter.isError).toBeFalsy();
      expect(scenariosAfter.text ?? "").not.toContain(SCENARIO_ID);

      if (folderId) {
        const folderContents = await call("managed_folder", {
          action: "contents",
          folderId,
        });
        if (!folderContents.isError) {
          expect(folderContents.text ?? "").not.toContain(FOLDER_REMOTE_PATH);
        }
      }
    });
  },
);
