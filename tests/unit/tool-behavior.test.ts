import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { access, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const clientMocks = vi.hoisted(() => ({
  del: vi.fn(),
  get: vi.fn(),
  getProjectKey: vi.fn((projectKey?: string) => projectKey ?? "TEST_PROJECT"),
  post: vi.fn(),
  put: vi.fn(),
  putVoid: vi.fn(),
  stream: vi.fn(),
  getText: vi.fn(),
  upload: vi.fn(),
}));

vi.mock("../../src/client.js", () => clientMocks);

import { register as registerDatasets } from "../../src/tools/datasets.js";
import { register as registerCodeEnvs } from "../../src/tools/code-envs.js";
import { register as registerConnections } from "../../src/tools/connections.js";
import { register as registerFolders } from "../../src/tools/folders.js";
import { register as registerJobs } from "../../src/tools/jobs.js";
import { register as registerProjects } from "../../src/tools/projects.js";
import { register as registerRecipes } from "../../src/tools/recipes.js";
import { register as registerScenarios } from "../../src/tools/scenarios.js";
import { register as registerVariables } from "../../src/tools/variables.js";

async function callTool(
  registerTool: (server: McpServer) => void,
  name: string,
  args: Record<string, unknown>,
): Promise<{
  text: string;
  isError: boolean | undefined;
  structured: Record<string, unknown> | undefined;
}> {
  const server = new McpServer({ name: "test", version: "0.0.1" });
  registerTool(server);
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  const client = new Client({ name: "test-client", version: "0.0.1" });
  await client.connect(clientTransport);
  try {
    const result = await client.callTool({ name, arguments: args });
    const content = result.content as Array<{ text?: string }> | undefined;
    const structured =
      result.structuredContent && typeof result.structuredContent === "object"
        ? (result.structuredContent as Record<string, unknown>)
        : undefined;
    return {
      text: content?.[0]?.text ?? "",
      isError: typeof result.isError === "boolean" ? result.isError : undefined,
      structured,
    };
  } finally {
    await client.close();
    await server.close();
  }
}

describe("Tool Behavior Coverage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    clientMocks.getProjectKey.mockImplementation(
      (projectKey?: string) => projectKey ?? "TEST_PROJECT",
    );
  });

  it("dataset create infers type from existing datasets on same connection", async () => {
    clientMocks.get.mockResolvedValue([
      { type: "Filesystem", params: { connection: "managed_conn" } },
    ]);
    clientMocks.post.mockResolvedValue({});

    const { text, isError } = await callTool(registerDatasets, "dataset", {
      action: "create",
      projectKey: "PROJ",
      datasetName: "new_ds",
      connection: "managed_conn",
    });

    expect(isError).toBeFalsy();
    expect(text).toContain('Dataset "new_ds" created');
    expect(clientMocks.post).toHaveBeenCalledWith(
      "/public/api/projects/PROJ/datasets/",
      expect.objectContaining({
        name: "new_ds",
        type: "Filesystem",
        params: expect.objectContaining({ connection: "managed_conn" }),
      }),
    );
  });

  it("dataset preview converts TSV stream to CSV and respects row limit", async () => {
    const body = new Response("a\tb\n1\t2\n3\t4\n").body;
    clientMocks.stream.mockResolvedValue({ body });

    const { text, isError } = await callTool(registerDatasets, "dataset", {
      action: "preview",
      projectKey: "PROJ",
      datasetName: "sample_ds",
      limit: 1,
    });

    expect(isError).toBeFalsy();
    expect(text).toBe("a,b\n1,2");
    expect(clientMocks.stream).toHaveBeenCalledWith(
      "/public/api/projects/PROJ/datasets/sample_ds/data/?format=tsv-excel-header&limit=1",
    );
  });

  it("dataset download uses a Windows-safe default filename", async () => {
    const workDir = await mkdtemp(join(tmpdir(), "dataiku-mcp-unit-"));
    const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(workDir);

    try {
      const body = new Response("a\tb\n1\t2\n").body;
      clientMocks.stream.mockResolvedValue({ body });

      const { text, isError } = await callTool(registerDatasets, "dataset", {
        action: "download",
        projectKey: "PROJ",
        datasetName: "sales:2026*Q1?",
        limit: 10,
      });

      expect(isError).toBeFalsy();
      expect(text).toContain("sales_2026_Q1_.csv.gz");

      const exported = join(workDir, "sales_2026_Q1_.csv.gz");
      await access(exported);
    } finally {
      cwdSpy.mockRestore();
      await rm(workDir, { recursive: true, force: true });
    }
  });

  it("dataset download rewrites Windows reserved names before extension", async () => {
    const workDir = await mkdtemp(join(tmpdir(), "dataiku-mcp-unit-"));
    const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(workDir);

    try {
      const body = new Response("a\tb\n1\t2\n").body;
      clientMocks.stream.mockResolvedValue({ body });

      const { text, isError } = await callTool(registerDatasets, "dataset", {
        action: "download",
        projectKey: "PROJ",
        datasetName: "NUL.txt",
        limit: 10,
      });

      expect(isError).toBeFalsy();
      expect(text).toContain("NUL_.txt.csv.gz");

      const exported = join(workDir, "NUL_.txt.csv.gz");
      await access(exported);
    } finally {
      cwdSpy.mockRestore();
      await rm(workDir, { recursive: true, force: true });
    }
  });

  it("dataset update deep-merges nested params without dropping existing keys", async () => {
    clientMocks.get.mockResolvedValue({
      name: "orders_ds",
      params: {
        connection: "snowflake_main",
        table: "ORDERS",
        schema: "PUBLIC",
      },
      formatParams: {
        separator: ",",
        charset: "utf8",
      },
    });
    clientMocks.put.mockResolvedValue({});

    const { text, isError } = await callTool(registerDatasets, "dataset", {
      action: "update",
      projectKey: "PROJ",
      datasetName: "orders_ds",
      data: {
        params: {
          schema: "ANALYTICS",
        },
      },
    });

    expect(isError).toBeFalsy();
    expect(text).toContain('Dataset "orders_ds" updated.');
    expect(clientMocks.put).toHaveBeenCalledWith(
      "/public/api/projects/PROJ/datasets/orders_ds",
      expect.objectContaining({
        params: {
          connection: "snowflake_main",
          table: "ORDERS",
          schema: "ANALYTICS",
        },
        formatParams: {
          separator: ",",
          charset: "utf8",
        },
      }),
    );
  });

  it("recipe get includes payload body and truncation when requested", async () => {
    clientMocks.get.mockResolvedValue({
      recipe: {
        name: "compute_ass",
        type: "python",
        inputs: { main: { items: [{ ref: "in_ds" }] } },
        outputs: { main: { items: [{ ref: "out_ds" }] } },
      },
      payload: "line1\nline2\nline3\nline4",
    });

    const { text, isError } = await callTool(registerRecipes, "recipe", {
      action: "get",
      projectKey: "PROJ",
      recipeName: "compute_ass",
      includePayload: true,
      payloadMaxLines: 2,
    });

    expect(isError).toBeFalsy();
    expect(text).toContain("Recipe: compute_ass (python)");
    expect(text).toContain("Payload Body:");
    expect(text).toContain("line1\nline2");
    expect(text).toContain("... (2 more lines not shown)");
  });

  it("recipe download uses a Windows-safe default filename", async () => {
    const workDir = await mkdtemp(join(tmpdir(), "dataiku-mcp-unit-"));
    const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(workDir);

    try {
      clientMocks.get.mockResolvedValue({ recipe: { name: "r1" } });

      const { text, isError } = await callTool(registerRecipes, "recipe", {
        action: "download",
        projectKey: "PROJ",
        recipeName: "join<orders>|v2",
      });

      expect(isError).toBeFalsy();
      expect(text).toContain("join_orders__v2.json");

      const savedPath = join(workDir, "join_orders__v2.json");
      await access(savedPath);

      const raw = await readFile(savedPath, "utf-8");
      expect(raw).toContain('"recipe"');
    } finally {
      cwdSpy.mockRestore();
      await rm(workDir, { recursive: true, force: true });
    }
  });

  it("recipe download rewrites Windows reserved names before extension", async () => {
    const workDir = await mkdtemp(join(tmpdir(), "dataiku-mcp-unit-"));
    const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(workDir);

    try {
      clientMocks.get.mockResolvedValue({ recipe: { name: "r1" } });

      const { text, isError } = await callTool(registerRecipes, "recipe", {
        action: "download",
        projectKey: "PROJ",
        recipeName: "CON.py",
      });

      expect(isError).toBeFalsy();
      expect(text).toContain("CON_.py.json");

      const savedPath = join(workDir, "CON_.py.json");
      await access(savedPath);

      const raw = await readFile(savedPath, "utf-8");
      expect(raw).toContain('"recipe"');
    } finally {
      cwdSpy.mockRestore();
      await rm(workDir, { recursive: true, force: true });
    }
  });

  it("recipe update deep-merges nested recipe fields", async () => {
    clientMocks.get.mockResolvedValue({
      recipe: {
        name: "r1",
        type: "python",
        inputs: { main: { items: [{ ref: "in_ds" }] } },
      },
      payload: "old payload",
      meta: { owner: "alice" },
    });
    clientMocks.put.mockResolvedValue({});

    const { text, isError } = await callTool(registerRecipes, "recipe", {
      action: "update",
      projectKey: "PROJ",
      recipeName: "r1",
      data: {
        recipe: { outputs: { main: { items: [{ ref: "out_ds" }] } } },
        payload: "new payload",
      },
    });

    expect(isError).toBeFalsy();
    expect(text).toContain('Recipe "r1" updated.');
    expect(clientMocks.put).toHaveBeenCalledWith(
      "/public/api/projects/PROJ/recipes/r1",
      expect.objectContaining({
        payload: "new payload",
        recipe: expect.objectContaining({
          name: "r1",
          type: "python",
          inputs: { main: { items: [{ ref: "in_ds" }] } },
          outputs: { main: { items: [{ ref: "out_ds" }] } },
        }),
      }),
    );
  });

  it("recipe create auto-creates outputs using inferred connection dataset type", async () => {
    clientMocks.get.mockResolvedValue([
      {
        name: "orders_input",
        type: "Snowflake",
        managed: false,
        params: { connection: "snowflake_main", schema: "PUBLIC" },
      },
    ]);
    clientMocks.post.mockResolvedValue({});

    const { text, isError } = await callTool(registerRecipes, "recipe", {
      action: "create",
      projectKey: "PROJ",
      type: "sql_query",
      inputDatasets: ["orders_input"],
      outputDataset: "orders_enriched",
      outputConnection: "snowflake_main",
    });

    expect(isError).toBeFalsy();
    expect(text).toContain('Recipe "sql_query_orders_enriched" created.');
    expect(clientMocks.post).toHaveBeenCalledWith(
      "/public/api/projects/PROJ/datasets/",
      expect.objectContaining({
        name: "orders_enriched",
        type: "Snowflake",
        params: expect.objectContaining({
          connection: "snowflake_main",
          mode: "table",
          table: "orders_enriched",
          schema: "PUBLIC",
        }),
      }),
    );
  });

  it("project map returns raw graph when includeRaw is true", async () => {
    clientMocks.get.mockImplementation(async (path: string) => {
      if (path.endsWith("/flow/graph/")) {
        return {
          nodes: {
            ds_a: {
              type: "DATASET",
              ref: "ds_a",
              predecessors: [],
              successors: ["prep_a"],
            },
            prep_a: {
              type: "RECIPE",
              subType: "python",
              ref: "prep_a",
              predecessors: ["ds_a"],
              successors: ["ds_b"],
            },
            ds_b: {
              type: "DATASET",
              ref: "ds_b",
              predecessors: ["prep_a"],
              successors: [],
            },
          },
          datasets: ["ds_a", "ds_b"],
          recipes: ["prep_a"],
          folders: [],
        };
      }
      if (path.endsWith("/managedfolders/")) return [];
      if (path.endsWith("/datasets/")) return [{ name: "ds_a" }, { name: "ds_b" }];
      if (path.endsWith("/recipes/")) return [{ name: "prep_a" }];
      throw new Error(`Unexpected get path: ${path}`);
    });

    const { text, isError, structured } = await callTool(registerProjects, "project", {
      action: "map",
      projectKey: "PROJ",
      includeRaw: true,
    });

    expect(isError).toBeFalsy();
    expect(text).toContain("Flow map for PROJ");
    const parsed = structured?.map as {
      raw?: unknown;
      nodes?: unknown[];
      edges?: unknown[];
    };
    expect(parsed?.raw).toBeDefined();
    expect(Array.isArray(parsed?.nodes)).toBe(true);
    expect(Array.isArray(parsed?.edges)).toBe(true);
  });

  it("project map omits raw graph by default", async () => {
    clientMocks.get.mockImplementation(async (path: string) => {
      if (path.endsWith("/flow/graph/")) {
        return {
          nodes: {},
          datasets: [],
          recipes: [],
          folders: [],
        };
      }
      if (path.endsWith("/managedfolders/")) return [];
      if (path.endsWith("/datasets/")) return [];
      if (path.endsWith("/recipes/")) return [];
      throw new Error(`Unexpected get path: ${path}`);
    });

    const { text, isError, structured } = await callTool(registerProjects, "project", {
      action: "map",
      projectKey: "PROJ",
    });

    expect(isError).toBeFalsy();
    expect(text).toContain("Flow map for PROJ");
    const parsed = structured as {
      map?: { raw?: unknown };
      truncation?: { maxNodes?: number | null; maxEdges?: number | null; truncated?: boolean };
    };
    expect(parsed?.map?.raw).toBeUndefined();
    expect(parsed?.truncation).toMatchObject({
      maxNodes: 300,
      maxEdges: 600,
      truncated: false,
    });
  });

  it("project map applies maxNodes/maxEdges truncation with metadata", async () => {
    const largeNodeCount = 120;
    const nodes: Record<
      string,
      {
        type: string;
        ref: string;
        predecessors: string[];
        successors: string[];
      }
    > = {};
    const datasets: string[] = [];
    const recipes: string[] = [];

    for (let index = 0; index < largeNodeCount; index += 1) {
      const isDataset = index % 2 === 0;
      const id = `${isDataset ? "ds" : "rc"}_${index.toString().padStart(3, "0")}`;
      const predecessor =
        index > 0
          ? `${(index - 1) % 2 === 0 ? "ds" : "rc"}_${(index - 1).toString().padStart(3, "0")}`
          : undefined;
      const successor =
        index < largeNodeCount - 1
          ? `${(index + 1) % 2 === 0 ? "ds" : "rc"}_${(index + 1).toString().padStart(3, "0")}`
          : undefined;

      nodes[id] = {
        type: isDataset ? "DATASET" : "RECIPE",
        ref: id,
        predecessors: predecessor ? [predecessor] : [],
        successors: successor ? [successor] : [],
      };

      if (isDataset) {
        datasets.push(id);
      } else {
        recipes.push(id);
      }
    }

    clientMocks.get.mockImplementation(async (path: string) => {
      if (path.endsWith("/flow/graph/")) {
        return { nodes, datasets, recipes, folders: [] };
      }
      if (path.endsWith("/managedfolders/")) return [];
      if (path.endsWith("/datasets/")) return datasets.map((name) => ({ name }));
      if (path.endsWith("/recipes/")) return recipes.map((name) => ({ name }));
      throw new Error(`Unexpected get path: ${path}`);
    });

    const { text, isError, structured } = await callTool(registerProjects, "project", {
      action: "map",
      projectKey: "PROJ",
      maxNodes: 30,
      maxEdges: 20,
    });

    expect(isError).toBeFalsy();
    expect(text).toContain("Truncated: yes");

    const parsed = structured as {
      map?: { nodes?: Array<{ id: string }>; edges?: Array<{ from: string; to: string }> };
      truncation?: {
        truncated?: boolean;
        maxNodes?: number | null;
        maxEdges?: number | null;
        nodeCountBefore?: number;
        nodeCountAfter?: number;
        edgeCountBefore?: number;
        edgeCountAfter?: number;
      };
    };

    expect(parsed?.truncation).toMatchObject({
      truncated: true,
      maxNodes: 30,
      maxEdges: 20,
      nodeCountBefore: 120,
      nodeCountAfter: 30,
      edgeCountBefore: 119,
      edgeCountAfter: 0,
    });

    expect(parsed?.map?.nodes).toHaveLength(30);
    expect(parsed?.map?.edges).toHaveLength(0);

    const nodeIds = new Set((parsed?.map?.nodes ?? []).map((node) => node.id));
    expect(
      (parsed?.map?.edges ?? []).every((edge) => nodeIds.has(edge.from) && nodeIds.has(edge.to)),
    ).toBe(true);
  });

  it("connection infer marks connection as managed if any dataset is managed", async () => {
    clientMocks.get.mockResolvedValue([
      {
        name: "inbound_a",
        type: "Snowflake",
        managed: false,
        params: { connection: "warehouse", schema: "RAW" },
      },
      {
        name: "inbound_b",
        type: "Snowflake",
        managed: true,
        params: { connection: "warehouse", schema: "CURATED" },
      },
    ]);

    const { text, isError } = await callTool(registerConnections, "connection", {
      action: "infer",
      projectKey: "PROJ",
    });

    expect(isError).toBeFalsy();
    expect(text).toContain("warehouse");
    expect(text).toContain("managed");
    expect(text).toContain("RAW");
    expect(text).toContain("CURATED");
  });

  it("variable set read-merges standard/local keys before writing", async () => {
    clientMocks.get.mockResolvedValue({
      standard: { keep: 1, replace: "old" },
      local: { localKeep: "x" },
    });
    clientMocks.putVoid.mockResolvedValue({});

    const { isError } = await callTool(registerVariables, "variable", {
      action: "set",
      projectKey: "PROJ",
      standard: { replace: "new", added: true },
      local: { localAdded: 42 },
    });

    expect(isError).toBeFalsy();
    expect(clientMocks.putVoid).toHaveBeenCalledWith("/public/api/projects/PROJ/variables/", {
      standard: { keep: 1, replace: "new", added: true },
      local: { localKeep: "x", localAdded: 42 },
    });
  });

  it("scenario update deep-merges nested params", async () => {
    clientMocks.get.mockResolvedValue({
      id: "nightly",
      active: true,
      params: {
        steps: [{ type: "build_flowitem" }],
        triggers: [{ type: "time" }],
        reporters: [{ type: "mail-scenario" }],
      },
    });
    clientMocks.put.mockResolvedValue({});

    const { text, isError } = await callTool(registerScenarios, "scenario", {
      action: "update",
      projectKey: "PROJ",
      scenarioId: "nightly",
      data: {
        params: {
          triggers: [{ type: "dataset-change" }],
        },
      },
    });

    expect(isError).toBeFalsy();
    expect(text).toContain('Scenario "nightly" updated.');
    expect(clientMocks.put).toHaveBeenCalledWith(
      "/public/api/projects/PROJ/scenarios/nightly/",
      expect.objectContaining({
        params: {
          steps: [{ type: "build_flowitem" }],
          triggers: [{ type: "dataset-change" }],
          reporters: [{ type: "mail-scenario" }],
        },
      }),
    );
  });

  it("job log returns only tail when maxLogLines is set", async () => {
    clientMocks.getText.mockResolvedValue("l1\nl2\nl3\nl4\nl5");

    const { text, isError } = await callTool(registerJobs, "job", {
      action: "log",
      projectKey: "PROJ",
      jobId: "job_123",
      maxLogLines: 2,
    });

    expect(isError).toBeFalsy();
    expect(text).toContain("showing last 2 of 5 lines");
    expect(text).toContain("l4\nl5");
    expect(text).not.toContain("l1");
  });

  it("job wait polls until terminal state and includes log tail when requested", async () => {
    clientMocks.get
      .mockResolvedValueOnce({
        baseStatus: {
          def: { id: "job_123", type: "RECURSIVE_BUILD" },
          state: "RUNNING",
        },
        globalState: {
          done: 0,
          total: 1,
          failed: 0,
          running: 1,
        },
      })
      .mockResolvedValueOnce({
        baseStatus: {
          def: { id: "job_123", type: "RECURSIVE_BUILD" },
          state: "DONE",
        },
        globalState: {
          done: 1,
          total: 1,
          failed: 0,
          running: 0,
        },
      });
    clientMocks.getText.mockResolvedValue("l1\nl2\nl3");

    const { text, isError } = await callTool(registerJobs, "job", {
      action: "wait",
      projectKey: "PROJ",
      jobId: "job_123",
      pollIntervalMs: 1,
      timeoutMs: 2000,
      includeLogs: true,
      maxLogLines: 2,
    });

    expect(isError).toBeFalsy();
    expect(text).toContain("State: DONE");
    expect(text).toContain("Polls: 2");
    expect(text).toContain("Latest log tail");
    expect(text).toContain("l2\nl3");
    expect(clientMocks.getText).toHaveBeenCalledWith("/public/api/projects/PROJ/jobs/job_123/log/");
  });

  it("managed_folder contents reports file count and metadata", async () => {
    clientMocks.get.mockResolvedValue({
      items: [
        { path: "a.csv", size: 1536, lastModified: Date.UTC(2026, 0, 5) },
        { path: "b.csv", size: 12, lastModified: Date.UTC(2026, 0, 6) },
      ],
    });

    const { text, isError } = await callTool(registerFolders, "managed_folder", {
      action: "contents",
      projectKey: "PROJ",
      folderId: "folder_1",
    });

    expect(isError).toBeFalsy();
    expect(text).toContain("2 files:");
    expect(text).toContain("a.csv");
    expect(text).toContain("b.csv");
  });

  it("managed_folder download normalizes Windows separators and default filename", async () => {
    const workDir = await mkdtemp(join(tmpdir(), "dataiku-mcp-unit-"));
    const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(workDir);

    try {
      const body = new Response("hello-windows").body;
      clientMocks.stream.mockResolvedValue({ body });

      const { text, isError } = await callTool(registerFolders, "managed_folder", {
        action: "download",
        projectKey: "PROJ",
        folderId: "folder_1",
        path: "nested\\reports\\daily:extract?.csv",
      });

      expect(isError).toBeFalsy();
      expect(text).toContain('Downloaded "nested/reports/daily:extract?.csv"');
      expect(clientMocks.stream).toHaveBeenCalledWith(
        "/public/api/projects/PROJ/managedfolders/folder_1/contents/nested%2Freports%2Fdaily%3Aextract%3F.csv",
      );

      const downloadedPath = join(workDir, "daily_extract_.csv");
      await access(downloadedPath);

      const downloaded = await readFile(downloadedPath, "utf-8");
      expect(downloaded).toBe("hello-windows");
    } finally {
      cwdSpy.mockRestore();
      await rm(workDir, { recursive: true, force: true });
    }
  });

  it("managed_folder download rewrites Windows reserved names before extension", async () => {
    const workDir = await mkdtemp(join(tmpdir(), "dataiku-mcp-unit-"));
    const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(workDir);

    try {
      const body = new Response("hello-reserved").body;
      clientMocks.stream.mockResolvedValue({ body });

      const { text, isError } = await callTool(registerFolders, "managed_folder", {
        action: "download",
        projectKey: "PROJ",
        folderId: "folder_1",
        path: "nested\\reports\\PRN.log",
      });

      expect(isError).toBeFalsy();
      expect(text).toContain('Downloaded "nested/reports/PRN.log"');
      expect(clientMocks.stream).toHaveBeenCalledWith(
        "/public/api/projects/PROJ/managedfolders/folder_1/contents/nested%2Freports%2FPRN.log",
      );

      const downloadedPath = join(workDir, "PRN_.log");
      await access(downloadedPath);

      const downloaded = await readFile(downloadedPath, "utf-8");
      expect(downloaded).toBe("hello-reserved");
    } finally {
      cwdSpy.mockRestore();
      await rm(workDir, { recursive: true, force: true });
    }
  });

  it("code_env get summarizes requested and installed packages", async () => {
    clientMocks.get.mockResolvedValue({
      envName: "py39",
      envLang: "PYTHON",
      desc: { pythonInterpreter: "PYTHON39" },
      specPackageList: "pandas\nnumpy",
      actualPackageList: "numpy==2.0.0\npandas==2.2.1\n",
    });

    const { text, isError, structured } = await callTool(registerCodeEnvs, "code_env", {
      action: "get",
      envLang: "PYTHON",
      envName: "py39",
    });
    expect(isError).toBeFalsy();
    expect(text).toContain("py39 (PYTHON, PYTHON39)");
    expect(text).toContain("Requested packages:");
    expect(text).toContain("Installed packages:");
    expect(structured?.env).toMatchObject({
      envName: "py39",
      envLang: "PYTHON",
      requestedPackageCount: 2,
      installedPackageCount: 2,
      requestedPackagesTruncated: false,
      installedPackagesTruncated: false,
      packageLimit: 200,
    });
  });

  it("code_env get defaults to capping package arrays", async () => {
    const names = Array.from(
      { length: 240 },
      (_, index) => `pkg_${index.toString().padStart(3, "0")}`,
    );
    clientMocks.get.mockResolvedValue({
      envName: "py-capped",
      envLang: "PYTHON",
      specPackageList: names.join("\n"),
      actualPackageList: names.map((name, index) => `${name}==${index}.0.0`).join("\n"),
    });

    const { isError, structured } = await callTool(registerCodeEnvs, "code_env", {
      action: "get",
      envLang: "PYTHON",
      envName: "py-capped",
    });

    const env = structured?.env as Record<string, unknown> | undefined;
    expect(isError).toBeFalsy();
    expect(env?.requestedPackageCount).toBe(240);
    expect(env?.installedPackageCount).toBe(240);
    expect((env?.requestedPackages as unknown[] | undefined)?.length).toBe(200);
    expect((env?.installedPackages as unknown[] | undefined)?.length).toBe(200);
    expect(env?.requestedPackagesTruncated).toBe(true);
    expect(env?.installedPackagesTruncated).toBe(true);
    expect(env?.packageLimit).toBe(200);
  });

  it("code_env get honors maxPackages and lets full=true bypass caps", async () => {
    const names = Array.from(
      { length: 25 },
      (_, index) => `pkg_${index.toString().padStart(2, "0")}`,
    );
    clientMocks.get.mockResolvedValue({
      envName: "py-custom",
      envLang: "PYTHON",
      specPackageList: names.join("\n"),
      actualPackageList: names.map((name, index) => `${name}==${index}.0.0`).join("\n"),
    });

    const capped = await callTool(registerCodeEnvs, "code_env", {
      action: "get",
      envLang: "PYTHON",
      envName: "py-custom",
      maxPackages: 10,
    });
    const cappedEnv = capped.structured?.env as Record<string, unknown> | undefined;
    expect(capped.isError).toBeFalsy();
    expect((cappedEnv?.requestedPackages as unknown[] | undefined)?.length).toBe(10);
    expect((cappedEnv?.installedPackages as unknown[] | undefined)?.length).toBe(10);
    expect(cappedEnv?.packageLimit).toBe(10);
    expect(cappedEnv?.requestedPackagesTruncated).toBe(true);
    expect(cappedEnv?.installedPackagesTruncated).toBe(true);

    const full = await callTool(registerCodeEnvs, "code_env", {
      action: "get",
      envLang: "PYTHON",
      envName: "py-custom",
      full: true,
      maxPackages: 10,
    });
    const fullEnv = full.structured?.env as Record<string, unknown> | undefined;
    expect(full.isError).toBeFalsy();
    expect((fullEnv?.requestedPackages as unknown[] | undefined)?.length).toBe(25);
    expect((fullEnv?.installedPackages as unknown[] | undefined)?.length).toBe(25);
    expect(fullEnv?.packageLimit).toBeNull();
    expect(fullEnv?.requestedPackagesTruncated).toBe(false);
    expect(fullEnv?.installedPackagesTruncated).toBe(false);
  });
});
