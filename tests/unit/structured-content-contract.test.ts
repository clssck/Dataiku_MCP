import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DataikuError } from "../../src/client.js";

const clientMocks = vi.hoisted(() => ({
  del: vi.fn(),
  get: vi.fn(),
  getProjectKey: vi.fn((projectKey?: string) => projectKey ?? "TEST_PROJECT"),
  getText: vi.fn(),
  post: vi.fn(),
  put: vi.fn(),
  putVoid: vi.fn(),
  stream: vi.fn(),
  upload: vi.fn(),
}));

vi.mock("../../src/client.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../src/client.js")>();
  return {
    ...actual,
    ...clientMocks,
  };
});

import { register as registerJobs } from "../../src/tools/jobs.js";
import { register as registerDatasets } from "../../src/tools/datasets.js";
import { register as registerProjects } from "../../src/tools/projects.js";
import { register as registerVariables } from "../../src/tools/variables.js";

type ToolCallResult = {
  text: string;
  isError: boolean | undefined;
  structured: Record<string, unknown> | undefined;
};

async function callTool(
  registerTool: (server: McpServer) => void,
  name: string,
  args: Record<string, unknown>,
): Promise<ToolCallResult> {
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

describe("Structured content contract", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    clientMocks.getProjectKey.mockImplementation(
      (projectKey?: string) => projectKey ?? "TEST_PROJECT",
    );
  });

  it("project list includes stable pagination + counts shape", async () => {
    clientMocks.get.mockResolvedValue([
      { projectKey: "A", name: "Alpha", shortDesc: "first" },
      { projectKey: "B", name: "Beta", shortDesc: "second" },
    ]);

    const result = await callTool(registerProjects, "project", {
      action: "list",
      limit: 1,
      offset: 0,
      query: "a",
    });

    expect(result.isError).toBeFalsy();
    expect(result.structured).toBeDefined();
    expect(Object.keys(result.structured ?? {}).sort()).toEqual([
      "filtered",
      "hasMore",
      "items",
      "limit",
      "offset",
      "ok",
      "query",
      "total",
    ]);
    expect(result.structured).toMatchObject({
      ok: true,
      total: 2,
      filtered: 2,
      offset: 0,
      hasMore: true,
      limit: 1,
      query: "a",
    });
  });

  it("job list includes stable pagination + ids/counts shape", async () => {
    clientMocks.get.mockResolvedValue([
      {
        def: { id: "job_001", initiator: "alice" },
        state: "DONE",
        startTime: Date.UTC(2026, 0, 1),
      },
      {
        def: { id: "job_002", initiator: "bob" },
        state: "RUNNING",
        startTime: Date.UTC(2026, 0, 2),
      },
    ]);

    const result = await callTool(registerJobs, "job", {
      action: "list",
      query: "job",
      limit: 1,
      offset: 0,
    });

    expect(result.isError).toBeFalsy();
    expect(result.structured).toMatchObject({
      ok: true,
      total: 2,
      filtered: 2,
      offset: 0,
      hasMore: true,
      limit: 1,
      query: "job",
    });

    const first = (result.structured?.items as Array<{ def: { id: string } }> | undefined)?.[0];
    expect(first?.def.id).toBe("job_001");
  });

  it("dataset get is summary-first by default", async () => {
    clientMocks.get.mockResolvedValue({
      name: "orders_ds",
      type: "Filesystem",
      managed: true,
      projectKey: "PROJ",
      params: {
        connection: "filesystem_managed",
        path: "orders_ds",
      },
      formatType: "csv",
      formatParams: { separator: "\t", charset: "utf8", compress: "gz" },
      schema: {
        columns: [
          { name: "order_id", type: "string" },
          { name: "amount", type: "double" },
        ],
      },
      tags: ["finance"],
    });

    const result = await callTool(registerDatasets, "dataset", {
      action: "get",
      projectKey: "PROJ",
      datasetName: "orders_ds",
    });

    expect(result.isError).toBeFalsy();
    expect(result.structured).toMatchObject({
      ok: true,
      dataset: {
        name: "orders_ds",
        type: "Filesystem",
        managed: true,
        connection: "filesystem_managed",
        schemaColumnCount: 2,
        tagCount: 1,
      },
    });
    expect(result.structured?.definition).toBeUndefined();
    expect(result.text).toContain("includeDefinition=true");
  });

  it("dataset get includes full definition when includeDefinition=true", async () => {
    const definition = {
      name: "orders_ds",
      type: "Filesystem",
      params: { connection: "filesystem_managed" },
    };
    clientMocks.get.mockResolvedValue(definition);

    const result = await callTool(registerDatasets, "dataset", {
      action: "get",
      projectKey: "PROJ",
      datasetName: "orders_ds",
      includeDefinition: true,
    });

    expect(result.isError).toBeFalsy();
    expect(result.structured?.definition).toEqual(definition);
    expect(result.text).toContain("Definition: included");
  });

  it("job get is summary-first by default", async () => {
    clientMocks.get.mockResolvedValue({
      baseStatus: {
        def: {
          id: "job_001",
          type: "RECURSIVE_BUILD",
          initiator: "alice",
          outputs: [{ targetDataset: "orders" }],
        },
        state: "RUNNING",
        jobStartTime: Date.UTC(2026, 0, 1, 12, 0, 0),
        activities: {
          prep: {
            recipeName: "prep_orders",
            state: "RUNNING",
            totalTime: 3_000,
            runningTime: 2_500,
          },
        },
      },
      globalState: { done: 1, failed: 0, running: 1, total: 2 },
    });

    const result = await callTool(registerJobs, "job", {
      action: "get",
      projectKey: "PROJ",
      jobId: "job_001",
    });

    expect(result.isError).toBeFalsy();
    expect(result.structured).toMatchObject({
      ok: true,
      state: "RUNNING",
      normalizedState: "nonTerminal",
      job: {
        id: "job_001",
        type: "RECURSIVE_BUILD",
        initiator: "alice",
        targetDatasets: ["orders"],
        activityCount: 1,
        progress: { done: 1, failed: 0, running: 1, total: 2 },
      },
    });
    expect(result.structured?.definition).toBeUndefined();
    expect(result.text).toContain("includeDefinition=true");
  });

  it("job get includes full definition when includeDefinition=true", async () => {
    const definition = {
      baseStatus: {
        def: { id: "job_001", type: "RECURSIVE_BUILD" },
        state: "DONE",
      },
      globalState: { done: 1, failed: 0, running: 0, total: 1 },
    };
    clientMocks.get.mockResolvedValue(definition);

    const result = await callTool(registerJobs, "job", {
      action: "get",
      projectKey: "PROJ",
      jobId: "job_001",
      includeDefinition: true,
    });

    expect(result.isError).toBeFalsy();
    expect(result.structured?.definition).toEqual(definition);
    expect(result.text).toContain("Definition: included");
  });

  it("project list defaults to bounded limit when limit is omitted", async () => {
    const projects = Array.from({ length: 150 }, (_, index) => ({
      projectKey: `P_${index.toString().padStart(3, "0")}`,
      name: `Project ${index}`,
    }));
    clientMocks.get.mockResolvedValue(projects);

    const result = await callTool(registerProjects, "project", {
      action: "list",
    });

    expect(result.isError).toBeFalsy();
    expect(result.structured).toMatchObject({
      ok: true,
      total: 150,
      filtered: 150,
      offset: 0,
      limit: 100,
      hasMore: true,
      query: null,
    });
    expect((result.structured?.items as unknown[] | undefined)?.length).toBe(100);
  });

  it("project map includes map + truncation contract fields", async () => {
    clientMocks.get.mockImplementation(async (path: string) => {
      if (path.endsWith("/flow/graph/")) {
        return {
          nodes: {
            ds_a: { type: "DATASET", ref: "ds_a", predecessors: [], successors: ["prep_a"] },
            prep_a: {
              type: "RECIPE",
              ref: "prep_a",
              predecessors: ["ds_a"],
              successors: ["ds_b"],
            },
            ds_b: { type: "DATASET", ref: "ds_b", predecessors: ["prep_a"], successors: [] },
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

    const result = await callTool(registerProjects, "project", {
      action: "map",
      projectKey: "PROJ",
      maxNodes: 2,
      maxEdges: 1,
    });

    expect(result.isError).toBeFalsy();
    expect(result.structured).toMatchObject({ ok: true });
    expect(Object.keys(result.structured ?? {}).sort()).toEqual(["map", "ok", "truncation"]);

    const map = result.structured?.map as Record<string, unknown> | undefined;
    expect(map?.projectKey).toBe("PROJ");
    expect(Array.isArray(map?.nodes)).toBe(true);
    expect(Array.isArray(map?.edges)).toBe(true);
    expect(Array.isArray(map?.roots)).toBe(true);
    expect(Array.isArray(map?.leaves)).toBe(true);

    const truncation = result.structured?.truncation as Record<string, unknown> | undefined;
    expect(Object.keys(truncation ?? {}).sort()).toEqual([
      "edgeCountAfter",
      "edgeCountBefore",
      "maxEdges",
      "maxNodes",
      "nodeCountAfter",
      "nodeCountBefore",
      "truncated",
    ]);
    expect(truncation).toMatchObject({
      truncated: true,
      maxNodes: 2,
      maxEdges: 1,
      nodeCountBefore: 3,
      nodeCountAfter: 2,
      edgeCountBefore: 2,
      edgeCountAfter: 0,
    });
  });

  it("validation error responses keep reason contract", async () => {
    const result = await callTool(registerVariables, "variable", {
      action: "set",
      projectKey: "PROJ",
    });

    expect(result.isError).toBe(true);
    expect(result.structured).toMatchObject({
      ok: false,
      reason: "missing_patch",
      projectKey: "PROJ",
    });
  });

  it("variable get defaults to capped key arrays with truncation metadata", async () => {
    const standard = Object.fromEntries(
      Array.from({ length: 140 }, (_, index) => [`s_${index.toString().padStart(3, "0")}`, index]),
    );
    const local = Object.fromEntries(
      Array.from({ length: 130 }, (_, index) => [`l_${index.toString().padStart(3, "0")}`, index]),
    );

    clientMocks.get.mockResolvedValue({ standard, local });

    const result = await callTool(registerVariables, "variable", {
      action: "get",
      projectKey: "PROJ",
    });

    expect(result.isError).toBeFalsy();
    expect(result.structured).toMatchObject({
      ok: true,
      keyLimit: 100,
      standardKeyCount: 140,
      localKeyCount: 130,
      standardKeysTruncated: true,
      localKeysTruncated: true,
    });
    expect((result.structured?.standardKeys as unknown[] | undefined)?.length).toBe(100);
    expect((result.structured?.localKeys as unknown[] | undefined)?.length).toBe(100);
  });

  it("transport/dataiku errors include category + retryability contract", async () => {
    clientMocks.get.mockRejectedValue(
      new DataikuError(429, "Too Many Requests", JSON.stringify({ message: "rate limited" })),
    );

    const result = await callTool(registerProjects, "project", {
      action: "get",
      projectKey: "PROJ",
    });

    expect(result.isError).toBe(true);
    expect(result.structured).toMatchObject({
      ok: false,
      reason: "dataiku_error",
      status: 429,
      category: "transient",
      retryable: true,
    });
    expect(result.text).toContain("Error type: transient");
  });
});
