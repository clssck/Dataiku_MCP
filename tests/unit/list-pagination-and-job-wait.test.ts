import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { beforeEach, describe, expect, it, vi } from "vitest";

const clientMocks = vi.hoisted(() => ({
  del: vi.fn(),
  get: vi.fn(),
  getProjectKey: vi.fn((projectKey?: string) => projectKey ?? "TEST_PROJECT"),
  getText: vi.fn(),
  post: vi.fn(),
  put: vi.fn(),
  stream: vi.fn(),
  upload: vi.fn(),
}));

vi.mock("../../src/client.js", () => clientMocks);

import { register as registerDatasets } from "../../src/tools/datasets.js";
import { register as registerCodeEnvs } from "../../src/tools/code-envs.js";
import { register as registerFolders } from "../../src/tools/folders.js";
import { register as registerJobs } from "../../src/tools/jobs.js";
import { register as registerProjects } from "../../src/tools/projects.js";
import { register as registerRecipes } from "../../src/tools/recipes.js";
import { register as registerScenarios } from "../../src/tools/scenarios.js";

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

describe("Pagination + wait/buildAndWait behavior", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    clientMocks.getProjectKey.mockImplementation(
      (projectKey?: string) => projectKey ?? "TEST_PROJECT",
    );
  });

  it("project list supports query + offset + limit with structured page metadata", async () => {
    clientMocks.get.mockResolvedValue([
      { projectKey: "A", name: "Alpha" },
      { projectKey: "B", name: "Beta" },
      { projectKey: "C", name: "Gamma" },
    ]);

    const result = await callTool(registerProjects, "project", {
      action: "list",
      projectKey: "PROJ",
      query: "a",
      offset: 1,
      limit: 1,
    });

    expect(result.isError).toBeFalsy();
    expect(result.text).toContain("• B: Beta");
    expect(result.structured?.total).toBe(3);
    expect(result.structured?.filtered).toBe(3);
    expect(result.structured?.offset).toBe(1);
    expect(result.structured?.limit).toBe(1);
    expect(result.structured?.query).toBe("a");
  });

  it("dataset list supports query + limit", async () => {
    clientMocks.get.mockResolvedValue([
      { name: "orders", type: "Filesystem" },
      { name: "sales_daily", type: "Filesystem" },
      { name: "sales_monthly", type: "Filesystem" },
    ]);

    const result = await callTool(registerDatasets, "dataset", {
      action: "list",
      projectKey: "PROJ",
      query: "sales",
      limit: 1,
    });

    expect(result.isError).toBeFalsy();
    expect(result.text).toMatch(/sales_/);
    expect(result.structured?.filtered).toBe(2);
    expect(result.structured?.limit).toBe(1);
  });

  it("recipe list supports offset + limit", async () => {
    clientMocks.get.mockResolvedValue([
      { name: "r0", type: "python" },
      { name: "r1", type: "python" },
      { name: "r2", type: "python" },
    ]);

    const result = await callTool(registerRecipes, "recipe", {
      action: "list",
      projectKey: "PROJ",
      offset: 1,
      limit: 1,
    });

    expect(result.isError).toBeFalsy();
    expect(result.text).toContain("• r1 (python)");
    expect(result.structured?.offset).toBe(1);
    expect(result.structured?.limit).toBe(1);
  });

  it("scenario list supports query", async () => {
    clientMocks.get.mockResolvedValue([
      { id: "nightly_orders", name: "Nightly Orders", active: true },
      { id: "hourly_sales", name: "Hourly Sales", active: false },
    ]);

    const result = await callTool(registerScenarios, "scenario", {
      action: "list",
      projectKey: "PROJ",
      query: "nightly",
    });

    expect(result.isError).toBeFalsy();
    expect(result.text).toContain("nightly_orders");
    expect(result.structured?.filtered).toBe(1);
  });

  it("managed_folder list supports query + limit", async () => {
    clientMocks.get.mockResolvedValue([
      { id: "mf_raw", name: "Raw Files", type: "Filesystem" },
      { id: "mf_exports", name: "Exports", type: "Filesystem" },
    ]);

    const result = await callTool(registerFolders, "managed_folder", {
      action: "list",
      projectKey: "PROJ",
      query: "export",
      limit: 1,
    });

    expect(result.isError).toBeFalsy();
    expect(result.text).toContain("mf_exports");
    expect(result.structured?.filtered).toBe(1);
    expect(result.structured?.limit).toBe(1);
  });

  it("managed_folder contents supports query + offset + limit", async () => {
    clientMocks.get.mockResolvedValue({
      items: [
        { path: "sales/orders.csv", size: 1024, lastModified: Date.UTC(2026, 0, 1) },
        { path: "sales/customers.csv", size: 2048, lastModified: Date.UTC(2026, 0, 2) },
        { path: "logs/run.log", size: 512, lastModified: Date.UTC(2026, 0, 3) },
      ],
    });

    const result = await callTool(registerFolders, "managed_folder", {
      action: "contents",
      projectKey: "PROJ",
      folderId: "mf_exports",
      query: "sales",
      offset: 1,
      limit: 1,
    });

    expect(result.isError).toBeFalsy();
    expect(result.text).toContain("Showing 1 of 2 files");
    expect(result.structured?.total).toBe(3);
    expect(result.structured?.filtered).toBe(2);
    expect(result.structured?.offset).toBe(1);
    expect(result.structured?.limit).toBe(1);
    expect(result.structured?.hasMore).toBe(false);
  });

  it("managed_folder contents keeps requested limit metadata for empty folders", async () => {
    clientMocks.get.mockResolvedValue({ items: [] });

    const result = await callTool(registerFolders, "managed_folder", {
      action: "contents",
      projectKey: "PROJ",
      folderId: "mf_exports",
      limit: 10,
    });

    expect(result.isError).toBeFalsy();
    expect(result.text).toContain("Folder is empty.");
    expect(result.structured?.total).toBe(0);
    expect(result.structured?.filtered).toBe(0);
    expect(result.structured?.limit).toBe(10);
    expect(result.structured?.hasMore).toBe(false);
  });

  it("code_env list defaults to bounded page size when limit is omitted", async () => {
    const envs = Array.from({ length: 150 }, (_, index) => ({
      envName: `env_${index.toString().padStart(3, "0")}`,
      envLang: "PYTHON",
    }));
    clientMocks.get.mockResolvedValue(envs);

    const result = await callTool(registerCodeEnvs, "code_env", {
      action: "list",
    });

    expect(result.isError).toBeFalsy();
    expect(result.structured?.total).toBe(150);
    expect(result.structured?.filtered).toBe(150);
    expect(result.structured?.offset).toBe(0);
    expect(result.structured?.limit).toBe(100);
    expect(result.structured?.hasMore).toBe(true);
    expect((result.structured?.envs as unknown[] | undefined)?.length).toBe(100);
  });

  it("job list supports query + offset + limit", async () => {
    clientMocks.get.mockResolvedValue([
      { def: { id: "job_a", initiator: "alice" }, state: "DONE", startTime: Date.UTC(2026, 0, 1) },
      { def: { id: "job_b", initiator: "bob" }, state: "RUNNING", startTime: Date.UTC(2026, 0, 2) },
      {
        def: { id: "job_c", initiator: "carol" },
        state: "FAILED",
        startTime: Date.UTC(2026, 0, 3),
      },
    ]);

    const result = await callTool(registerJobs, "job", {
      action: "list",
      projectKey: "PROJ",
      query: "job",
      offset: 1,
      limit: 1,
    });

    expect(result.isError).toBeFalsy();
    expect(result.text).toContain("• job_b");
    expect(result.structured?.total).toBe(3);
    expect(result.structured?.filtered).toBe(3);
    expect(result.structured?.offset).toBe(1);
    expect(result.structured?.limit).toBe(1);
  });

  it("job buildAndWait waits for terminal state", async () => {
    clientMocks.post.mockResolvedValue({ id: "job_42" });
    clientMocks.get
      .mockResolvedValueOnce({
        baseStatus: { def: { id: "job_42", type: "RECURSIVE_BUILD" }, state: "RUNNING" },
        globalState: { done: 0, total: 1, failed: 0, running: 1 },
      })
      .mockResolvedValueOnce({
        baseStatus: { def: { id: "job_42", type: "RECURSIVE_BUILD" }, state: "DONE" },
        globalState: { done: 1, total: 1, failed: 0, running: 0 },
      });

    const result = await callTool(registerJobs, "job", {
      action: "buildAndWait",
      projectKey: "PROJ",
      datasetName: "orders",
      pollIntervalMs: 1,
      timeoutMs: 5_000,
    });

    expect(result.isError).toBeFalsy();
    expect(result.text).toContain("Job started: job_42");
    expect(result.text).toContain("State: DONE");
    expect(result.structured?.startedJobId).toBe("job_42");
    expect(result.structured?.state).toBe("DONE");
    expect(result.structured?.normalizedState).toBe("terminalSuccess");
  });

  it("job buildAndWait marks terminal failure states as errors", async () => {
    clientMocks.post.mockResolvedValue({ id: "job_failed" });
    clientMocks.get
      .mockResolvedValueOnce({
        baseStatus: { def: { id: "job_failed", type: "RECURSIVE_BUILD" }, state: "RUNNING" },
        globalState: { done: 0, total: 1, failed: 0, running: 1 },
      })
      .mockResolvedValueOnce({
        baseStatus: { def: { id: "job_failed", type: "RECURSIVE_BUILD" }, state: "FAILED" },
        globalState: { done: 0, total: 1, failed: 1, running: 0 },
      });

    const result = await callTool(registerJobs, "job", {
      action: "buildAndWait",
      projectKey: "PROJ",
      datasetName: "orders",
      pollIntervalMs: 1,
      timeoutMs: 5_000,
    });

    expect(result.isError).toBe(true);
    expect(result.text).toContain("Job started: job_failed");
    expect(result.text).toContain("State: FAILED");
    expect(result.text).toContain("terminal failure state");
    expect(result.structured?.ok).toBe(false);
    expect(result.structured?.terminalSuccess).toBe(false);
    expect(result.structured?.startedJobId).toBe("job_failed");
    expect(result.structured?.normalizedState).toBe("terminalFailure");
  });

  it("job wait timeout reports normalized timeout state", async () => {
    clientMocks.get.mockResolvedValue({
      baseStatus: { def: { id: "job_slow", type: "RECURSIVE_BUILD" }, state: "RUNNING" },
      globalState: { done: 0, total: 1, failed: 0, running: 1 },
    });

    const result = await callTool(registerJobs, "job", {
      action: "wait",
      projectKey: "PROJ",
      jobId: "job_slow",
      pollIntervalMs: 1,
      timeoutMs: 1,
    });

    expect(result.isError).toBe(true);
    expect(result.text).toContain("Timed out waiting for job job_slow");
    expect(result.structured?.normalizedState).toBe("timeout");
    expect(result.structured?.state).toBe("RUNNING");
  });

  it("job get includes nonTerminal normalized state for running jobs", async () => {
    clientMocks.get.mockResolvedValue({
      baseStatus: {
        def: { id: "job_live", type: "RECURSIVE_BUILD", initiator: "alice" },
        state: "RUNNING",
      },
      globalState: { done: 0, total: 2, failed: 0, running: 1 },
    });

    const result = await callTool(registerJobs, "job", {
      action: "get",
      projectKey: "PROJ",
      jobId: "job_live",
    });

    expect(result.isError).toBeFalsy();
    expect(result.structured?.state).toBe("RUNNING");
    expect(result.structured?.normalizedState).toBe("nonTerminal");
  });
});
