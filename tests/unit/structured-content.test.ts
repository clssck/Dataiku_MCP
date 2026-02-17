import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { beforeEach, describe, expect, it, vi } from "vitest";

const clientMocks = vi.hoisted(() => ({
  get: vi.fn(),
  getProjectKey: vi.fn((projectKey?: string) => projectKey ?? "TEST_PROJECT"),
  putVoid: vi.fn(),
}));

vi.mock("../../src/client.js", () => clientMocks);

import { register as registerVariables } from "../../src/tools/variables.js";

type ToolCallResult = {
  text: string;
  isError: boolean | undefined;
  structured: Record<string, unknown> | undefined;
};

async function callTool(args: Record<string, unknown>): Promise<ToolCallResult> {
  const server = new McpServer({ name: "test", version: "0.0.1" });
  registerVariables(server);

  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);

  const client = new Client({ name: "test-client", version: "0.0.1" });
  await client.connect(clientTransport);

  try {
    const result = await client.callTool({ name: "variable", arguments: args });
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

describe("Structured Content", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    clientMocks.getProjectKey.mockImplementation(
      (projectKey?: string) => projectKey ?? "TEST_PROJECT",
    );
  });

  it("returns structuredContent on validation errors", async () => {
    const result = await callTool({ action: "set", projectKey: "PROJ" });

    expect(result.isError).toBe(true);
    expect(result.text).toContain("at least one of standard or local");
    expect(result.structured).toBeDefined();
    expect(result.structured?.ok).toBe(false);
    expect(result.structured?.reason).toBe("missing_patch");
  });

  it("returns structuredContent on successful responses", async () => {
    clientMocks.get.mockResolvedValue({ standard: {}, local: {} });
    clientMocks.putVoid.mockResolvedValue(undefined);

    const result = await callTool({
      action: "set",
      projectKey: "PROJ",
      standard: { foo: "bar" },
    });

    expect(result.isError).toBeFalsy();
    expect(result.text).toContain("Variables updated");
    expect(result.structured).toBeDefined();
    expect(result.structured?.ok).toBe(true);
    expect(result.structured?.changedStandardKeys).toEqual(["foo"]);
  });
});
