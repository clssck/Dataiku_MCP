import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";
import { DataikuError, get } from "../../src/client.js";
import { registerTool } from "../../src/tools/register-tool.js";

type ToolCallResult = {
  text: string;
  isError: boolean | undefined;
  structured: Record<string, unknown> | undefined;
};

async function callRegisteredTool(
  handler: () => Promise<Record<string, unknown>> | Record<string, unknown>,
): Promise<ToolCallResult> {
  const server = new McpServer({ name: "test", version: "0.0.1" });
  registerTool(
    server,
    "sample",
    {
      description: "sample tool",
      inputSchema: z.object({ action: z.literal("run") }),
    },
    async () => handler(),
  );

  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);

  const client = new Client({ name: "test-client", version: "0.0.1" });
  await client.connect(clientTransport);

  try {
    const result = await client.callTool({
      name: "sample",
      arguments: { action: "run" },
    });
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

describe("registerTool error wrapping", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    delete process.env.DATAIKU_DEBUG_LATENCY;
  });

  it("returns DataikuError details with retry metadata in structured content", async () => {
    const result = await callRegisteredTool(() => {
      throw new DataikuError(
        429,
        "Too Many Requests",
        JSON.stringify({ message: "rate limited" }),
        {
          method: "GET",
          enabled: true,
          maxAttempts: 3,
          attempts: 3,
          retries: 2,
          delaysMs: [10, 20],
          timedOut: false,
        },
      );
    });

    expect(result.isError).toBe(true);
    expect(result.text).toContain("Retry attempts: 3/3");
    expect(result.structured).toMatchObject({
      ok: false,
      reason: "dataiku_error",
      status: 429,
      category: "transient",
      retryable: true,
      retry: {
        method: "GET",
        maxAttempts: 3,
        attempts: 3,
        retries: 2,
      },
    });
  });

  it("returns structured unexpected_error payload for unknown failures", async () => {
    const result = await callRegisteredTool(() => {
      throw new Error("boom");
    });

    expect(result.isError).toBe(true);
    expect(result.text).toContain("boom");
    expect(result.structured).toMatchObject({
      ok: false,
      reason: "unexpected_error",
      message: "boom",
    });
  });

  it("adds per-tool latency debug metrics when debug mode is enabled", async () => {
    const originalUrl = process.env.DATAIKU_URL;
    const originalKey = process.env.DATAIKU_API_KEY;
    const originalDebug = process.env.DATAIKU_DEBUG_LATENCY;

    process.env.DATAIKU_URL = "https://example.dataiku.io";
    process.env.DATAIKU_API_KEY = "test-token";
    process.env.DATAIKU_DEBUG_LATENCY = "1";

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        statusText: "OK",
      }),
    );

    try {
      const result = await callRegisteredTool(async () => {
        const response = await get<{ ok: boolean }>("/public/api/projects/");
        return {
          content: [{ type: "text", text: `ok=${response.ok}` }],
          structuredContent: { ok: true },
        };
      });

      expect(result.isError).toBeFalsy();
      const debug = result.structured?.debug as
        | {
            latency?: {
              enabled?: boolean;
              tool?: string;
              action?: string | null;
              apiCallCount?: number;
              apiCalls?: Array<{
                method?: string;
                outcome?: string;
                status?: number;
                path?: string;
              }>;
            };
          }
        | undefined;
      expect(debug?.latency?.enabled).toBe(true);
      expect(debug?.latency?.tool).toBe("sample");
      expect(debug?.latency?.action).toBe("run");
      expect(debug?.latency?.apiCallCount).toBe(1);
      expect(debug?.latency?.apiCalls?.[0]).toMatchObject({
        method: "GET",
        outcome: "success",
        status: 200,
        path: "/public/api/projects/",
      });
    } finally {
      fetchSpy.mockRestore();
      if (originalUrl) {
        process.env.DATAIKU_URL = originalUrl;
      } else {
        delete process.env.DATAIKU_URL;
      }
      if (originalKey) {
        process.env.DATAIKU_API_KEY = originalKey;
      } else {
        delete process.env.DATAIKU_API_KEY;
      }
      if (originalDebug) {
        process.env.DATAIKU_DEBUG_LATENCY = originalDebug;
      } else {
        delete process.env.DATAIKU_DEBUG_LATENCY;
      }
    }
  });
});
