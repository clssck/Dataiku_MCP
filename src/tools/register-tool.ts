import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { DataikuError } from "../client.js";
import {
  type ApiLatencyRecord,
  isToolLatencyDebugEnabled,
  runWithToolLatency,
} from "../debug-latency.js";

type ToolResult = {
  content?: Array<{ type: string; text?: string; [key: string]: unknown }>;
  structuredContent?: Record<string, unknown>;
  isError?: boolean;
  [key: string]: unknown;
};

function extractPrimaryText(result: ToolResult): string | undefined {
  if (!Array.isArray(result.content)) return undefined;
  for (const item of result.content) {
    if (item.type === "text" && typeof item.text === "string") {
      return item.text;
    }
  }
  return undefined;
}

function withStructuredContent(result: ToolResult): ToolResult {
  if (result.structuredContent && typeof result.structuredContent === "object") {
    return result;
  }

  const text = extractPrimaryText(result);
  return {
    ...result,
    structuredContent: {
      ok: result.isError !== true,
      ...(text !== undefined ? { text } : {}),
    },
  };
}

function toErrorToolResult(error: unknown): ToolResult {
  if (error instanceof DataikuError) {
    return {
      isError: true,
      content: [{ type: "text", text: error.message }],
      structuredContent: {
        ok: false,
        reason: "dataiku_error",
        status: error.status,
        statusText: error.statusText,
        category: error.category,
        retryable: error.retryable,
        retryHint: error.retryHint,
        ...(error.retry ? { retry: error.retry } : {}),
      },
    };
  }

  const message = error instanceof Error ? error.message : String(error);
  return {
    isError: true,
    content: [{ type: "text", text: message }],
    structuredContent: {
      ok: false,
      reason: "unexpected_error",
      message,
    },
  };
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return undefined;
}

function extractAction(args: unknown[]): string | null {
  const first = args[0];
  if (!first || typeof first !== "object" || Array.isArray(first)) return null;
  const action = (first as Record<string, unknown>).action;
  return typeof action === "string" ? action : null;
}

function withLatencyDebug(
  result: ToolResult,
  toolName: string,
  action: string | null,
  totalMs: number,
  apiCalls: ApiLatencyRecord[],
): ToolResult {
  const structured =
    result.structuredContent && typeof result.structuredContent === "object"
      ? result.structuredContent
      : {};
  const existingDebug = asRecord(structured.debug) ?? {};
  const apiDurationMs = apiCalls.reduce((sum, call) => sum + call.durationMs, 0);
  const slowestApiMs = apiCalls.reduce((max, call) => Math.max(max, call.durationMs), 0);

  return {
    ...result,
    structuredContent: {
      ...structured,
      debug: {
        ...existingDebug,
        latency: {
          enabled: true,
          tool: toolName,
          action,
          totalMs,
          apiCallCount: apiCalls.length,
          apiDurationMs,
          slowestApiMs,
          apiCalls,
        },
      },
    },
  };
}

export function registerTool<TArgs extends unknown[]>(
  server: McpServer,
  name: string,
  config: Record<string, unknown>,
  handler: (...args: TArgs) => ToolResult | Promise<ToolResult>,
) {
  return server.registerTool(
    name,
    config as never,
    (async (...args: TArgs) => {
      const latencyDebugEnabled = isToolLatencyDebugEnabled();
      const action = extractAction(args);
      const { result, totalMs, apiCalls } = await runWithToolLatency(
        latencyDebugEnabled,
        async () => {
          try {
            const raw = await handler(...args);
            return withStructuredContent(raw);
          } catch (error) {
            return withStructuredContent(toErrorToolResult(error));
          }
        },
      );

      return latencyDebugEnabled
        ? withLatencyDebug(result, name, action, totalMs, apiCalls)
        : result;
    }) as never,
  );
}
