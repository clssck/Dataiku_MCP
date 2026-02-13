import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

type ToolResult = {
   content?: Array<{ type: string; text?: string;[key: string]: unknown }>;
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

export function registerTool(
   server: McpServer,
   name: string,
   config: Record<string, unknown>,
   handler: (...args: any[]) => ToolResult | Promise<ToolResult>,
) {
   return server.registerTool(
      name,
      config as never,
      (async (...args: any[]) => {
         const result = await handler(...args);
         return withStructuredContent(result);
      }) as never,
   );
}
