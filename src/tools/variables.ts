import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { get, getProjectKey, putVoid } from "../client.js";
import { actionInput, actionSchema, optionalProjectKey } from "./action-schema.js";
import { registerTool } from "./register-tool.js";

interface ProjectVariables {
  standard: Record<string, unknown>;
  local: Record<string, unknown>;
}

function sortedKeys(record: Record<string, unknown> | undefined): string[] {
  return Object.keys(record ?? {}).sort((a, b) => a.localeCompare(b));
}

function summarizeKeys(label: string, keys: string[], maxItems = 20): string {
  if (keys.length === 0) return `${label}: (none)`;
  const shown = keys.slice(0, maxItems).join(", ");
  const remaining = keys.length - Math.min(keys.length, maxItems);
  return `${label}: ${shown}${remaining > 0 ? ` (+${remaining} more)` : ""}`;
}

const variableInputSchema = actionSchema([
  actionInput("get", {
    projectKey: optionalProjectKey,
    maxKeys: z.number().int().min(1).optional(),
  }),
  actionInput("set", {
    projectKey: optionalProjectKey,
    standard: z.record(z.string(), z.unknown()).optional(),
    local: z.record(z.string(), z.unknown()).optional(),
  }),
]);

export function register(server: McpServer) {
  registerTool(
    server,
    "variable",
    {
      description:
        "Project variable ops: get/set. set merges provided standard/local keys with existing values.",
      inputSchema: variableInputSchema,
    },
    async ({ action, projectKey, standard, local, maxKeys }) => {
      const pk = getProjectKey(projectKey);
      const enc = encodeURIComponent(pk);
      const varsPath = `/public/api/projects/${enc}/variables/`;

      if (action === "get") {
        const vars = await get<ProjectVariables>(varsPath);
        const standardKeys = sortedKeys(vars.standard);
        const localKeys = sortedKeys(vars.local);
        const keyLimit = Math.max(1, maxKeys ?? 100);
        const standardKeysOut = standardKeys.slice(0, keyLimit);
        const localKeysOut = localKeys.slice(0, keyLimit);
        const parts = [
          "Variables:",
          `Standard keys: ${standardKeys.length}`,
          `Local keys: ${localKeys.length}`,
          summarizeKeys("Standard key names", standardKeys),
          summarizeKeys("Local key names", localKeys),
        ];
        return {
          content: [{ type: "text", text: parts.join("\n") }],
          structuredContent: {
            ok: true,
            projectKey: pk,
            standardKeyCount: standardKeys.length,
            localKeyCount: localKeys.length,
            standardKeys: standardKeysOut,
            localKeys: localKeysOut,
            keyLimit,
            standardKeysTruncated: standardKeysOut.length < standardKeys.length,
            localKeysTruncated: localKeysOut.length < localKeys.length,
          },
        };
      }

      // action === "set" — read-merge-write
      if (!standard && !local) {
        return {
          content: [
            {
              type: "text",
              text: "Error: at least one of standard or local is required for set.",
            },
          ],
          structuredContent: {
            ok: false,
            projectKey: pk,
            reason: "missing_patch",
          },
          isError: true,
        };
      }

      const existing = await get<ProjectVariables>(varsPath);
      const merged: ProjectVariables = {
        standard: { ...existing.standard, ...standard },
        local: { ...existing.local, ...local },
      };

      await putVoid(varsPath, merged);

      const changedStandard = sortedKeys(standard);
      const changedLocal = sortedKeys(local);
      const mergedStandard = sortedKeys(merged.standard);
      const mergedLocal = sortedKeys(merged.local);

      const parts = [
        "Variables updated.",
        `Changed standard keys: ${changedStandard.length}`,
        `Changed local keys: ${changedLocal.length}`,
        `Total standard keys: ${mergedStandard.length}`,
        `Total local keys: ${mergedLocal.length}`,
        summarizeKeys("Changed standard names", changedStandard),
        summarizeKeys("Changed local names", changedLocal),
      ];

      return {
        content: [{ type: "text", text: parts.join("\n") }],
        structuredContent: {
          ok: true,
          projectKey: pk,
          changedStandardKeys: changedStandard,
          changedLocalKeys: changedLocal,
          totalStandardKeys: mergedStandard.length,
          totalLocalKeys: mergedLocal.length,
        },
      };
    },
  );
}
