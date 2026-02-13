import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { get, getProjectKey, putVoid } from "../client.js";
import { registerTool } from "./register-tool.js";

const optionalProjectKey = z.string().optional();

interface ProjectVariables {
	standard: Record<string, unknown>;
	local: Record<string, unknown>;
}

function sortedKeys(record: Record<string, unknown> | undefined): string[] {
	return Object.keys(record ?? {}).sort((a, b) => a.localeCompare(b));
}

function summarizeKeys(
	label: string,
	keys: string[],
	maxItems = 20,
): string {
	if (keys.length === 0) return `${label}: (none)`;
	const shown = keys.slice(0, maxItems).join(", ");
	const remaining = keys.length - Math.min(keys.length, maxItems);
	return `${label}: ${shown}${remaining > 0 ? ` (+${remaining} more)` : ""}`;
}

export function register(server: McpServer) {
   registerTool(
      server,
		"variable",
		{
			description:
				"Project variable ops: get/set. set merges provided standard/local keys with existing values.",
			inputSchema: z.object({
				action: z.enum(["get", "set"]),
				projectKey: optionalProjectKey,
					standard: z
						.record(z.string(), z.unknown())
						.optional()
						,
					local: z
						.record(z.string(), z.unknown())
						.optional()
						,
			}),
		},
		async ({ action, projectKey, standard, local }) => {
			const pk = getProjectKey(projectKey);
			const enc = encodeURIComponent(pk);
			const varsPath = `/public/api/projects/${enc}/variables/`;

			if (action === "get") {
				const vars = await get<ProjectVariables>(varsPath);
				const standardKeys = sortedKeys(vars.standard);
				const localKeys = sortedKeys(vars.local);
				const parts = [
					"Variables:",
					`Standard keys: ${standardKeys.length}`,
					`Local keys: ${localKeys.length}`,
					summarizeKeys("Standard key names", standardKeys),
					summarizeKeys("Local key names", localKeys),
				];

				return {
					content: [{ type: "text", text: parts.join("\n") }],
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
			};
		},
	);
}
