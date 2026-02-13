import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { get } from "../client.js";
import { registerTool } from "./register-tool.js";

function summarizeItems(
	label: string,
	items: string[],
	maxItems = 20,
): string {
	if (items.length === 0) return `${label}: (none)`;
	const shown = items.slice(0, maxItems).join(", ");
	const remaining = items.length - Math.min(items.length, maxItems);
	return `${label}: ${items.length} (${shown}${remaining > 0 ? `, +${remaining} more` : ""})`;
}

export function register(server: McpServer) {
   registerTool(
      server,
		"code_env",
		{
			description:
				"Code env ops: list/get. get returns package summaries; set full=true for full package lists.",
			inputSchema: z.object({
				action: z.enum(["list", "get"]),
				envLang: z
					.enum(["PYTHON", "R"])
					.optional()
					,
				envName: z
					.string()
					.optional()
					,
				full: z
					.boolean()
					.optional()
					,
			}),
		},
		async ({ action, envLang, envName, full }) => {
			if (action === "list") {
				const envs = await get<
					Array<{
						envName: string;
						envLang: string;
						pythonInterpreter?: string;
						deploymentMode?: string;
					}>
				>("/public/api/admin/code-envs/");
				const text = envs
					.map(
						(e) =>
							`• ${e.envName} (${e.envLang}${e.pythonInterpreter ? `, ${e.pythonInterpreter}` : ""}${e.deploymentMode ? `, ${e.deploymentMode}` : ""})`,
					)
					.join("\n");
				return {
					content: [
						{ type: "text", text: text || "No code environments found." },
					],
				};
			}

			// action === "get"
			if (!envLang || !envName) {
				return {
					content: [
						{
							type: "text",
							text: "Error: envLang and envName are required for get.",
						},
					],
					isError: true,
				};
			}

			const langEnc = encodeURIComponent(envLang);
			const nameEnc = encodeURIComponent(envName);
			const env = await get<{
				envName: string;
				envLang: string;
				desc?: { pythonInterpreter?: string };
				actualPackageList?: string;
				specPackageList?: string;
			}>(`/public/api/admin/code-envs/${langEnc}/${nameEnc}/`);

			const parts: string[] = [
				`${env.envName} (${env.envLang}${env.desc?.pythonInterpreter ? `, ${env.desc.pythonInterpreter}` : ""})`,
			];

			// Show requested packages (spec) — these are what was configured
			if (env.specPackageList) {
				const requested = env.specPackageList
					.trim()
					.split("\n")
					.map((l) => l.trim())
					.filter(Boolean)
					.sort((a, b) => a.localeCompare(b));
				if (full) {
					parts.push(`\nRequested packages:\n${requested.join("\n")}`);
				} else {
					parts.push(`\n${summarizeItems("Requested packages", requested)}`);
				}
			}

			if (env.actualPackageList) {
				if (full) {
					// Full pip freeze output with versions
					parts.push(`\nInstalled packages:\n${env.actualPackageList.trim()}`);
				} else {
					// Summary — count + deterministic sample (names only)
					const pkgs = env.actualPackageList
						.trim()
						.split("\n")
						.filter((l) => l.trim());
					const names = pkgs
						.map((l) => l.split("==")[0].trim())
						.filter(Boolean)
						.sort((a, b) => a.localeCompare(b));
					parts.push(`\n${summarizeItems("Installed packages", names)}`);
				}
			}

			return {
				content: [{ type: "text", text: parts.join("\n") }],
			};
		},
	);
}
