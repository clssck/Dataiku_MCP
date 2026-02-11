import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { del, get, getProjectKey, post, put } from "../client.js";
import { deepMerge } from "./deep-merge.js";

const optionalProjectKey = z.string().optional();

export function register(server: McpServer) {
	server.registerTool(
		"scenario",
		{
			description:
				"Scenario ops: list/run/status/get/create/update/delete. get is summary-first; use includeScript/includeDefinition for details.",
			inputSchema: z.object({
				action: z
					.enum(["list", "run", "status", "get", "create", "update", "delete"])
					,
				projectKey: optionalProjectKey,
				scenarioId: z
					.string()
					.optional()
					,
				includeScript: z
					.boolean()
					.optional()
					,
				includeDefinition: z
					.boolean()
					.optional()
					,
				definitionMaxLines: z
					.number()
					.int()
					.min(1)
					.max(2000)
					.optional()
					,
				name: z.string().optional(),
				scenarioType: z
					.enum(["step_based", "custom_python"])
					.optional()
					,
					data: z
						.record(z.string(), z.unknown())
						.optional()
						,
			}),
		},
		async (args) => {
			const { action, projectKey, scenarioId } = args;
			const pk = getProjectKey(projectKey);
			const enc = encodeURIComponent(pk);

			if (action === "list") {
				const scenarios = await get<
					Array<{ id: string; name?: string; active?: boolean }>
				>(`/public/api/projects/${enc}/scenarios/`);
				const text = scenarios
					.map(
						(s) =>
							`• ${s.id}${s.name ? `: ${s.name}` : ""}${s.active !== undefined ? ` [${s.active ? "active" : "inactive"}]` : ""}`,
					)
					.join("\n");
				return {
					content: [{ type: "text", text: text || "No scenarios found." }],
				};
			}

			if (action === "create") {
				if (!scenarioId || !args.name) {
					return {
						content: [
							{
								type: "text",
								text: "Error: scenarioId and name are required for create.",
							},
						],
						isError: true,
					};
				}

				const scenarioType = args.scenarioType ?? "step_based";
				const body = {
					id: scenarioId,
					name: args.name,
					projectKey: pk,
					type: scenarioType,
					params:
						scenarioType === "step_based"
							? { steps: [], triggers: [], reporters: [] }
							: {},
					...args.data,
				};

				await post<Record<string, unknown>>(
					`/public/api/projects/${enc}/scenarios/`,
					body,
				);
				return {
					content: [
						{
							type: "text",
							text: `Scenario "${args.name}" (${scenarioId}) created. Type: ${scenarioType}.`,
						},
					],
				};
			}

			if (!scenarioId) {
				return {
					content: [
						{
							type: "text",
							text: "Error: scenarioId is required for this action.",
						},
					],
					isError: true,
				};
			}
			const scEnc = encodeURIComponent(scenarioId);

			if (action === "run") {
				const result = await post<{ id?: string; runId?: string }>(
					`/public/api/projects/${enc}/scenarios/${scEnc}/run/`,
				);
				const runId = result.id ?? result.runId ?? "unknown";
				return {
					content: [
						{
							type: "text",
							text: `Scenario ${scenarioId} triggered. Run ID: ${runId}`,
						},
					],
				};
			}

			if (action === "status") {
				const s = await get<{
					id?: string;
					name?: string;
					active?: boolean;
					running?: boolean;
					nextRun?: number;
					lastRun?: {
						runId?: string;
						outcome?: string;
						start?: number;
						end?: number;
						trigger?: { type?: string };
					};
				}>(`/public/api/projects/${enc}/scenarios/${scEnc}/light/`);
				const parts: string[] = [
					`Scenario: ${s.id ?? scenarioId}${s.name ? ` — "${s.name}"` : ""}`,
					`Active: ${s.active ?? false} | Running: ${s.running ?? false}`,
				];
				if (s.nextRun) {
					parts.push(`Next run: ${new Date(s.nextRun).toISOString()}`);
				}
				if (s.lastRun) {
					const lr = s.lastRun;
					const dur =
						lr.start && lr.end
							? `${((lr.end - lr.start) / 1000).toFixed(0)}s`
							: "?";
					parts.push(
						`Last run: ${lr.outcome ?? "unknown"} (${dur})${lr.trigger?.type ? ` trigger=${lr.trigger.type}` : ""}`,
					);
				}
				return { content: [{ type: "text", text: parts.join("\n") }] };
			}

			if (action === "get") {
				const sc = await get<{
					id: string;
					name?: string;
					type?: string;
					active?: boolean;
					projectKey?: string;
					params?: {
						steps?: unknown[];
						triggers?: unknown[];
						reporters?: unknown[];
						customScript?: { script?: string };
					};
					versionTag?: { versionNumber: number };
				}>(`/public/api/projects/${enc}/scenarios/${scEnc}/`);
				const parts: string[] = [
					`Scenario: ${sc.id}${sc.name ? ` — "${sc.name}"` : ""}`,
					`Type: ${sc.type ?? "unknown"} | Active: ${sc.active ?? false}`,
				];
				if (sc.versionTag) {
					parts.push(`Version: ${sc.versionTag.versionNumber}`);
				}
				const p = sc.params ?? {};
				if (p.steps?.length) parts.push(`Steps: ${p.steps.length}`);
				if (p.triggers?.length) parts.push(`Triggers: ${p.triggers.length}`);
				if (p.reporters?.length) parts.push(`Reporters: ${p.reporters.length}`);
				if (sc.type === "custom_python" && p.customScript?.script) {
					const script = p.customScript.script;
					const maxLines = args.definitionMaxLines ?? 120;
					const scriptLines = script.split("\n");
					parts.push(
						`Script: present (${scriptLines.length} lines, ${script.length} chars)`,
					);
					if (args.includeScript) {
						parts.push(`\nScript:\n${scriptLines.slice(0, maxLines).join("\n")}`);
						if (scriptLines.length > maxLines) {
							parts.push(
								`... (${scriptLines.length - maxLines} more lines not shown)`,
							);
						}
					} else {
						parts.push(
							"Tip: pass includeScript=true to include script body.",
						);
					}
				}
				// Include step/trigger/reporter details as compact JSON for editing
				if (p.steps?.length || p.triggers?.length || p.reporters?.length) {
					parts.push("Definition: present (steps/triggers/reporters)");
					if (args.includeDefinition) {
						const details: Record<string, unknown> = {};
						if (p.steps?.length) details.steps = p.steps;
						if (p.triggers?.length) details.triggers = p.triggers;
						if (p.reporters?.length) details.reporters = p.reporters;
						const maxLines = args.definitionMaxLines ?? 120;
						const definitionText = JSON.stringify(details, null, 2);
						const defLines = definitionText.split("\n");
						parts.push(`\nDefinition:\n${defLines.slice(0, maxLines).join("\n")}`);
						if (defLines.length > maxLines) {
							parts.push(`... (${defLines.length - maxLines} more lines not shown)`);
						}
					} else {
						parts.push(
							"Tip: pass includeDefinition=true to include JSON definition.",
						);
					}
				}
				return { content: [{ type: "text", text: parts.join("\n") }] };
			}

			if (action === "update") {
				if (!args.data) {
					return {
						content: [
							{ type: "text", text: "Error: data is required for update." },
						],
						isError: true,
					};
				}
				const current = await get<Record<string, unknown>>(
					`/public/api/projects/${enc}/scenarios/${scEnc}/`,
				);
				const merged = deepMerge(current, args.data);
				await put<Record<string, unknown>>(
					`/public/api/projects/${enc}/scenarios/${scEnc}/`,
					merged,
				);
				return {
					content: [
						{ type: "text", text: `Scenario "${scenarioId}" updated.` },
					],
				};
			}

			// action === "delete"
			await del(`/public/api/projects/${enc}/scenarios/${scEnc}/`);
			return {
				content: [{ type: "text", text: `Scenario "${scenarioId}" deleted.` }],
			};
		},
	);
}
