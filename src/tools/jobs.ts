import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { get, getProjectKey, getText, post } from "../client.js";
import { registerTool } from "./register-tool.js";

const optionalProjectKey = z.string().optional();

export function register(server: McpServer) {
   registerTool(
      server,
		"job",
		{
			description:
				"Job ops: list/get/log/build/abort. build computes a dataset target.",
			inputSchema: z.object({
				action: z
					.enum(["list", "get", "log", "build", "abort"])
					,
				projectKey: optionalProjectKey,
				jobId: z
					.string()
					.optional()
					,
				datasetName: z
					.string()
					.optional()
					,
				buildMode: z
					.enum([
						"RECURSIVE_BUILD",
						"NON_RECURSIVE_FORCED_BUILD",
						"RECURSIVE_FORCED_BUILD",
						"RECURSIVE_MISSING_ONLY_BUILD",
					])
					.optional()
					,
				activity: z
					.string()
					.optional()
					,
				autoUpdateSchema: z
					.boolean()
					.optional()
					,
				maxLogLines: z
					.number()
					.int()
					.min(1)
					.optional()
					,
			}),
		},
		async ({
			action,
			projectKey,
			jobId,
			datasetName,
			activity,
			buildMode,
			autoUpdateSchema,
			maxLogLines,
		}) => {
			const pk = getProjectKey(projectKey);
			const enc = encodeURIComponent(pk);

			if (action === "list") {
				const jobs = await get<
					Array<{
						def: { id: string; name?: string; initiator?: string };
						state?: string;
						startTime?: number;
					}>
				>(`/public/api/projects/${enc}/jobs/`);
				const text = jobs
					.map((j) => {
						const started = j.startTime
							? new Date(j.startTime).toISOString()
							: "unknown";
						return `• ${j.def.id} [${j.state ?? "unknown"}] started ${started}${j.def.initiator ? ` by ${j.def.initiator}` : ""}`;
					})
					.join("\n");
				return {
					content: [{ type: "text", text: text || "No jobs found." }],
				};
			}

			if (action === "build") {
				if (!datasetName) {
					return {
						content: [
							{
								type: "text",
								text: "Error: datasetName is required for build.",
							},
						],
						isError: true,
					};
				}
				const jobDef: Record<string, unknown> = {
					outputs: [{ projectKey: pk, id: datasetName, type: "DATASET" }],
					type: buildMode ?? "RECURSIVE_BUILD",
				};
				if (autoUpdateSchema) {
					jobDef.autoUpdateSchemaBeforeEachRecipeRun = true;
				}
				const job = await post<{ id: string }>(
					`/public/api/projects/${enc}/jobs/`,
					jobDef,
				);
				return {
					content: [
						{
							type: "text",
							text: `Job started: ${job.id}\nUse job(action: "get") to track progress.`,
						},
					],
				};
			}

			// get and abort require jobId
			if (!jobId) {
				return {
					content: [
						{ type: "text", text: "Error: jobId is required for this action." },
					],
					isError: true,
				};
			}
			const jobEnc = encodeURIComponent(jobId);

			// Trailing slash required — DSS Cloud proxy misroutes URLs ending in .NNN (job ID timestamps)
			if (action === "get") {
				const j = await get<{
					baseStatus?: {
						activities?: Record<
							string,
							{
								recipeName?: string;
								activityId?: string;
								recipeType?: string;
								state?: string;
								totalTime?: number;
								preparingTime?: number;
								waitingTime?: number;
								runningTime?: number;
							}
						>;
						def?: {
							id?: string;
							type?: string;
							initiator?: string;
							outputs?: Array<{ targetDataset?: string }>;
						};
						state?: string;
						jobStartTime?: number;
						jobEndTime?: number;
					};
					globalState?: {
						done?: number;
						failed?: number;
						running?: number;
						total?: number;
						aborted?: number;
					};
				}>(`/public/api/projects/${enc}/jobs/${jobEnc}/`);

				const bs = j.baseStatus ?? {};
				const def = bs.def ?? {};
				const gs = j.globalState ?? {};

				const parts: string[] = [
					`Job: ${def.id ?? jobId}`,
					`State: ${bs.state ?? "unknown"} | Type: ${def.type ?? "unknown"}`,
				];
				if (def.initiator) parts.push(`Initiator: ${def.initiator}`);
				const targets = (def.outputs ?? [])
					.map((o) => o.targetDataset)
					.filter(Boolean);
				if (targets.length > 0) parts.push(`Target: ${targets.join(", ")}`);

				const start = bs.jobStartTime;
				const end = bs.jobEndTime;
				if (start) {
					const dur = end ? `${((end - start) / 1000).toFixed(0)}s` : "running";
					parts.push(
						`Started: ${new Date(start).toISOString()} | Duration: ${dur}`,
					);
				}

				if (gs.total) {
					parts.push(
						`Progress: ${gs.done ?? 0}/${gs.total} done, ${gs.failed ?? 0} failed, ${gs.running ?? 0} running`,
					);
				}

				const activities = bs.activities ?? {};
				if (Object.keys(activities).length > 0) {
					parts.push("\nActivities:");
					for (const act of Object.values(activities)) {
						const total = act.totalTime
							? `${(act.totalTime / 1000).toFixed(1)}s`
							: "";
						const details: string[] = [];
						if (act.preparingTime && act.preparingTime > 0)
							details.push(`prep ${(act.preparingTime / 1000).toFixed(1)}s`);
						if (act.waitingTime && act.waitingTime > 0)
							details.push(`wait ${(act.waitingTime / 1000).toFixed(1)}s`);
						if (act.runningTime && act.runningTime > 0)
							details.push(`run ${(act.runningTime / 1000).toFixed(1)}s`);
						const detailStr =
							details.length > 0 ? ` (${details.join(", ")})` : "";
						parts.push(
							`  • ${act.recipeName ?? act.activityId} [${act.state ?? "?"}] ${total}${detailStr}`,
						);
					}
				}

				return { content: [{ type: "text", text: parts.join("\n") }] };
			}

			if (action === "log") {
				const query = activity
					? `?activity=${encodeURIComponent(activity)}`
					: "";
				const log = await getText(
					`/public/api/projects/${enc}/jobs/${jobEnc}/log/${query}`,
				);

				if (!log) {
					return {
						content: [{ type: "text", text: "No log output available." }],
					};
				}

				const lines = log.split("\n");
				const limit = maxLogLines ?? 50;
				if (lines.length > limit) {
					return {
						content: [
							{
								type: "text",
								text: `(showing last ${limit} of ${lines.length} lines — use maxLogLines to see more)\n${lines.slice(-limit).join("\n")}`,
							},
						],
					};
				}
				return { content: [{ type: "text", text: log }] };
			}

			// action === "abort"
			await post(`/public/api/projects/${enc}/jobs/${jobEnc}/abort/`);
			return {
				content: [{ type: "text", text: `Job ${jobId} abort requested.` }],
			};
		},
	);
}
