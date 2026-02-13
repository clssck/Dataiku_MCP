import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { get, getProjectKey } from "../client.js";
import { normalizeFlowGraph } from "./flow-map.js";
import { registerTool } from "./register-tool.js";

const optionalProjectKey = z.string().optional();

export function register(server: McpServer) {
   registerTool(
      server,
		"project",
		{
			description:
				"Project ops: list/get/metadata/flow/map. map returns normalized connectivity; includeRaw adds original graph payload.",
			inputSchema: z.object({
				action: z
					.enum(["list", "get", "metadata", "flow", "map"])
					,
				projectKey: optionalProjectKey,
				includeRaw: z
					.boolean()
					.optional()
					,
			}),
		},
		async ({ action, projectKey, includeRaw }) => {
			if (action === "list") {
				const projects = await get<
					Array<{ projectKey: string; name: string; shortDesc?: string }>
				>("/public/api/projects/");
				const text = projects
					.map(
						(p) =>
							`• ${p.projectKey}: ${p.name}${p.shortDesc ? ` — ${p.shortDesc}` : ""}`,
					)
					.join("\n");
				return {
					content: [{ type: "text", text: text || "No projects found." }],
				};
			}

			const pk = getProjectKey(projectKey);
			const enc = encodeURIComponent(pk);

			if (action === "get") {
				const p = await get<{
					projectKey: string;
					name: string;
					projectStatus?: string;
					ownerLogin?: string;
					shortDesc?: string;
					tags?: string[];
					versionTag?: { versionNumber: number; lastModifiedOn: number };
				}>(`/public/api/projects/${enc}/`);
				const parts: string[] = [
					`Project: ${p.projectKey} — "${p.name}"`,
					`Status: ${p.projectStatus ?? "unknown"} | Owner: ${p.ownerLogin ?? "unknown"}`,
				];
				if (p.shortDesc) parts.push(`Description: ${p.shortDesc}`);
				if (p.tags?.length) parts.push(`Tags: ${p.tags.join(", ")}`);
				if (p.versionTag) {
					parts.push(
						`Version: ${p.versionTag.versionNumber} (modified ${new Date(p.versionTag.lastModifiedOn).toISOString().slice(0, 10)})`,
					);
				}
				return { content: [{ type: "text", text: parts.join("\n") }] };
			}

			if (action === "metadata") {
				const m = await get<{
					label?: string;
					shortDesc?: string;
					description?: string;
					tags?: string[];
					customFields?: Record<string, unknown>;
					checklists?: {
						checklists?: Array<{
							title: string;
							items?: Array<{ done: boolean }>;
						}>;
					};
				}>(`/public/api/projects/${enc}/metadata`);
				const parts: string[] = [];
				if (m.label) parts.push(`Label: ${m.label}`);
				if (m.shortDesc) parts.push(`Description: ${m.shortDesc}`);
				if (m.tags?.length) parts.push(`Tags: ${m.tags.join(", ")}`);
				const cfKeys = Object.keys(m.customFields ?? {});
				parts.push(
					cfKeys.length > 0
						? `Custom fields (${cfKeys.length}): ${cfKeys.sort((a, b) => a.localeCompare(b)).join(", ")}`
						: "Custom fields: (none)",
				);
				for (const cl of m.checklists?.checklists ?? []) {
					const done = cl.items?.filter((i) => i.done).length ?? 0;
					const total = cl.items?.length ?? 0;
					parts.push(`Checklist "${cl.title}": ${done}/${total} done`);
				}
				return {
					content: [{ type: "text", text: parts.join("\n") || "No metadata." }],
				};
			}

			if (action === "map") {
				const rawGraph = await get<unknown>(`/public/api/projects/${enc}/flow/graph/`);
				const [foldersRes, datasetsRes, recipesRes] = await Promise.allSettled([
					get<Array<{ id?: string; name?: string }>>(
						`/public/api/projects/${enc}/managedfolders/`,
					),
					get<Array<{ name?: string }>>(`/public/api/projects/${enc}/datasets/`),
					get<Array<{ name?: string }>>(`/public/api/projects/${enc}/recipes/`),
				]);

				const folderNamesById: Record<string, string> = {};
				const allFolderIds: string[] = [];
				if (foldersRes.status === "fulfilled") {
					for (const f of foldersRes.value) {
						if (!f.id || f.id.length === 0) continue;
						allFolderIds.push(f.id);
						folderNamesById[f.id] = f.name ?? f.id;
					}
				}

				const allDatasetNames =
					datasetsRes.status === "fulfilled"
						? datasetsRes.value
								.map((d) => d.name)
								.filter((n): n is string => typeof n === "string" && n.length > 0)
						: [];

				const allRecipeNames =
					recipesRes.status === "fulfilled"
						? recipesRes.value
								.map((r) => r.name)
								.filter((n): n is string => typeof n === "string" && n.length > 0)
						: [];

				const normalized = normalizeFlowGraph(rawGraph, pk, {
					folderNamesById,
					allDatasetNames,
					allRecipeNames,
					allFolderIds,
				});
				const out = includeRaw ? { ...normalized, raw: rawGraph } : normalized;
				return {
					content: [{ type: "text", text: JSON.stringify(out, null, 2) }],
				};
			}

			// action === "flow"
			interface FlowNode {
				type: string;
				subType?: string;
				ref: string;
				predecessors: string[];
				successors: string[];
			}
			const graph = await get<{
				nodes: Record<string, FlowNode>;
				datasets: string[];
				recipes: string[];
				folders: string[];
			}>(`/public/api/projects/${enc}/flow/graph/`);

			const parts: string[] = [
				`Flow: ${graph.datasets.length} datasets, ${graph.recipes.length} recipes, ${graph.folders.length} folders`,
				`\nDatasets: ${graph.datasets.join(", ")}`,
			];
			if (graph.folders.length > 0) {
				parts.push(`Folders: ${graph.folders.join(", ")}`);
			}
			parts.push("\nPipeline:");
			for (const node of Object.values(graph.nodes)) {
				if (!node.type.includes("RECIPE")) continue;
				const inputs = node.predecessors.join(", ") || "(none)";
				const outputs = node.successors.join(", ") || "(none)";
				const typeLabel = node.subType ? ` (${node.subType})` : "";
				parts.push(`  ${inputs} → [${node.ref}${typeLabel}] → ${outputs}`);
			}
			return { content: [{ type: "text", text: parts.join("\n") }] };
		},
	);
}
