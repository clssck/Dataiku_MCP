import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { get, getProjectKey } from "../client.js";

const optionalProjectKey = z.string().optional();

export function register(server: McpServer) {
	server.registerTool(
		"connection",
		{
			description:
				"Connection discovery from project datasets (action: infer).",
			inputSchema: z.object({
				action: z.enum(["infer"]),
				projectKey: optionalProjectKey,
			}),
		},
		async ({ projectKey }) => {
			const pk = getProjectKey(projectKey);
			const enc = encodeURIComponent(pk);
			const datasets = await get<
				Array<{
					name: string;
					type?: string;
					params?: { connection?: string; schema?: string; catalog?: string };
					managed?: boolean;
				}>
			>(`/public/api/projects/${enc}/datasets/`);

			const connectionMap = new Map<
				string,
				{ types: Set<string>; managed: boolean; dbSchemas: Set<string> }
			>();

			for (const ds of datasets) {
				const conn = ds.params?.connection;
				if (!conn) continue;
				const existing = connectionMap.get(conn);
				if (existing) {
					if (ds.type) existing.types.add(ds.type);
					if (ds.params?.schema) existing.dbSchemas.add(ds.params.schema);
				} else {
					connectionMap.set(conn, {
						types: new Set(ds.type ? [ds.type] : []),
						managed: ds.managed ?? false,
						dbSchemas: new Set(ds.params?.schema ? [ds.params.schema] : []),
					});
				}
			}

			if (connectionMap.size === 0) {
				return {
					content: [
						{
							type: "text",
							text: "No connections found — project has no datasets with connection info.",
						},
					],
				};
			}

			const text = [...connectionMap.entries()]
				.map(([name, info]) => {
					const parts: string[] = [];
					const types = [...info.types].join("/");
					if (types) parts.push(types);
					if (info.managed) parts.push("managed");
					const schemas = [...info.dbSchemas];
					if (schemas.length > 0) parts.push(`schemas: ${schemas.join(", ")}`);
					return `• ${name}${parts.length > 0 ? ` (${parts.join(", ")})` : ""}`;
				})
				.join("\n");

			return {
				content: [
					{
						type: "text",
						text: `Connections found in project datasets:\n${text}\n\nUse these connection names with dataset(create) to add new datasets.`,
					},
				],
			};
		},
	);
}
