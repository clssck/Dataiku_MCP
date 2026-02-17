import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { get, getProjectKey } from "../client.js";
import { actionInput, actionSchema, optionalProjectKey } from "./action-schema.js";
import { registerTool } from "./register-tool.js";

const connectionInputSchema = actionSchema([
  actionInput("infer", {
    projectKey: optionalProjectKey,
  }),
]);

export function register(server: McpServer) {
  registerTool(
    server,
    "connection",
    {
      description: "Connection discovery from project datasets (action: infer).",
      inputSchema: connectionInputSchema,
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
          existing.managed = existing.managed || (ds.managed ?? false);
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
          structuredContent: {
            ok: true,
            projectKey: pk,
            connectionCount: 0,
            connections: [],
          },
        };
      }

      const connections = [...connectionMap.entries()]
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([name, info]) => ({
          name,
          types: [...info.types].sort((a, b) => a.localeCompare(b)),
          managed: info.managed,
          dbSchemas: [...info.dbSchemas].sort((a, b) => a.localeCompare(b)),
        }));

      const text = connections
        .map((connection) => {
          const parts: string[] = [];
          if (connection.types.length > 0) parts.push(connection.types.join("/"));
          if (connection.managed) parts.push("managed");
          if (connection.dbSchemas.length > 0) {
            parts.push(`schemas: ${connection.dbSchemas.join(", ")}`);
          }
          return `• ${connection.name}${parts.length > 0 ? ` (${parts.join(", ")})` : ""}`;
        })
        .join("\n");
      return {
        content: [
          {
            type: "text",
            text: `Connections found in project datasets:\n${text}\n\nUse these connection names with dataset(create) to add new datasets.`,
          },
        ],
        structuredContent: {
          ok: true,
          projectKey: pk,
          connectionCount: connections.length,
          connections,
        },
      };
    },
  );
}
