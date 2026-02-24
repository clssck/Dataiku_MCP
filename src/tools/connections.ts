import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { get, getProjectKey } from "../client.js";
import { actionInput, actionSchema, optionalProjectKey } from "./action-schema.js";
import { registerTool } from "./register-tool.js";

const connectionInferModeSchema = z.enum(["fast", "rich"]);
const connectionInputSchema = actionSchema([
  actionInput("infer", {
    projectKey: optionalProjectKey,
    mode: connectionInferModeSchema.optional(),
  }),
]);

interface RichConnectionSummary {
  name: string;
  types: string[];
  managed: boolean;
  dbSchemas: string[];
}

function normalizeConnectionNames(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter((item): item is string => typeof item === "string" && item.length > 0)
    .sort((a, b) => a.localeCompare(b));
}

async function listConnectionNames(): Promise<string[]> {
  const raw = await get<unknown>("/public/api/connections/get-names/");
  return normalizeConnectionNames(raw);
}

async function inferRichConnectionsFromDatasets(
  projectEnc: string,
): Promise<RichConnectionSummary[]> {
  const datasets = await get<
    Array<{
      name: string;
      type?: string;
      params?: { connection?: string; schema?: string; catalog?: string };
      managed?: boolean;
    }>
  >(`/public/api/projects/${projectEnc}/datasets/`);

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

  return [...connectionMap.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([name, info]) => ({
      name,
      types: [...info.types].sort((a, b) => a.localeCompare(b)),
      managed: info.managed,
      dbSchemas: [...info.dbSchemas].sort((a, b) => a.localeCompare(b)),
    }));
}

function formatRichConnectionsText(connections: RichConnectionSummary[]): string {
  return connections
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
}

export function register(server: McpServer) {
  registerTool(
    server,
    "connection",
    {
      description:
        "Connection discovery (action: infer). Default mode=fast uses DSS connection names; mode=rich scans project datasets for inferred type/schema/managed details.",
      inputSchema: connectionInputSchema,
    },
    async ({ projectKey, mode }) => {
      const pk = getProjectKey(projectKey);
      const enc = encodeURIComponent(pk);
      const inferMode = mode ?? "fast";

      if (inferMode === "fast") {
        try {
          const connectionNames = await listConnectionNames();
          if (connectionNames.length === 0) {
            return {
              content: [
                {
                  type: "text",
                  text: "No connections found — DSS reported no available connection names.",
                },
              ],
              structuredContent: {
                ok: true,
                mode: "fast",
                projectKey: pk,
                connectionCount: 0,
                connections: [],
              },
            };
          }

          return {
            content: [
              {
                type: "text",
                text: [
                  "Connections available on DSS:",
                  ...connectionNames.map((name) => `• ${name}`),
                  "",
                  'Use connection(action: "infer", mode: "rich") for dataset-derived type/schema details.',
                ].join("\n"),
              },
            ],
            structuredContent: {
              ok: true,
              mode: "fast",
              projectKey: pk,
              connectionCount: connectionNames.length,
              connections: connectionNames.map((name) => ({ name })),
            },
          };
        } catch (error) {
          const detail = error instanceof Error ? error.message : String(error);
          const richConnections = await inferRichConnectionsFromDatasets(enc);

          if (richConnections.length === 0) {
            return {
              content: [
                {
                  type: "text",
                  text: "No connections found — fast lookup failed and fallback dataset scan found no connection metadata.",
                },
              ],
              structuredContent: {
                ok: true,
                mode: "fast",
                projectKey: pk,
                connectionCount: 0,
                connections: [],
                fallback: "datasetScan",
                warning: `Fast connection lookup failed: ${detail}`,
              },
            };
          }

          const richText = formatRichConnectionsText(richConnections);
          return {
            content: [
              {
                type: "text",
                text: [
                  "Connections found in project datasets:",
                  richText,
                  "",
                  "Note: fast lookup failed and fell back to dataset scan.",
                ].join("\n"),
              },
            ],
            structuredContent: {
              ok: true,
              mode: "fast",
              projectKey: pk,
              connectionCount: richConnections.length,
              connections: richConnections,
              fallback: "datasetScan",
              warning: `Fast connection lookup failed: ${detail}`,
            },
          };
        }
      }

      const richConnections = await inferRichConnectionsFromDatasets(enc);
      if (richConnections.length === 0) {
        return {
          content: [
            {
              type: "text",
              text: "No connections found — project has no datasets with connection info.",
            },
          ],
          structuredContent: {
            ok: true,
            mode: "rich",
            projectKey: pk,
            connectionCount: 0,
            connections: [],
          },
        };
      }

      const richText = formatRichConnectionsText(richConnections);
      return {
        content: [
          {
            type: "text",
            text: `Connections found in project datasets:\n${richText}\n\nUse these connection names with dataset(create) to add new datasets.`,
          },
        ],
        structuredContent: {
          ok: true,
          mode: "rich",
          projectKey: pk,
          connectionCount: richConnections.length,
          connections: richConnections,
        },
      };
    },
  );
}
