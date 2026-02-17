import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { get, getProjectKey } from "../client.js";
import {
  actionInput,
  actionSchema,
  optionalProjectKey,
  paginationFields,
} from "./action-schema.js";
import {
  normalizeFlowGraph,
  type NormalizedFlowEdge,
  type NormalizedFlowMap,
  type NormalizedFlowNode,
} from "./flow-map.js";
import { registerTool } from "./register-tool.js";
import { emptyListText, filterByQuery, formatBulletText, paginateItems } from "./list-format.js";

const projectInputSchema = actionSchema([
  actionInput("list", {
    ...paginationFields,
  }),
  actionInput("get", {
    projectKey: optionalProjectKey,
  }),
  actionInput("metadata", {
    projectKey: optionalProjectKey,
  }),
  actionInput("flow", {
    projectKey: optionalProjectKey,
  }),
  actionInput("map", {
    projectKey: optionalProjectKey,
    includeRaw: z.boolean().optional(),
    maxNodes: z.number().int().min(1).optional(),
    maxEdges: z.number().int().min(1).optional(),
  }),
]);

const DEFAULT_MAP_MAX_NODES = 300;
const DEFAULT_MAP_MAX_EDGES = 600;

interface FlowMapTruncationSummary {
  truncated: boolean;
  maxNodes: number | null;
  maxEdges: number | null;
  nodeCountBefore: number;
  nodeCountAfter: number;
  edgeCountBefore: number;
  edgeCountAfter: number;
}

function computeRootsAndLeaves(
  nodes: NormalizedFlowNode[],
  edges: NormalizedFlowEdge[],
): { roots: string[]; leaves: string[] } {
  const inDegree = new Map<string, number>();
  const outDegree = new Map<string, number>();

  for (const node of nodes) {
    inDegree.set(node.id, 0);
    outDegree.set(node.id, 0);
  }

  for (const edge of edges) {
    inDegree.set(edge.to, (inDegree.get(edge.to) ?? 0) + 1);
    outDegree.set(edge.from, (outDegree.get(edge.from) ?? 0) + 1);
  }

  const roots = nodes
    .filter((node) => (inDegree.get(node.id) ?? 0) === 0)
    .map((node) => node.id)
    .sort((a, b) => a.localeCompare(b));

  const leaves = nodes
    .filter((node) => (outDegree.get(node.id) ?? 0) === 0)
    .map((node) => node.id)
    .sort((a, b) => a.localeCompare(b));

  return { roots, leaves };
}

function truncateFlowMap(
  normalized: NormalizedFlowMap,
  maxNodes: number | undefined,
  maxEdges: number | undefined,
): { map: NormalizedFlowMap; truncation: FlowMapTruncationSummary } {
  const nodes = maxNodes === undefined ? normalized.nodes : normalized.nodes.slice(0, maxNodes);
  const nodeIds = new Set(nodes.map((node) => node.id));
  const edgesWithinNodes = normalized.edges.filter(
    (edge) => nodeIds.has(edge.from) && nodeIds.has(edge.to),
  );
  const edges = maxEdges === undefined ? edgesWithinNodes : edgesWithinNodes.slice(0, maxEdges);

  const { roots, leaves } = computeRootsAndLeaves(nodes, edges);
  const truncation = {
    truncated: nodes.length < normalized.nodes.length || edges.length < normalized.edges.length,
    maxNodes: maxNodes ?? null,
    maxEdges: maxEdges ?? null,
    nodeCountBefore: normalized.nodes.length,
    nodeCountAfter: nodes.length,
    edgeCountBefore: normalized.edges.length,
    edgeCountAfter: edges.length,
  };

  const warnings = truncation.truncated
    ? [
        ...normalized.warnings,
        `Flow map truncated (nodes ${truncation.nodeCountAfter}/${truncation.nodeCountBefore}, edges ${truncation.edgeCountAfter}/${truncation.edgeCountBefore}).`,
      ]
    : normalized.warnings;

  return {
    map: {
      ...normalized,
      nodes,
      edges,
      roots,
      leaves,
      warnings,
      stats: {
        nodeCount: nodes.length,
        edgeCount: edges.length,
        datasets: nodes.filter((node) => node.kind === "dataset").length,
        recipes: nodes.filter((node) => node.kind === "recipe").length,
        roots: roots.length,
        leaves: leaves.length,
      },
    },
    truncation,
  };
}

export function register(server: McpServer) {
  registerTool(
    server,
    "project",
    {
      description:
        "Project ops: list/get/metadata/flow/map. map returns normalized connectivity in structuredContent.map; includeRaw adds original graph payload.",
      inputSchema: projectInputSchema,
    },
    async ({ action, projectKey, includeRaw, maxNodes, maxEdges, limit, offset, query }) => {
      if (action === "list") {
        const projects =
          await get<Array<{ projectKey: string; name: string; shortDesc?: string }>>(
            "/public/api/projects/",
          );
        const filtered = filterByQuery(projects, query, (project) => [
          project.projectKey,
          project.name,
          project.shortDesc,
        ]);
        const {
          items: page,
          offset: pageOffset,
          limit: pageLimit,
          hasMore,
        } = paginateItems(filtered, limit, offset);
        const text = formatBulletText(
          page.map(
            (project) =>
              `${project.projectKey}: ${project.name}${project.shortDesc ? ` — ${project.shortDesc}` : ""}`,
          ),
          emptyListText("projects"),
        );
        return {
          content: [{ type: "text", text }],
          structuredContent: {
            ok: true,
            total: projects.length,
            filtered: filtered.length,
            offset: pageOffset,
            limit: pageLimit,
            query: query ?? null,
            items: page,
            hasMore,
          },
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
        return {
          content: [{ type: "text", text: parts.join("\n") }],
          structuredContent: { ok: true, project: p },
        };
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
          structuredContent: {
            ok: true,
            metadata: m,
            customFieldCount: cfKeys.length,
          },
        };
      }

      if (action === "map") {
        const rawGraph = await get<unknown>(`/public/api/projects/${enc}/flow/graph/`);
        const [foldersRes, datasetsRes, recipesRes] = await Promise.allSettled([
          get<Array<{ id?: string; name?: string }>>(`/public/api/projects/${enc}/managedfolders/`),
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
        const effectiveMaxNodes = maxNodes ?? DEFAULT_MAP_MAX_NODES;
        const effectiveMaxEdges = maxEdges ?? DEFAULT_MAP_MAX_EDGES;
        const { map, truncation } = truncateFlowMap(
          normalized,
          effectiveMaxNodes,
          effectiveMaxEdges,
        );
        const out = includeRaw ? { ...map, raw: rawGraph } : map;
        const summaryLines = [
          `Flow map for ${pk}`,
          `Nodes: ${out.stats.nodeCount} (datasets=${out.stats.datasets}, recipes=${out.stats.recipes})`,
          `Edges: ${out.stats.edgeCount}`,
          `Roots: ${out.stats.roots} | Leaves: ${out.stats.leaves}`,
          `Warnings: ${out.warnings.length}`,
          truncation.truncated
            ? `Truncated: yes (nodes ${truncation.nodeCountAfter}/${truncation.nodeCountBefore}, edges ${truncation.edgeCountAfter}/${truncation.edgeCountBefore})`
            : "Truncated: no",
          includeRaw
            ? "Raw flow payload included in structuredContent.map.raw."
            : "Raw flow payload omitted (set includeRaw=true to include).",
        ];

        summaryLines.push(
          `Applied limits: maxNodes=${truncation.maxNodes ?? "unbounded"}, maxEdges=${truncation.maxEdges ?? "unbounded"}`,
        );
        if (out.roots.length > 0) {
          summaryLines.push(`Sample roots: ${out.roots.slice(0, 5).join(", ")}`);
        }
        if (out.leaves.length > 0) {
          summaryLines.push(`Sample leaves: ${out.leaves.slice(0, 5).join(", ")}`);
        }
        return {
          content: [{ type: "text", text: summaryLines.join("\n") }],
          structuredContent: {
            ok: true,
            map: out,
            truncation,
          },
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
      return {
        content: [{ type: "text", text: parts.join("\n") }],
        structuredContent: { ok: true, flow: graph },
      };
    },
  );
}
