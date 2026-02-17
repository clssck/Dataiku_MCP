import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { del, get, getProjectKey, post, put } from "../client.js";
import { deepMerge } from "./deep-merge.js";
import { registerTool } from "./register-tool.js";
import { emptyListText, filterByQuery, formatBulletText, paginateItems } from "./list-format.js";

const optionalProjectKey = z.string().optional();

const scenarioPatchSchema = z.record(z.string(), z.unknown());

const scenarioInputSchema = z.discriminatedUnion("action", [
  z.object({
    action: z.literal("list"),
    projectKey: optionalProjectKey,
    limit: z.number().int().min(1).optional(),
    offset: z.number().int().min(0).optional(),
    query: z.string().optional(),
  }),
  z.object({
    action: z.literal("create"),
    projectKey: optionalProjectKey,
    scenarioId: z.string().min(1),
    name: z.string().min(1),
    scenarioType: z.enum(["step_based", "custom_python"]).optional(),
    data: scenarioPatchSchema.optional(),
  }),
  z.object({
    action: z.literal("run"),
    projectKey: optionalProjectKey,
    scenarioId: z.string().min(1),
  }),
  z.object({
    action: z.literal("status"),
    projectKey: optionalProjectKey,
    scenarioId: z.string().min(1),
  }),
  z.object({
    action: z.literal("get"),
    projectKey: optionalProjectKey,
    scenarioId: z.string().min(1),
    includeScript: z.boolean().optional(),
    includeDefinition: z.boolean().optional(),
    definitionMaxLines: z.number().int().min(1).max(2000).optional(),
  }),
  z.object({
    action: z.literal("update"),
    projectKey: optionalProjectKey,
    scenarioId: z.string().min(1),
    data: scenarioPatchSchema,
  }),
  z.object({
    action: z.literal("delete"),
    projectKey: optionalProjectKey,
    scenarioId: z.string().min(1),
  }),
]);

export function register(server: McpServer) {
  registerTool(
    server,
    "scenario",
    {
      description:
        "Scenario ops: list/run/status/get/create/update/delete. get is summary-first; use includeScript/includeDefinition for details.",
      inputSchema: scenarioInputSchema,
    },
    async (args: Record<string, unknown>) => {
      const typedArgs = args as {
        action: string;
        projectKey?: string;
        scenarioId?: string;
        limit?: number;
        offset?: number;
        query?: string;
        name?: string;
        scenarioType?: "step_based" | "custom_python";
        data?: Record<string, unknown>;
        includeScript?: boolean;
        includeDefinition?: boolean;
        definitionMaxLines?: number;
      };
      const {
        action,
        projectKey,
        scenarioId,
        limit,
        offset,
        query,
        name,
        scenarioType,
        data,
        includeScript,
        includeDefinition,
        definitionMaxLines,
      } = typedArgs;
      const pk = getProjectKey(projectKey);
      const enc = encodeURIComponent(pk);

      if (action === "list") {
        const scenarios = await get<Array<{ id: string; name?: string; active?: boolean }>>(
          `/public/api/projects/${enc}/scenarios/`,
        );
        const filtered = filterByQuery(scenarios, query, (scenario) => [
          scenario.id,
          scenario.name,
          scenario.active === undefined ? "" : scenario.active ? "active" : "inactive",
        ]);
        const {
          items: page,
          offset: pageOffset,
          limit: pageLimit,
          hasMore,
        } = paginateItems(filtered, limit, offset);
        const text = formatBulletText(
          page.map(
            (scenario) =>
              `${scenario.id}${scenario.name ? `: ${scenario.name}` : ""}${scenario.active !== undefined ? ` [${scenario.active ? "active" : "inactive"}]` : ""}`,
          ),
          emptyListText("scenarios"),
        );
        return {
          content: [{ type: "text", text }],
          structuredContent: {
            ok: true,
            total: scenarios.length,
            filtered: filtered.length,
            offset: pageOffset,
            limit: pageLimit,
            query: query ?? null,
            items: page,
            hasMore,
          },
        };
      }

      if (action === "create") {
        if (!scenarioId || !name) {
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

        const scenarioTypeLabel = scenarioType ?? "step_based";
        const body = {
          id: scenarioId,
          name,
          projectKey: pk,
          type: scenarioTypeLabel,
          params:
            scenarioTypeLabel === "step_based" ? { steps: [], triggers: [], reporters: [] } : {},
          ...(data ?? {}),
        };

        await post<Record<string, unknown>>(`/public/api/projects/${enc}/scenarios/`, body);
        return {
          content: [
            {
              type: "text",
              text: `Scenario "${name}" (${scenarioId}) created. Type: ${scenarioTypeLabel}.`,
            },
          ],
          structuredContent: {
            ok: true,
            scenarioId,
            name,
            scenarioType: scenarioTypeLabel,
            created: true,
          },
        };
      }

      if (!scenarioId && action !== "create") {
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
      const scEnc = encodeURIComponent(scenarioId as string);

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
          structuredContent: { ok: true, scenarioId, runId, triggered: true },
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
          const dur = lr.start && lr.end ? `${((lr.end - lr.start) / 1000).toFixed(0)}s` : "?";
          parts.push(
            `Last run: ${lr.outcome ?? "unknown"} (${dur})${lr.trigger?.type ? ` trigger=${lr.trigger.type}` : ""}`,
          );
        }
        return {
          content: [{ type: "text", text: parts.join("\n") }],
          structuredContent: { ok: true, status: s },
        };
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
          const maxLines = definitionMaxLines ?? 120;
          const scriptLines = script.split("\n");
          parts.push(`Script: present (${scriptLines.length} lines, ${script.length} chars)`);
          if (includeScript) {
            parts.push(`\nScript:\n${scriptLines.slice(0, maxLines).join("\n")}`);
            if (scriptLines.length > maxLines) {
              parts.push(`... (${scriptLines.length - maxLines} more lines not shown)`);
            }
          } else {
            parts.push("Tip: pass includeScript=true to include script body.");
          }
        }
        // Include step/trigger/reporter details as compact JSON for editing
        if (p.steps?.length || p.triggers?.length || p.reporters?.length) {
          parts.push("Definition: present (steps/triggers/reporters)");
          if (includeDefinition) {
            const details: Record<string, unknown> = {};
            if (p.steps?.length) details.steps = p.steps;
            if (p.triggers?.length) details.triggers = p.triggers;
            if (p.reporters?.length) details.reporters = p.reporters;
            const maxLines = definitionMaxLines ?? 120;
            const definitionText = JSON.stringify(details, null, 2);
            const defLines = definitionText.split("\n");
            parts.push(`\nDefinition:\n${defLines.slice(0, maxLines).join("\n")}`);
            if (defLines.length > maxLines) {
              parts.push(`... (${defLines.length - maxLines} more lines not shown)`);
            }
          } else {
            parts.push("Tip: pass includeDefinition=true to include JSON definition.");
          }
        }
        return {
          content: [{ type: "text", text: parts.join("\n") }],
          structuredContent: { ok: true, scenario: sc },
        };
      }

      if (action === "update") {
        if (!data) {
          return {
            content: [{ type: "text", text: "Error: data is required for update." }],
            isError: true,
          };
        }
        const current = await get<Record<string, unknown>>(
          `/public/api/projects/${enc}/scenarios/${scEnc}/`,
        );
        const merged = deepMerge(current, data);
        await put<Record<string, unknown>>(
          `/public/api/projects/${enc}/scenarios/${scEnc}/`,
          merged,
        );
        return {
          content: [{ type: "text", text: `Scenario "${scenarioId}" updated.` }],
          structuredContent: { ok: true, scenarioId, updated: true },
        };
      }

      // action === "delete"
      await del(`/public/api/projects/${enc}/scenarios/${scEnc}/`);
      return {
        content: [{ type: "text", text: `Scenario "${scenarioId}" deleted.` }],
        structuredContent: { ok: true, scenarioId, deleted: true },
      };
    },
  );
}
