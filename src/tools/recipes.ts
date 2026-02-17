import { writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { del, get, getProjectKey, post, put } from "../client.js";
import { registerTool } from "./register-tool.js";
import { emptyListText, filterByQuery, formatBulletText, paginateItems } from "./list-format.js";

const optionalProjectKey = z.string().optional();

const WINDOWS_RESERVED_FILE_NAMES = /^(con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³])$/i;
function sanitizeFileName(name: string, fallback: string): string {
  const sanitized = name
    .replace(/[<>:"/\\|?*\u0000-\u001F]/g, "_")
    .replace(/[. ]+$/g, "")
    .trim();
  if (!sanitized) return fallback;
  const dotIndex = sanitized.indexOf(".");
  const baseName = dotIndex === -1 ? sanitized : sanitized.slice(0, dotIndex);
  const extension = dotIndex === -1 ? "" : sanitized.slice(dotIndex);
  if (WINDOWS_RESERVED_FILE_NAMES.test(baseName)) return `${baseName}_${extension}`;
  return sanitized;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function asStringArray(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) return undefined;
  const out = value.filter((v): v is string => typeof v === "string" && v.length > 0);
  return out.length > 0 ? out : undefined;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  return value as Record<string, unknown>;
}

function missingRecipeDefinitionError(recipeName: string) {
  return {
    content: [
      {
        type: "text",
        text: `Error: recipe "${recipeName}" was not found or returned an empty definition.`,
      },
    ],
    structuredContent: { ok: false, recipeName, reason: "missing_recipe_definition" },
    isError: true as const,
  };
}

const patchSchema = z.record(z.string(), z.unknown());

const recipeCreateSchema = z
  .object({
    action: z.literal("create"),
    projectKey: optionalProjectKey,
    type: z.string().min(1),
    name: z.string().optional(),
    inputDatasets: z.array(z.string().min(1)).min(1).optional(),
    outputDataset: z.string().optional(),
    inputs: patchSchema.optional(),
    outputs: patchSchema.optional(),
    payload: z.string().optional(),
    outputConnection: z.string().optional(),
  })
  .passthrough()
  .superRefine((value, ctx) => {
    const hasSimple =
      Array.isArray(value.inputDatasets) &&
      value.inputDatasets.length > 0 &&
      typeof value.outputDataset === "string" &&
      value.outputDataset.length > 0;

    const hasExplicit =
      typeof value.name === "string" &&
      value.name.length > 0 &&
      value.inputs !== undefined &&
      value.outputs !== undefined;

    if (hasSimple || hasExplicit) return;
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message:
        "type and (inputDatasets + outputDataset) or (name + inputs + outputs) are required for create.",
    });
  });

const recipeInputSchema = z.discriminatedUnion("action", [
  z.object({
    action: z.literal("list"),
    projectKey: optionalProjectKey,
    limit: z.number().int().min(1).optional(),
    offset: z.number().int().min(0).optional(),
    query: z.string().optional(),
  }),
  recipeCreateSchema,
  z.object({
    action: z.literal("get"),
    projectKey: optionalProjectKey,
    recipeName: z.string().min(1),
    includePayload: z.boolean().optional(),
    payloadMaxLines: z.number().int().optional(),
  }),
  z.object({
    action: z.literal("update"),
    projectKey: optionalProjectKey,
    recipeName: z.string().min(1),
    data: patchSchema,
  }),
  z.object({
    action: z.literal("delete"),
    projectKey: optionalProjectKey,
    recipeName: z.string().min(1),
  }),
  z.object({
    action: z.literal("download"),
    projectKey: optionalProjectKey,
    recipeName: z.string().min(1),
    outputPath: z.string().optional(),
  }),
]);

export function register(server: McpServer) {
  registerTool(
    server,
    "recipe",
    {
      description:
        "Recipe ops: list/get/create/update/delete/download. get is summary-first; set includePayload=true to include payload snippets.",
      inputSchema: recipeInputSchema,
    },
    async (args: Record<string, unknown>) => {
      const typedArgs = args as {
        action: string;
        projectKey?: string;
        limit?: number;
        offset?: number;
        query?: string;
        recipeName?: string;
        includePayload?: boolean;
        payloadMaxLines?: number;
        data?: Record<string, unknown>;
        outputPath?: string;
        [key: string]: unknown;
      };
      const {
        action,
        projectKey,
        limit,
        offset,
        query,
        recipeName,
        includePayload,
        payloadMaxLines,
        data,
        outputPath,
      } = typedArgs;
      const raw = typedArgs as Record<string, unknown>;
      const pk = getProjectKey(projectKey);
      const enc = encodeURIComponent(pk);

      if (action === "list") {
        const recipes = await get<Array<{ name: string; type?: string }>>(
          `/public/api/projects/${enc}/recipes/`,
        );
        const filtered = filterByQuery(recipes, query, (recipe) => [recipe.name, recipe.type]);
        const {
          items: page,
          offset: pageOffset,
          limit: pageLimit,
          hasMore,
        } = paginateItems(filtered, limit, offset);
        const text = formatBulletText(
          page.map((recipe) => `${recipe.name}${recipe.type ? ` (${recipe.type})` : ""}`),
          emptyListText("recipes"),
        );
        return {
          content: [{ type: "text", text }],
          structuredContent: {
            ok: true,
            total: recipes.length,
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
        const type = asString(raw.type);
        const payload = asString(raw.payload);

        // Build inputs/outputs from simple or advanced form
        const inputDatasets = asStringArray(raw.inputDatasets);
        const outputDataset = asString(raw.outputDataset);
        let inputs = asRecord(raw.inputs);
        let outputs = asRecord(raw.outputs);

        if (!inputs && inputDatasets) {
          inputs = {
            main: {
              items: inputDatasets.map((ref) => ({ ref, deps: [] })),
            },
          };
        }
        if (!outputs && outputDataset) {
          outputs = {
            main: {
              items: [{ ref: outputDataset, appendMode: false }],
            },
          };
        }

        // Auto-generate recipe name if not provided
        const name =
          asString(raw.name) ?? (type && outputDataset ? `${type}_${outputDataset}` : undefined);

        if (!type || !name || !inputs || !outputs) {
          return {
            content: [
              {
                type: "text",
                text: "Error: type and (inputDatasets + outputDataset) or (name + inputs + outputs) are required for create.",
              },
            ],
            isError: true,
          };
        }

        // Resolve output connection — explicit, or auto-detect from existing datasets
        const existingDs = await get<
          Array<{
            name: string;
            type?: string;
            params?: {
              connection?: string;
              schema?: string;
              catalog?: string;
            };
            managed?: boolean;
          }>
        >(`/public/api/projects/${enc}/datasets/`);

        let outputConnection = asString(raw.outputConnection);
        if (!outputConnection) {
          const managedDs = existingDs.find((d) => d.managed && d.params?.connection);
          if (managedDs?.params?.connection) {
            outputConnection = managedDs.params.connection;
          }
        }

        // Auto-create missing output datasets
        const createdDatasets: string[] = [];
        if (outputConnection) {
          const existingNames = new Set(existingDs.map((d) => d.name));
          const connectionSample = existingDs.find(
            (d) => d.params?.connection === outputConnection && d.type,
          );
          const inferredOutputType = connectionSample?.type ?? "Filesystem";

          const outputRoles = outputs as Record<string, { items?: Array<{ ref?: string }> }>;
          for (const role of Object.values(outputRoles)) {
            for (const item of role.items ?? []) {
              if (item.ref && !existingNames.has(item.ref)) {
                const datasetBody: Record<string, unknown> =
                  inferredOutputType === "Filesystem"
                    ? {
                        projectKey: pk,
                        name: item.ref,
                        type: inferredOutputType,
                        params: {
                          connection: outputConnection,
                          path: `\${projectKey}/${item.ref}`,
                        },
                        formatType: "csv",
                        formatParams: {
                          style: "excel",
                          charset: "utf8",
                          separator: "\t",
                          quoteChar: '"',
                          escapeChar: "\\",
                          dateSerializationFormat: "ISO",
                          arrayMapFormat: "json",
                          parseHeaderRow: true,
                          compress: "gz",
                        },
                        managed: true,
                      }
                    : {
                        projectKey: pk,
                        name: item.ref,
                        type: inferredOutputType,
                        params: {
                          connection: outputConnection,
                          mode: "table",
                          table: item.ref,
                          ...(connectionSample?.params?.schema
                            ? { schema: connectionSample.params.schema }
                            : {}),
                          ...(connectionSample?.params?.catalog
                            ? { catalog: connectionSample.params.catalog }
                            : {}),
                        },
                        managed: connectionSample?.managed ?? false,
                      };

                await post(`/public/api/projects/${enc}/datasets/`, datasetBody);
                existingNames.add(item.ref);
                createdDatasets.push(item.ref);
              }
            }
          }
        }

        const recipePrototype: Record<string, unknown> = {
          type,
          name,
          projectKey: pk,
          inputs,
          outputs,
        };
        const creationSettings: Record<string, unknown> = {};
        if (payload !== undefined) {
          creationSettings.script = payload;
        }
        await post<Record<string, unknown>>(`/public/api/projects/${enc}/recipes/`, {
          recipePrototype,
          creationSettings,
        });

        // For join recipes: configure join conditions after creation
        let joinConfigured = false;
        const joinOnValue = raw.joinOn;
        const joinCols =
          typeof joinOnValue === "string" ? [joinOnValue] : asStringArray(joinOnValue);
        const joinType = asString(raw.joinType) ?? "LEFT";
        if (type === "join" && joinCols?.length) {
          const rnEnc = encodeURIComponent(name);
          const full = await get<{
            recipe: Record<string, unknown>;
            payload: string;
          }>(`/public/api/projects/${enc}/recipes/${rnEnc}`);

          // DSS returns empty payload for fresh join recipes — construct from scratch
          const inputCount =
            inputDatasets?.length ??
            (inputs as Record<string, { items?: unknown[] }>)?.main?.items?.length ??
            2;

          const virtualInputs: Record<string, unknown>[] = [{ index: 0, preFilter: {} }];
          for (let i = 1; i < inputCount; i++) {
            virtualInputs.push({
              index: i,
              on: joinCols.map((col) => ({
                column: col,
                type: "string",
                related: col,
                relatedType: "string",
                maxMatches: 1,
              })),
              joinType,
              preFilter: {},
            });
          }

          const joinPayload = {
            virtualInputs,
            computedColumns: [],
            postFilter: {},
          };

          // Ensure inputs/outputs are set — DSS may not persist them from the POST for join recipes
          const updatedFull = {
            ...full,
            recipe: {
              ...(full.recipe as Record<string, unknown>),
              inputs,
              outputs,
            },
            payload: JSON.stringify(joinPayload),
          };

          await put(`/public/api/projects/${enc}/recipes/${rnEnc}`, updatedFull);
          joinConfigured = true;
        }

        const confirmParts = [`Recipe "${name}" created.`];
        if (createdDatasets.length > 0) {
          confirmParts.push(`Auto-created output datasets: ${createdDatasets.join(", ")}`);
        }
        if (joinConfigured) {
          confirmParts.push(`Configured ${joinType} join on: ${joinCols?.join(", ")}`);
        }
        return {
          content: [{ type: "text", text: confirmParts.join("\n") }],
          structuredContent: {
            ok: true,
            recipeName: name,
            type,
            createdDatasets,
            joinConfigured,
          },
        };
      }

      // All remaining actions require recipeName
      if (!recipeName) {
        return {
          content: [
            {
              type: "text",
              text: "Error: recipeName is required for this action.",
            },
          ],
          isError: true,
        };
      }
      const rnEnc = encodeURIComponent(recipeName);

      if (action === "get") {
        const full = await get<Record<string, unknown>>(
          `/public/api/projects/${enc}/recipes/${rnEnc}`,
        );
        const recipe = asRecord(full.recipe);
        const recipeLabel = asString(recipe?.name);
        const recipeType = asString(recipe?.type);
        if (!recipe || !recipeLabel || !recipeType) {
          return missingRecipeDefinitionError(recipeName);
        }

        const r = recipe as {
          name: string;
          type: string;
          projectKey?: string;
          inputs?: Record<string, { items?: Array<{ ref: string }> }>;
          outputs?: Record<string, { items?: Array<{ ref: string }> }>;
        };
        const parts: string[] = [`Recipe: ${recipeLabel} (${recipeType})`];
        // Format inputs
        for (const [role, val] of Object.entries(r.inputs ?? {})) {
          const refs = (val.items ?? []).map((i) => i.ref).join(", ");
          if (refs) parts.push(`Input ${role}: ${refs}`);
        }
        // Format outputs
        for (const [role, val] of Object.entries(r.outputs ?? {})) {
          const refs = (val.items ?? []).map((i) => i.ref).join(", ");
          if (refs) parts.push(`Output ${role}: ${refs}`);
        }

        const payloadText = asString(full.payload);
        if (payloadText) {
          const payloadLines = payloadText.split("\n");
          parts.push(
            `Payload: present (${payloadLines.length} lines, ${payloadText.length} chars)`,
          );

          if (includePayload) {
            const maxLines = payloadMaxLines ?? 120;
            const shown = payloadLines.slice(0, maxLines).join("\n");
            parts.push(`\nPayload Body:\n${shown}`);
            if (payloadLines.length > maxLines) {
              parts.push(`... (${payloadLines.length - maxLines} more lines not shown)`);
            }
          } else {
            parts.push("Tip: pass includePayload=true to include payload body.");
          }
        }
        return {
          content: [{ type: "text", text: parts.join("\n") }],
          structuredContent: { ok: true, recipe: full },
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
          `/public/api/projects/${enc}/recipes/${rnEnc}`,
        );
        const currentRecipe = asRecord(current.recipe);
        if (!currentRecipe) {
          return missingRecipeDefinitionError(recipeName);
        }
        // Deep merge: preserve nested recipe object fields
        const mergedRecipe = {
          ...currentRecipe,
          ...((data.recipe as object) ?? {}),
        };
        const merged = { ...current, ...data, recipe: mergedRecipe };
        await put<Record<string, unknown>>(`/public/api/projects/${enc}/recipes/${rnEnc}`, merged);
        return {
          content: [{ type: "text", text: `Recipe "${recipeName}" updated.` }],
          structuredContent: { ok: true, recipeName, updated: true },
        };
      }

      if (action === "delete") {
        await del(`/public/api/projects/${enc}/recipes/${rnEnc}`);
        return {
          content: [{ type: "text", text: `Recipe "${recipeName}" deleted.` }],
          structuredContent: { ok: true, recipeName, deleted: true },
        };
      }

      // action === "download"
      const recipe = await get<Record<string, unknown>>(
        `/public/api/projects/${enc}/recipes/${rnEnc}`,
      );
      const recipeDefinition = asRecord(recipe.recipe);
      if (!recipeDefinition) {
        return missingRecipeDefinitionError(recipeName);
      }
      const safeRecipeName = sanitizeFileName(recipeName, "recipe");
      const filePath = outputPath ?? resolve(process.cwd(), `${safeRecipeName}.json`);
      await writeFile(filePath, JSON.stringify(recipe, null, 2), "utf-8");
      return {
        content: [{ type: "text", text: `Recipe "${recipeName}" saved to ${filePath}` }],
        structuredContent: { ok: true, recipeName, filePath, saved: true },
      };
    },
  );
}
