import { createWriteStream } from "node:fs";
import { resolve } from "node:path";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createGzip } from "node:zlib";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { del, get, getProjectKey, post, put, stream } from "../client.js";
import { deepMerge } from "./deep-merge.js";
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

function asRecord(value: unknown): Record<string, unknown> | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  return value as Record<string, unknown>;
}

function csvEscape(field: string): string {
  if (
    field.includes(",") ||
    field.includes('"') ||
    field.includes("\n") ||
    field.includes("\r") ||
    field.includes("\t")
  ) {
    return `"${field.replace(/"/g, '""')}"`;
  }
  return field;
}

interface TsvStreamState {
  currentField: string;
  currentRow: string[];
  inQuotes: boolean;
  pendingQuoteInQuotes: boolean;
}

function createTsvStreamState(): TsvStreamState {
  return {
    currentField: "",
    currentRow: [],
    inQuotes: false,
    pendingQuoteInQuotes: false,
  };
}

function consumeTsvChunk(
  text: string,
  state: TsvStreamState,
  onRow: (row: string[]) => void,
): void {
  let i = 0;

  if (state.pendingQuoteInQuotes) {
    state.pendingQuoteInQuotes = false;
    const first = text[0];
    if (first === '"') {
      state.currentField += '"';
      i = 1;
    } else if (first === "\t" || first === "\n" || first === "\r") {
      state.inQuotes = false;
    } else if (first !== undefined) {
      // Ambiguous terminal quote from previous chunk; keep it as data.
      state.currentField += '"';
    }
  }

  for (; i < text.length; i++) {
    const ch = text[i];

    if (state.inQuotes) {
      if (ch === '"') {
        const next = text[i + 1];
        if (next === '"') {
          state.currentField += '"';
          i++;
          continue;
        }
        if (next === undefined) {
          state.pendingQuoteInQuotes = true;
          continue;
        }
        if (next === "\t" || next === "\n" || next === "\r") {
          state.inQuotes = false;
          continue;
        }
        // Quote in the middle of quoted field text — keep it literal.
        state.currentField += '"';
        continue;
      }
      state.currentField += ch;
      continue;
    }

    if (ch === '"' && state.currentField.length === 0) {
      state.inQuotes = true;
      continue;
    }
    if (ch === "\t") {
      state.currentRow.push(state.currentField);
      state.currentField = "";
      continue;
    }
    if (ch === "\n") {
      state.currentRow.push(state.currentField);
      state.currentField = "";
      const row = state.currentRow;
      state.currentRow = [];
      onRow(row);
      continue;
    }
    if (ch === "\r") {
      continue;
    }

    state.currentField += ch;
  }
}

function flushTsvStream(state: TsvStreamState, onRow: (row: string[]) => void): void {
  if (state.pendingQuoteInQuotes) {
    state.currentField += '"';
    state.pendingQuoteInQuotes = false;
  }
  if (state.currentField.length === 0 && state.currentRow.length === 0) return;
  state.currentRow.push(state.currentField);
  state.currentField = "";
  const row = state.currentRow;
  state.currentRow = [];
  onRow(row);
}

function rowToCsv(row: string[]): string {
  return row.map((field) => csvEscape(field)).join(",");
}

function isBlankRow(row: string[]): boolean {
  return row.length === 1 && row[0].length === 0;
}

function emitCsvLineWithLimit(
  row: string[],
  maxDataRows: number,
  emittedRows: { value: number },
  onLine: (line: string) => void,
): boolean {
  if (isBlankRow(row)) return false;

  const isHeader = emittedRows.value === 0;
  if (!isHeader && emittedRows.value - 1 >= maxDataRows) {
    return true;
  }

  onLine(rowToCsv(row));
  emittedRows.value += 1;

  if (!isHeader && emittedRows.value - 1 >= maxDataRows) {
    return true;
  }
  return false;
}

async function collectPreviewCsv(
  body: ReadableStream<Uint8Array>,
  maxDataRows: number,
): Promise<string> {
  const state = createTsvStreamState();
  const emittedRows = { value: 0 };
  const lines: string[] = [];
  let done = false;

  const nodeStream = Readable.fromWeb(body as import("stream/web").ReadableStream);
  for await (const chunk of nodeStream) {
    if (done) break;
    consumeTsvChunk(Buffer.from(chunk).toString("utf-8"), state, (row) => {
      if (done) return;
      done = emitCsvLineWithLimit(row, maxDataRows, emittedRows, (line) => {
        lines.push(line);
      });
    });
    if (done) {
      nodeStream.destroy();
      break;
    }
  }

  if (!done) {
    flushTsvStream(state, (row) => {
      if (done) return;
      done = emitCsvLineWithLimit(row, maxDataRows, emittedRows, (line) => {
        lines.push(line);
      });
    });
  }

  return lines.join("\n");
}

function tsvToCsvTransform(maxDataRows: number): Transform {
  const state = createTsvStreamState();
  const emittedRows = { value: 0 };
  let done = false;
  const maxRows = Math.max(1, maxDataRows);

  return new Transform({
    transform(chunk: Buffer, _encoding, callback) {
      if (done) {
        callback();
        return;
      }

      consumeTsvChunk(chunk.toString("utf-8"), state, (row) => {
        if (done) return;
        done = emitCsvLineWithLimit(row, maxRows, emittedRows, (line) => {
          this.push(`${line}\n`);
        });
      });

      if (done) {
        this.push(null);
      }
      callback();
    },
    flush(callback) {
      if (done) {
        callback();
        return;
      }

      flushTsvStream(state, (row) => {
        if (done) return;
        done = emitCsvLineWithLimit(row, maxRows, emittedRows, (line) => {
          this.push(`${line}\n`);
        });
      });
      callback();
    },
  });
}

export function tsvLineToCSV(line: string): string {
  const fields: string[] = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (inQuotes) {
      const next = line[i + 1];
      if (ch === '"' && next === '"') {
        field += '"';
        i++;
        continue;
      }
      if (ch === '"' && (next === "\t" || next === "\r" || next === undefined)) {
        inQuotes = false;
        continue;
      }
      field += ch;
      continue;
    }
    if (ch === '"' && field.length === 0) {
      inQuotes = true;
      continue;
    }
    if (ch === "\t") {
      fields.push(field);
      field = "";
      continue;
    }
    if (ch === "\r") {
      continue;
    }
    field += ch;
  }

  fields.push(field);
  return rowToCsv(fields);
}

const datasetInputSchema = z.discriminatedUnion("action", [
  z.object({
    action: z.literal("list"),
    projectKey: optionalProjectKey,
    limit: z.number().int().min(1).optional(),
    offset: z.number().int().min(0).optional(),
    query: z.string().optional(),
  }),
  z
    .object({
      action: z.literal("create"),
      projectKey: optionalProjectKey,
      datasetName: z.string(),
      connection: z.string(),
      type: z.string().optional(),
      table: z.string().optional(),
      dbSchema: z.string().optional(),
      catalog: z.string().optional(),
      formatType: z.string().optional(),
      formatParams: z.record(z.string(), z.unknown()).optional(),
      managed: z.boolean().optional(),
    })
    .passthrough(),
  z.object({
    action: z.literal("get"),
    projectKey: optionalProjectKey,
    datasetName: z.string(),
    includeDefinition: z.boolean().optional(),
  }),
  z.object({
    action: z.literal("schema"),
    projectKey: optionalProjectKey,
    datasetName: z.string(),
  }),
  z.object({
    action: z.literal("metadata"),
    projectKey: optionalProjectKey,
    datasetName: z.string(),
  }),
  z.object({
    action: z.literal("delete"),
    projectKey: optionalProjectKey,
    datasetName: z.string(),
  }),
  z.object({
    action: z.literal("preview"),
    projectKey: optionalProjectKey,
    datasetName: z.string(),
    limit: z.number().int().min(1).optional(),
  }),
  z.object({
    action: z.literal("download"),
    projectKey: optionalProjectKey,
    datasetName: z.string(),
    limit: z.number().int().min(1).optional(),
    outputDir: z.string().optional(),
  }),
  z.object({
    action: z.literal("update"),
    projectKey: optionalProjectKey,
    datasetName: z.string(),
    data: z.record(z.string(), z.unknown()),
  }),
]);

export function register(server: McpServer) {
  registerTool(
    server,
    "dataset",
    {
      description:
        "Dataset ops: list/get/schema/preview/metadata/download/create/update/delete. get is summary-first; set includeDefinition=true to include full JSON definition.",
      inputSchema: datasetInputSchema,
    },
    async (args: Record<string, unknown>) => {
      const {
        action,
        projectKey,
        datasetName,
        limit,
        offset,
        query,
        outputDir,
        data,
        includeDefinition,
      } = args as {
        action: string;
        projectKey?: string;
        datasetName?: string;
        limit?: number;
        offset?: number;
        query?: string;
        outputDir?: string;
        data?: Record<string, unknown>;
        includeDefinition?: boolean;
      };
      const raw = args as Record<string, unknown>;
      const pk = getProjectKey(projectKey);
      const enc = encodeURIComponent(pk);

      if (action === "list") {
        const datasets = await get<Array<{ name: string; type?: string; shortDesc?: string }>>(
          `/public/api/projects/${enc}/datasets/`,
        );
        const filtered = filterByQuery(datasets, query, (dataset) => [
          dataset.name,
          dataset.type,
          dataset.shortDesc,
        ]);
        const {
          items: page,
          offset: pageOffset,
          limit: pageLimit,
          hasMore,
        } = paginateItems(filtered, limit, offset);
        const text = formatBulletText(
          page.map(
            (dataset) =>
              `${dataset.name}${dataset.type ? ` (${dataset.type})` : ""}${dataset.shortDesc ? ` — ${dataset.shortDesc}` : ""}`,
          ),
          emptyListText("datasets"),
        );
        return {
          content: [{ type: "text", text }],
          structuredContent: {
            ok: true,
            total: datasets.length,
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
        const connection = asString(raw.connection);
        if (!datasetName || !connection) {
          return {
            content: [
              {
                type: "text",
                text: "Error: datasetName and connection are required for create.",
              },
            ],
            isError: true,
          };
        }

        // Auto-detect dataset type from existing datasets using the same connection
        const table = asString(raw.table);
        const dbSchema = asString(raw.dbSchema);
        const catalog = asString(raw.catalog);
        const formatType = asString(raw.formatType);
        const formatParams = asRecord(raw.formatParams);
        const managed = typeof raw.managed === "boolean" ? raw.managed : undefined;
        let dsType = asString(raw.type);
        if (!dsType) {
          const existing = await get<Array<{ type?: string; params?: { connection?: string } }>>(
            `/public/api/projects/${enc}/datasets/`,
          );
          const match = existing.find((d) => d.params?.connection === connection && d.type);
          dsType = match?.type ?? (table ? "Snowflake" : "Filesystem");
        }

        let body: Record<string, unknown>;

        if (table) {
          // Database dataset (Snowflake, PostgreSQL, etc.)
          const params: Record<string, unknown> = {
            connection,
            mode: "table",
            table,
          };
          if (dbSchema) params.schema = dbSchema;
          if (catalog) params.catalog = catalog;

          body = {
            projectKey: pk,
            name: datasetName,
            type: dsType,
            params,
            managed: managed ?? false,
          };
        } else {
          // Filesystem dataset
          body = {
            projectKey: pk,
            name: datasetName,
            type: dsType,
            params: {
              connection,
              path: `\${projectKey}/${datasetName}`,
            },
            formatType: formatType ?? "csv",
            formatParams: formatParams ?? {
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
            managed: managed ?? true,
          };
        }

        await post<Record<string, unknown>>(`/public/api/projects/${enc}/datasets/`, body);
        const confirmParts = [`Dataset "${datasetName}" created on connection "${connection}".`];
        if (table) confirmParts.push(`Table: ${table}`);
        return {
          content: [{ type: "text", text: confirmParts.join(" ") }],
          structuredContent: {
            ok: true,
            datasetName,
            connection,
            created: true,
            table: table ?? null,
          },
        };
      }

      if (!datasetName) {
        return {
          content: [
            {
              type: "text",
              text: "Error: datasetName is required for this action.",
            },
          ],
          isError: true,
        };
      }

      const dsEnc = encodeURIComponent(datasetName);

      if (action === "delete") {
        await del(`/public/api/projects/${enc}/datasets/${dsEnc}`);
        return {
          content: [{ type: "text", text: `Dataset "${datasetName}" deleted.` }],
          structuredContent: { ok: true, datasetName, deleted: true },
        };
      }

      if (action === "get") {
        const d = await get<{
          name: string;
          type: string;
          projectKey?: string;
          managed?: boolean;
          params?: {
            connection?: string;
            path?: string;
            table?: string;
            schema?: string;
            catalog?: string;
            folderSmartId?: string;
          };
          formatType?: string;
          formatParams?: {
            separator?: string;
            charset?: string;
            compress?: string;
          };
          schema?: { columns: Array<{ name: string; type: string }> };
          tags?: string[];
        }>(`/public/api/projects/${enc}/datasets/${dsEnc}`);

        const parts: string[] = [
          `Dataset: ${d.name}`,
          `Type: ${d.type} | Managed: ${d.managed ?? false}`,
        ];
        if (d.params?.connection) parts.push(`Connection: ${d.params.connection}`);
        if (d.params?.folderSmartId) parts.push(`Source folder: ${d.params.folderSmartId}`);
        if (d.params?.table) parts.push(`Table: ${d.params.table}`);
        if (d.params?.schema) parts.push(`DB Schema: ${d.params.schema}`);
        if (d.params?.path) parts.push(`Path: ${d.params.path}`);
        if (d.formatType) {
          const fp = d.formatParams ?? {};
          const details: string[] = [d.formatType];
          if (fp.separator) details.push(`sep="${fp.separator === "\t" ? "\\t" : fp.separator}"`);
          if (fp.charset) details.push(fp.charset);
          if (fp.compress) details.push(fp.compress);
          parts.push(`Format: ${details.join(", ")}`);
        }
        const cols = d.schema?.columns ?? [];
        const maxColumnsShown = 20;
        if (cols.length > 0) {
          const shownColumns = cols
            .slice(0, maxColumnsShown)
            .map((c) => `${c.name} (${c.type})`)
            .join(", ");
          const remainingColumns = cols.length - Math.min(cols.length, maxColumnsShown);
          parts.push(
            `Schema (${cols.length} cols): ${shownColumns}${remainingColumns > 0 ? `, +${remainingColumns} more` : ""}`,
          );
        }
        const tags = d.tags ?? [];
        if (tags.length > 0) {
          const maxTagsShown = 20;
          const shownTags = tags.slice(0, maxTagsShown).join(", ");
          const remainingTags = tags.length - Math.min(tags.length, maxTagsShown);
          parts.push(`Tags: ${shownTags}${remainingTags > 0 ? `, +${remainingTags} more` : ""}`);
        }

        const summary = {
          name: d.name,
          type: d.type,
          managed: d.managed ?? false,
          projectKey: d.projectKey ?? null,
          connection: d.params?.connection ?? null,
          folderSmartId: d.params?.folderSmartId ?? null,
          table: d.params?.table ?? null,
          dbSchema: d.params?.schema ?? null,
          catalog: d.params?.catalog ?? null,
          path: d.params?.path ?? null,
          formatType: d.formatType ?? null,
          separator: d.formatParams?.separator ?? null,
          charset: d.formatParams?.charset ?? null,
          compress: d.formatParams?.compress ?? null,
          schemaColumnCount: cols.length,
          tagCount: tags.length,
        };

        if (includeDefinition) {
          parts.push("Definition: included in structuredContent.definition.");
        } else {
          parts.push("Tip: pass includeDefinition=true to include full dataset definition.");
        }
        return {
          content: [{ type: "text", text: parts.join("\n") }],
          structuredContent: {
            ok: true,
            dataset: summary,
            ...(includeDefinition ? { definition: d } : {}),
          },
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
          `/public/api/projects/${enc}/datasets/${dsEnc}`,
        );
        const merged = deepMerge(current, data);
        await put<Record<string, unknown>>(`/public/api/projects/${enc}/datasets/${dsEnc}`, merged);
        return {
          content: [{ type: "text", text: `Dataset "${datasetName}" updated.` }],
          structuredContent: { ok: true, datasetName, updated: true },
        };
      }

      if (action === "schema") {
        const schema = await get<{
          columns: Array<{ name: string; type: string }>;
        }>(`/public/api/projects/${enc}/datasets/${dsEnc}/schema`);
        const text = schema.columns.map((c) => `• ${c.name}: ${c.type}`).join("\n");
        return {
          content: [{ type: "text", text: text || "No columns in schema." }],
          structuredContent: { ok: true, columns: schema.columns },
        };
      }

      if (action === "preview") {
        const previewLimit = Math.max(1, Math.min(limit ?? 20, 500));
        const res = await stream(
          `/public/api/projects/${enc}/datasets/${dsEnc}/data/?format=tsv-excel-header&limit=${previewLimit}`,
        );
        const csv = await collectPreviewCsv(res.body as ReadableStream<Uint8Array>, previewLimit);
        return {
          content: [{ type: "text", text: csv || "No data." }],
          structuredContent: { ok: true, datasetName, previewLimit, format: "csv" },
        };
      }

      if (action === "metadata") {
        const m = await get<{
          tags?: string[];
          customFields?: Record<string, unknown>;
          checklists?: {
            checklists?: Array<{
              title: string;
              items?: Array<{ done: boolean }>;
            }>;
          };
        }>(`/public/api/projects/${enc}/datasets/${dsEnc}/metadata`);
        const parts: string[] = [];
        parts.push(m.tags?.length ? `Tags: ${m.tags.join(", ")}` : "Tags: (none)");
        const cfKeys = Object.keys(m.customFields ?? {});
        parts.push(
          cfKeys.length > 0
            ? `Custom fields (${cfKeys.length}): ${cfKeys.sort((a, b) => a.localeCompare(b)).join(", ")}`
            : "Custom fields: (none)",
        );
        for (const cl of m.checklists?.checklists ?? []) {
          const done = cl.items?.filter((i) => i.done).length ?? 0;
          parts.push(`Checklist "${cl.title}": ${done}/${cl.items?.length ?? 0} done`);
        }
        return {
          content: [{ type: "text", text: parts.join("\n") }],
          structuredContent: { ok: true, metadata: m },
        };
      }

      // action === "download"
      const downloadLimit = Math.max(1, limit ?? 100_000);
      const res = await stream(
        `/public/api/projects/${enc}/datasets/${dsEnc}/data/?format=tsv-excel-header&limit=${downloadLimit}`,
      );

      const dir = outputDir ?? process.cwd();
      const safeDatasetName = sanitizeFileName(datasetName, "dataset");
      const filePath = resolve(dir, `${safeDatasetName}.csv.gz`);

      const nodeStream = Readable.fromWeb(res.body as import("stream/web").ReadableStream);
      const csvTransform = tsvToCsvTransform(downloadLimit);
      const gzip = createGzip();
      const fileOut = createWriteStream(filePath);

      await pipeline(nodeStream, csvTransform, gzip, fileOut);

      return {
        content: [
          {
            type: "text",
            text: `Dataset "${datasetName}" exported to ${filePath}`,
          },
        ],
        structuredContent: {
          ok: true,
          datasetName,
          filePath,
          exported: true,
          limit: downloadLimit,
        },
      };
    },
  );
}
