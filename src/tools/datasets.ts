import { createWriteStream } from "node:fs";
import { resolve } from "node:path";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createGzip } from "node:zlib";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { del, get, getProjectKey, post, put, stream } from "../client.js";
import { deepMerge } from "./deep-merge.js";

const optionalProjectKey = z.string().optional();

function asString(value: unknown): string | undefined {
	return typeof value === "string" && value.length > 0 ? value : undefined;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
	if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
	return value as Record<string, unknown>;
}

function tsvToCsvTransform(limit: number): Transform {
	let lineCount = 0;
	let remainder = "";

	return new Transform({
		transform(chunk: Buffer, _encoding, callback) {
			const data = remainder + chunk.toString("utf-8");
			const lines = data.split("\n");
			remainder = lines.pop() ?? "";

			for (const line of lines) {
				if (lineCount > limit) break;
				if (line === "") continue;
				this.push(`${tsvLineToCSV(line)}\n`);
				lineCount++;
			}

			if (lineCount > limit) {
				this.push(null);
			}
			callback();
		},
		flush(callback) {
			if (remainder && lineCount <= limit) {
				this.push(`${tsvLineToCSV(remainder)}\n`);
			}
			callback();
		},
	});
}

export function tsvLineToCSV(line: string): string {
	return line
		.split("\t")
		.map((field) => {
			if (field.includes(",") || field.includes('"') || field.includes("\n")) {
				return `"${field.replace(/"/g, '""')}"`;
			}
			return field;
		})
		.join(",");
}

export function register(server: McpServer) {
	server.registerTool(
		"dataset",
		{
			description:
				"Dataset ops: list/get/schema/preview/metadata/download/create/update/delete. Create requires datasetName+connection; update merges partial data.",
			inputSchema: z.object({
				action: z
					.enum([
						"list",
						"get",
						"schema",
						"preview",
						"metadata",
						"download",
						"create",
						"update",
						"delete",
					])
					,
				projectKey: optionalProjectKey,
				datasetName: z
					.string()
					.optional()
					,
				limit: z
					.number()
					.int()
					.optional()
					,
				outputDir: z
					.string()
					.optional()
					,
				connection: z
					.string()
					.optional()
					,
				data: z
					.record(z.unknown())
					.optional()
					,
			}).passthrough(),
		},
		async (args) => {
			const { action, projectKey, datasetName, limit, outputDir } = args;
			const raw = args as Record<string, unknown>;
			const pk = getProjectKey(projectKey);
			const enc = encodeURIComponent(pk);

			if (action === "list") {
				const datasets = await get<
					Array<{ name: string; type?: string; shortDesc?: string }>
				>(`/public/api/projects/${enc}/datasets/`);
				const text = datasets
					.map(
						(d) =>
							`• ${d.name}${d.type ? ` (${d.type})` : ""}${d.shortDesc ? ` — ${d.shortDesc}` : ""}`,
					)
					.join("\n");
				return {
					content: [{ type: "text", text: text || "No datasets found." }],
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
				const managed =
					typeof raw.managed === "boolean" ? raw.managed : undefined;
				let dsType = asString(raw.type);
				if (!dsType) {
					const existing = await get<
						Array<{ type?: string; params?: { connection?: string } }>
					>(`/public/api/projects/${enc}/datasets/`);
					const match = existing.find(
						(d) => d.params?.connection === connection && d.type,
					);
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

				await post<Record<string, unknown>>(
					`/public/api/projects/${enc}/datasets/`,
					body,
				);
				const confirmParts = [
					`Dataset "${datasetName}" created on connection "${connection}".`,
				];
				if (table) confirmParts.push(`Table: ${table}`);
				return { content: [{ type: "text", text: confirmParts.join(" ") }] };
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
					content: [
						{ type: "text", text: `Dataset "${datasetName}" deleted.` },
					],
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
				if (d.params?.connection)
					parts.push(`Connection: ${d.params.connection}`);
				if (d.params?.folderSmartId)
					parts.push(`Source folder: ${d.params.folderSmartId}`);
				if (d.params?.table) parts.push(`Table: ${d.params.table}`);
				if (d.params?.schema) parts.push(`DB Schema: ${d.params.schema}`);
				if (d.params?.path) parts.push(`Path: ${d.params.path}`);
				if (d.formatType) {
					const fp = d.formatParams ?? {};
					const details: string[] = [d.formatType];
					if (fp.separator)
						details.push(
							`sep="${fp.separator === "\t" ? "\\t" : fp.separator}"`,
						);
					if (fp.charset) details.push(fp.charset);
					if (fp.compress) details.push(fp.compress);
					parts.push(`Format: ${details.join(", ")}`);
				}
				const cols = d.schema?.columns ?? [];
				if (cols.length > 0) {
					parts.push(
						`Schema (${cols.length} cols): ${cols.map((c) => `${c.name} (${c.type})`).join(", ")}`,
					);
				}
				if (d.tags?.length) parts.push(`Tags: ${d.tags.join(", ")}`);
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
					`/public/api/projects/${enc}/datasets/${dsEnc}`,
				);
				const merged = deepMerge(current, args.data);
				await put<Record<string, unknown>>(
					`/public/api/projects/${enc}/datasets/${dsEnc}`,
					merged,
				);
				return {
					content: [
						{ type: "text", text: `Dataset "${datasetName}" updated.` },
					],
				};
			}

			if (action === "schema") {
				const schema = await get<{
					columns: Array<{ name: string; type: string }>;
				}>(`/public/api/projects/${enc}/datasets/${dsEnc}/schema`);
				const text = schema.columns
					.map((c) => `• ${c.name}: ${c.type}`)
					.join("\n");
				return {
					content: [{ type: "text", text: text || "No columns in schema." }],
				};
			}

			if (action === "preview") {
				const previewLimit = Math.min(limit ?? 20, 500);
				const res = await stream(
					`/public/api/projects/${enc}/datasets/${dsEnc}/data/?format=tsv-excel-header`,
				);

				const nodeStream = Readable.fromWeb(
					res.body as import("stream/web").ReadableStream,
				);
				const chunks: Buffer[] = [];
				const maxLines = previewLimit + 1; // header + N rows
				let lineCount = 0;

				for await (const chunk of nodeStream) {
					chunks.push(Buffer.from(chunk));
					const text = Buffer.concat(chunks).toString("utf-8");
					lineCount = text.split("\n").filter((l) => l).length;
					if (lineCount >= maxLines) {
						nodeStream.destroy();
						break;
					}
				}

				const lines = Buffer.concat(chunks)
					.toString("utf-8")
					.split("\n")
					.filter((l) => l)
					.slice(0, maxLines);
				const csv = lines.map((l) => tsvLineToCSV(l)).join("\n");
				return {
					content: [{ type: "text", text: csv || "No data." }],
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
				parts.push(
					m.tags?.length ? `Tags: ${m.tags.join(", ")}` : "Tags: (none)",
				);
				const cfKeys = Object.keys(m.customFields ?? {});
				parts.push(
					cfKeys.length > 0
						? `Custom fields (${cfKeys.length}): ${cfKeys.sort((a, b) => a.localeCompare(b)).join(", ")}`
						: "Custom fields: (none)",
				);
				for (const cl of m.checklists?.checklists ?? []) {
					const done = cl.items?.filter((i) => i.done).length ?? 0;
					parts.push(
						`Checklist "${cl.title}": ${done}/${cl.items?.length ?? 0} done`,
					);
				}
				return { content: [{ type: "text", text: parts.join("\n") }] };
			}

			// action === "download"
			const downloadLimit = limit ?? 100_000;
			const res = await stream(
				`/public/api/projects/${enc}/datasets/${dsEnc}/data/?format=tsv-excel-header`,
			);

			const dir = outputDir ?? process.cwd();
			const filePath = resolve(dir, `${datasetName}.csv.gz`);

			const nodeStream = Readable.fromWeb(
				res.body as import("stream/web").ReadableStream,
			);
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
			};
		},
	);
}
