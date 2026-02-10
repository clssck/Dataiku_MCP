import { createWriteStream } from "node:fs";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { del, get, getProjectKey, stream, upload } from "../client.js";

const optionalProjectKey = z.string().optional();

export function register(server: McpServer) {
	server.registerTool(
		"managed_folder",
		{
			description:
				"Managed folder ops: list/get/contents/download/upload/delete_file.",
			inputSchema: z.object({
				action: z
					.enum([
						"list",
						"get",
						"contents",
						"download",
						"upload",
						"delete_file",
					])
					,
				projectKey: optionalProjectKey,
				folderId: z
					.string()
					.optional()
					,
				path: z
					.string()
					.optional()
					,
				localPath: z
					.string()
					.optional()
					,
			}),
		},
		async ({ action, projectKey, folderId, path, localPath }) => {
			const pk = getProjectKey(projectKey);
			const enc = encodeURIComponent(pk);

			if (action === "list") {
				const folders = await get<
					Array<{ id: string; name?: string; type?: string }>
				>(`/public/api/projects/${enc}/managedfolders/`);
				const text = folders
					.map(
						(f) =>
							`• ${f.id}${f.name ? `: ${f.name}` : ""}${f.type ? ` (${f.type})` : ""}`,
					)
					.join("\n");
				return {
					content: [
						{ type: "text", text: text || "No managed folders found." },
					],
				};
			}

			if (!folderId) {
				return {
					content: [
						{
							type: "text",
							text: "Error: folderId is required for this action.",
						},
					],
					isError: true,
				};
			}
			const fEnc = encodeURIComponent(folderId);

			if (action === "get") {
				const f = await get<{
					id?: string;
					name?: string;
					type?: string;
					projectKey?: string;
					params?: { connection?: string; path?: string };
					tags?: string[];
				}>(`/public/api/projects/${enc}/managedfolders/${fEnc}`);
				const parts: string[] = [
					`Folder: ${f.id ?? folderId}${f.name ? ` — "${f.name}"` : ""}`,
					`Type: ${f.type ?? "unknown"}`,
				];
				if (f.params?.connection)
					parts.push(`Connection: ${f.params.connection}`);
				if (f.params?.path) parts.push(`Path: ${f.params.path}`);
				if (f.tags?.length) parts.push(`Tags: ${f.tags.join(", ")}`);
				return { content: [{ type: "text", text: parts.join("\n") }] };
			}

			if (action === "contents") {
				const c = await get<{
					items?: Array<{
						path: string;
						size?: number;
						lastModified?: number;
					}>;
				}>(`/public/api/projects/${enc}/managedfolders/${fEnc}/contents/`);

				const items = c.items ?? [];
				if (items.length === 0) {
					return { content: [{ type: "text", text: "Folder is empty." }] };
				}

				const formatSize = (bytes?: number) => {
					if (!bytes || bytes < 0) return "?";
					if (bytes < 1024) return `${bytes}B`;
					if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)}KB`;
					return `${(bytes / 1024 / 1024).toFixed(1)}MB`;
				};

				const lines = items.map((item) => {
					const date = item.lastModified
						? new Date(item.lastModified).toISOString().slice(0, 10)
						: "?";
					return `• ${item.path} (${formatSize(item.size)}, ${date})`;
				});
				lines.unshift(`${items.length} files:`);
				return { content: [{ type: "text", text: lines.join("\n") }] };
			}

			if (!path) {
				return {
					content: [
						{ type: "text", text: "Error: path is required for this action." },
					],
					isError: true,
				};
			}
			const pEnc = encodeURIComponent(path);

			if (action === "download") {
				const res = await stream(
					`/public/api/projects/${enc}/managedfolders/${fEnc}/contents/${pEnc}`,
				);

				const dest =
					localPath ??
					resolve(process.cwd(), path.split("/").pop() ?? "download");
				const nodeStream = Readable.fromWeb(
					res.body as import("stream/web").ReadableStream,
				);
				const fileOut = createWriteStream(dest);
				await pipeline(nodeStream, fileOut);

				return {
					content: [{ type: "text", text: `Downloaded "${path}" to ${dest}` }],
				};
			}

			if (action === "upload") {
				if (!localPath) {
					return {
						content: [
							{
								type: "text",
								text: "Error: localPath is required for upload.",
							},
						],
						isError: true,
					};
				}

				await upload(
					`/public/api/projects/${enc}/managedfolders/${fEnc}/contents/${pEnc}`,
					localPath,
				);

				return {
					content: [
						{
							type: "text",
							text: `Uploaded "${localPath}" to folder "${folderId}" at "${path}"`,
						},
					],
				};
			}

			// action === "delete_file"
			await del(
				`/public/api/projects/${enc}/managedfolders/${fEnc}/contents/${pEnc}`,
			);
			return {
				content: [
					{
						type: "text",
						text: `Deleted "${path}" from folder "${folderId}".`,
					},
				],
			};
		},
	);
}
