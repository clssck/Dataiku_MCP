import { createWriteStream } from "node:fs";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { del, get, getProjectKey, stream, upload } from "../client.js";
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

function normalizeRemotePath(path: string): string {
  return path.replace(/\\/g, "/");
}

function inferDownloadFileName(remotePath: string): string {
  const leaf = normalizeRemotePath(remotePath).split("/").filter(Boolean).pop();
  return sanitizeFileName(leaf ?? "", "download");
}

const managedFolderInputSchema = z.discriminatedUnion("action", [
  z.object({
    action: z.literal("list"),
    projectKey: optionalProjectKey,
    limit: z.number().int().min(1).optional(),
    offset: z.number().int().min(0).optional(),
    query: z.string().optional(),
  }),
  z.object({
    action: z.literal("get"),
    projectKey: optionalProjectKey,
    folderId: z.string().min(1),
  }),
  z.object({
    action: z.literal("contents"),
    projectKey: optionalProjectKey,
    folderId: z.string().min(1),
    limit: z.number().int().min(1).optional(),
    offset: z.number().int().min(0).optional(),
    query: z.string().optional(),
  }),
  z.object({
    action: z.literal("download"),
    projectKey: optionalProjectKey,
    folderId: z.string().min(1),
    path: z.string().min(1),
    localPath: z.string().optional(),
  }),
  z.object({
    action: z.literal("upload"),
    projectKey: optionalProjectKey,
    folderId: z.string().min(1),
    path: z.string().min(1),
    localPath: z.string().min(1),
  }),
  z.object({
    action: z.literal("delete_file"),
    projectKey: optionalProjectKey,
    folderId: z.string().min(1),
    path: z.string().min(1),
  }),
]);

export function register(server: McpServer) {
  registerTool(
    server,
    "managed_folder",
    {
      description: "Managed folder ops: list/get/contents/download/upload/delete_file.",
      inputSchema: managedFolderInputSchema,
    },
    async ({ action, projectKey, folderId, path, localPath, limit, offset, query }) => {
      const pk = getProjectKey(projectKey);
      const enc = encodeURIComponent(pk);

      if (action === "list") {
        const folders = await get<Array<{ id: string; name?: string; type?: string }>>(
          `/public/api/projects/${enc}/managedfolders/`,
        );
        const filtered = filterByQuery(folders, query, (folder) => [
          folder.id,
          folder.name,
          folder.type,
        ]);
        const {
          items: page,
          offset: pageOffset,
          limit: pageLimit,
          hasMore,
        } = paginateItems(filtered, limit, offset);
        const text = formatBulletText(
          page.map(
            (folder) =>
              `${folder.id}${folder.name ? `: ${folder.name}` : ""}${folder.type ? ` (${folder.type})` : ""}`,
          ),
          emptyListText("managed folders"),
        );
        return {
          content: [{ type: "text", text }],
          structuredContent: {
            ok: true,
            total: folders.length,
            filtered: filtered.length,
            offset: pageOffset,
            limit: pageLimit,
            query: query ?? null,
            items: page,
            hasMore,
          },
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
        if (f.params?.connection) parts.push(`Connection: ${f.params.connection}`);
        if (f.params?.path) parts.push(`Path: ${f.params.path}`);
        if (f.tags?.length) parts.push(`Tags: ${f.tags.join(", ")}`);
        return {
          content: [{ type: "text", text: parts.join("\n") }],
          structuredContent: { ok: true, folder: f },
        };
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
          return {
            content: [{ type: "text", text: "Folder is empty." }],
            structuredContent: {
              ok: true,
              total: 0,
              filtered: 0,
              offset: 0,
              limit: Math.max(1, limit ?? 200),
              hasMore: false,
              query: query ?? null,
              itemCount: 0,
              items: [],
            },
          };
        }

        const filteredItems = filterByQuery(items, query, (item) => [item.path]);
        const {
          items: pageItems,
          offset: pageOffset,
          limit: pageLimit,
          hasMore,
        } = paginateItems(filteredItems, limit, offset, 200);
        const formatSize = (bytes?: number) => {
          if (!bytes || bytes < 0) return "?";
          if (bytes < 1024) return `${bytes}B`;
          if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)}KB`;
          return `${(bytes / 1024 / 1024).toFixed(1)}MB`;
        };

        const lines = pageItems.map((item) => {
          const date = item.lastModified
            ? new Date(item.lastModified).toISOString().slice(0, 10)
            : "?";
          return `• ${item.path} (${formatSize(item.size)}, ${date})`;
        });
        const header =
          hasMore || pageOffset > 0 || filteredItems.length !== items.length
            ? `Showing ${pageItems.length} of ${filteredItems.length} files${
                filteredItems.length !== items.length ? ` (filtered from ${items.length})` : ""
              }:`
            : `${filteredItems.length} files:`;
        lines.unshift(header);
        return {
          content: [{ type: "text", text: lines.join("\n") }],
          structuredContent: {
            ok: true,
            total: items.length,
            filtered: filteredItems.length,
            offset: pageOffset,
            limit: pageLimit,
            hasMore,
            query: query ?? null,
            itemCount: filteredItems.length,
            items: pageItems,
          },
        };
      }

      if (!path) {
        return {
          content: [{ type: "text", text: "Error: path is required for this action." }],
          isError: true,
        };
      }
      const normalizedPath = normalizeRemotePath(path);
      const pEnc = encodeURIComponent(normalizedPath);

      if (action === "download") {
        const res = await stream(
          `/public/api/projects/${enc}/managedfolders/${fEnc}/contents/${pEnc}`,
        );

        const dest = localPath ?? resolve(process.cwd(), inferDownloadFileName(normalizedPath));
        const nodeStream = Readable.fromWeb(res.body as import("stream/web").ReadableStream);
        const fileOut = createWriteStream(dest);
        await pipeline(nodeStream, fileOut);

        return {
          content: [{ type: "text", text: `Downloaded "${normalizedPath}" to ${dest}` }],
          structuredContent: {
            ok: true,
            folderId,
            path: normalizedPath,
            localPath: dest,
            downloaded: true,
          },
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
          structuredContent: {
            ok: true,
            folderId,
            path: normalizedPath,
            localPath,
            uploaded: true,
          },
        };
      }

      // action === "delete_file"
      await del(`/public/api/projects/${enc}/managedfolders/${fEnc}/contents/${pEnc}`);
      return {
        content: [
          {
            type: "text",
            text: `Deleted "${path}" from folder "${folderId}".`,
          },
        ],
        structuredContent: { ok: true, folderId, path: normalizedPath, deleted: true },
      };
    },
  );
}
