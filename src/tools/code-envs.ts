import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { get } from "../client.js";
import { actionInput, actionSchema, paginationFields } from "./action-schema.js";
import { registerTool } from "./register-tool.js";
import { emptyListText, filterByQuery, formatBulletText, paginateItems } from "./list-format.js";

function summarizeItems(label: string, items: string[], maxItems = 20): string {
  if (items.length === 0) return `${label}: (none)`;
  const shown = items.slice(0, maxItems).join(", ");
  const remaining = items.length - Math.min(items.length, maxItems);
  return `${label}: ${items.length} (${shown}${remaining > 0 ? `, +${remaining} more` : ""})`;
}

const codeEnvLangSchema = z.enum(["PYTHON", "R"]);

const codeEnvInputSchema = actionSchema([
  actionInput("list", {
    envLang: codeEnvLangSchema.optional(),
    ...paginationFields,
  }),
  actionInput("get", {
    envLang: codeEnvLangSchema,
    envName: z.string().min(1),
    full: z.boolean().optional(),
    maxPackages: z.number().int().min(1).optional(),
  }),
]);

export function register(server: McpServer) {
  registerTool(
    server,
    "code_env",
    {
      description:
        "Code env ops: list/get. get returns package summaries; set full=true for full package lists.",
      inputSchema: codeEnvInputSchema,
    },
    async ({ action, envLang, envName, full, limit, offset, query, maxPackages }) => {
      if (action === "list") {
        const envs = await get<
          Array<{
            envName: string;
            envLang: string;
            pythonInterpreter?: string;
            deploymentMode?: string;
          }>
        >("/public/api/admin/code-envs/");
        const filteredByLang = envLang ? envs.filter((env) => env.envLang === envLang) : envs;
        const filteredEnvs = filterByQuery(filteredByLang, query, (env) => [
          env.envName,
          env.envLang,
          env.pythonInterpreter,
          env.deploymentMode,
        ]);
        const {
          items: pagedEnvs,
          offset: pageOffset,
          limit: pageLimit,
          hasMore,
        } = paginateItems(filteredEnvs, limit, offset);
        const text = formatBulletText(
          pagedEnvs.map(
            (env) =>
              `${env.envName} (${env.envLang}${env.pythonInterpreter ? `, ${env.pythonInterpreter}` : ""}${env.deploymentMode ? `, ${env.deploymentMode}` : ""})`,
          ),
          emptyListText("code environments"),
        );
        return {
          content: [{ type: "text", text }],
          structuredContent: {
            ok: true,
            total: envs.length,
            filtered: filteredEnvs.length,
            offset: pageOffset,
            limit: pageLimit,
            hasMore,
            query: query ?? null,
            envLang: envLang ?? null,
            envs: pagedEnvs,
          },
        };
      }

      // action === "get"

      const langEnc = encodeURIComponent(envLang);
      const nameEnc = encodeURIComponent(envName);
      const env = await get<{
        envName: string;
        envLang: string;
        desc?: { pythonInterpreter?: string };
        actualPackageList?: string;
        specPackageList?: string;
      }>(`/public/api/admin/code-envs/${langEnc}/${nameEnc}/`);

      const requestedPackages = env.specPackageList
        ? env.specPackageList
            .trim()
            .split("\n")
            .map((line) => line.trim())
            .filter(Boolean)
            .sort((a, b) => a.localeCompare(b))
        : [];

      const installedPackageLines = env.actualPackageList
        ? env.actualPackageList
            .trim()
            .split("\n")
            .map((line) => line.trim())
            .filter(Boolean)
        : [];

      const installedPackageNames = installedPackageLines
        .map((line) => line.split("==")[0].trim())
        .filter(Boolean)
        .sort((a, b) => a.localeCompare(b));
      const packageLimit = full ? Number.POSITIVE_INFINITY : Math.max(1, maxPackages ?? 200);
      const requestedPackagesOut = requestedPackages.slice(0, packageLimit);
      const installedPackagesOut = full
        ? installedPackageLines
        : installedPackageNames.slice(0, packageLimit);
      const parts: string[] = [
        `${env.envName} (${env.envLang}${env.desc?.pythonInterpreter ? `, ${env.desc.pythonInterpreter}` : ""})`,
      ];

      if (requestedPackages.length > 0) {
        if (full) {
          parts.push(`\nRequested packages:\n${requestedPackages.join("\n")}`);
        } else {
          parts.push(`\n${summarizeItems("Requested packages", requestedPackages)}`);
        }
      }

      if (installedPackageLines.length > 0) {
        if (full) {
          parts.push(`\nInstalled packages:\n${installedPackageLines.join("\n")}`);
        } else {
          parts.push(`\n${summarizeItems("Installed packages", installedPackageNames)}`);
        }
      }
      return {
        content: [{ type: "text", text: parts.join("\n") }],
        structuredContent: {
          ok: true,
          env: {
            envName: env.envName,
            envLang: env.envLang,
            pythonInterpreter: env.desc?.pythonInterpreter ?? null,
            requestedPackageCount: requestedPackages.length,
            installedPackageCount: installedPackageLines.length,
            requestedPackages: requestedPackagesOut,
            installedPackages: installedPackagesOut,
            requestedPackagesTruncated: requestedPackagesOut.length < requestedPackages.length,
            installedPackagesTruncated: installedPackagesOut.length < installedPackageNames.length,
            packageLimit: Number.isFinite(packageLimit) ? packageLimit : null,
          },
        },
      };
    },
  );
}
