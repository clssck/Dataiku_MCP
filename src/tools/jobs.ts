import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { get, getProjectKey, getText, post } from "../client.js";
import { registerTool } from "./register-tool.js";
import { emptyListText, filterByQuery, formatBulletText, paginateItems } from "./list-format.js";

const optionalProjectKey = z.string().optional();

const buildModeSchema = z.enum([
  "RECURSIVE_BUILD",
  "NON_RECURSIVE_FORCED_BUILD",
  "RECURSIVE_FORCED_BUILD",
  "RECURSIVE_MISSING_ONLY_BUILD",
]);

const jobInputSchema = z.discriminatedUnion("action", [
  z.object({
    action: z.literal("list"),
    projectKey: optionalProjectKey,
    limit: z.number().int().min(1).optional(),
    offset: z.number().int().min(0).optional(),
    query: z.string().optional(),
  }),
  z.object({
    action: z.literal("build"),
    projectKey: optionalProjectKey,
    datasetName: z.string(),
    buildMode: buildModeSchema.optional(),
    autoUpdateSchema: z.boolean().optional(),
  }),
  z.object({
    action: z.literal("buildAndWait"),
    projectKey: optionalProjectKey,
    datasetName: z.string(),
    buildMode: buildModeSchema.optional(),
    autoUpdateSchema: z.boolean().optional(),
    activity: z.string().optional(),
    includeLogs: z.boolean().optional(),
    maxLogLines: z.number().int().min(1).optional(),
    pollIntervalMs: z.number().int().min(1).optional(),
    timeoutMs: z.number().int().min(1).optional(),
  }),
  z.object({
    action: z.literal("get"),
    projectKey: optionalProjectKey,
    jobId: z.string(),
    includeDefinition: z.boolean().optional(),
  }),
  z.object({
    action: z.literal("wait"),
    projectKey: optionalProjectKey,
    jobId: z.string(),
    activity: z.string().optional(),
    includeLogs: z.boolean().optional(),
    maxLogLines: z.number().int().min(1).optional(),
    pollIntervalMs: z.number().int().min(1).optional(),
    timeoutMs: z.number().int().min(1).optional(),
  }),
  z.object({
    action: z.literal("log"),
    projectKey: optionalProjectKey,
    jobId: z.string(),
    activity: z.string().optional(),
    maxLogLines: z.number().int().min(1).optional(),
  }),
  z.object({
    action: z.literal("abort"),
    projectKey: optionalProjectKey,
    jobId: z.string(),
  }),
]);

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isTerminalState(state: string | undefined): boolean {
  const normalized = (state ?? "").toUpperCase();
  return ["DONE", "FAILED", "ABORTED", "KILLED", "CANCELED", "CANCELLED", "ERROR"].includes(
    normalized,
  );
}

type NormalizedJobWaitState = "terminalSuccess" | "terminalFailure" | "timeout" | "nonTerminal";

function isSuccessfulTerminalState(state: string | undefined): boolean {
  return (state ?? "").toUpperCase() === "DONE";
}

function normalizeJobWaitState(
  state: string | undefined,
  timedOut = false,
): NormalizedJobWaitState {
  if (timedOut) return "timeout";
  if (!isTerminalState(state)) return "nonTerminal";
  return isSuccessfulTerminalState(state) ? "terminalSuccess" : "terminalFailure";
}

interface WaitJobOptions {
  projectEnc: string;
  jobId: string;
  activity?: string;
  includeLogs?: boolean;
  maxLogLines?: number;
  pollIntervalMs?: number;
  timeoutMs?: number;
}

async function waitForJob({
  projectEnc,
  jobId,
  activity,
  includeLogs,
  maxLogLines,
  pollIntervalMs,
  timeoutMs,
}: WaitJobOptions): Promise<{
  content: Array<{ type: "text"; text: string }>;
  structuredContent: Record<string, unknown>;
  isError?: boolean;
}> {
  const intervalMs = Math.max(1, pollIntervalMs ?? 2000);
  const timeout = Math.max(intervalMs, timeoutMs ?? 120_000);
  const startedAt = Date.now();
  let pollCount = 0;
  const jobEnc = encodeURIComponent(jobId);

  while (true) {
    pollCount += 1;
    const j = await get<{
      baseStatus?: {
        def?: {
          id?: string;
          type?: string;
        };
        state?: string;
      };
      globalState?: {
        done?: number;
        failed?: number;
        running?: number;
        total?: number;
      };
    }>(`/public/api/projects/${projectEnc}/jobs/${jobEnc}/`);

    const bs = j.baseStatus ?? {};
    const def = bs.def ?? {};
    const gs = j.globalState ?? {};
    const state = bs.state ?? "unknown";
    const elapsedMs = Date.now() - startedAt;
    const normalizedState = normalizeJobWaitState(state);

    if (normalizedState === "terminalSuccess" || normalizedState === "terminalFailure") {
      const terminalSuccess = normalizedState === "terminalSuccess";
      const parts: string[] = [
        `Job: ${def.id ?? jobId}`,
        `State: ${state} | Type: ${def.type ?? "unknown"}`,
        `Elapsed: ${(elapsedMs / 1000).toFixed(1)}s | Polls: ${pollCount}`,
      ];
      if (gs.total) {
        parts.push(
          `Progress: ${gs.done ?? 0}/${gs.total} done, ${gs.failed ?? 0} failed, ${gs.running ?? 0} running`,
        );
      }

      if (!terminalSuccess) {
        parts.push(
          "Result: terminal failure state. Treat as failed run and inspect logs/job details.",
        );
      }
      if (includeLogs) {
        const logQuery = activity ? `?activity=${encodeURIComponent(activity)}` : "";
        const log = await getText(
          `/public/api/projects/${projectEnc}/jobs/${jobEnc}/log/${logQuery}`,
        );
        if (log) {
          const lines = log.split("\n");
          const limit = maxLogLines ?? 50;
          const tail = lines.length > limit ? lines.slice(-limit).join("\n") : log;
          parts.push(
            `\nLatest log tail (${Math.min(limit, lines.length)}/${lines.length} lines):\n${tail}`,
          );
        } else {
          parts.push("\nLatest log tail: (none)");
        }
      }
      return {
        content: [{ type: "text", text: parts.join("\n") }],
        structuredContent: {
          ok: terminalSuccess,
          jobId: def.id ?? jobId,
          state,
          rawState: state,
          normalizedState,
          type: def.type ?? "unknown",
          elapsedMs,
          pollCount,
          progress: gs,
          terminalSuccess,
        },
        ...(terminalSuccess ? {} : { isError: true }),
      };
    }

    if (elapsedMs >= timeout) {
      return {
        content: [
          {
            type: "text",
            text: `Timed out waiting for job ${jobId}. Last state: ${state} after ${(elapsedMs / 1000).toFixed(1)}s and ${pollCount} polls.`,
          },
        ],
        structuredContent: {
          ok: false,
          jobId,
          state,
          rawState: state,
          normalizedState: normalizeJobWaitState(state, true),
          elapsedMs,
          pollCount,
          timeoutMs: timeout,
        },
        isError: true,
      };
    }

    await sleep(Math.min(intervalMs, timeout - elapsedMs));
  }
}

export function register(server: McpServer) {
  registerTool(
    server,
    "job",
    {
      description:
        "Job ops: list/get/log/build/buildAndWait/wait/abort. get is summary-first; set includeDefinition=true to include full JSON definition.",
      inputSchema: jobInputSchema,
    },
    async ({
      action,
      projectKey,
      jobId,
      datasetName,
      activity,
      buildMode,
      autoUpdateSchema,
      maxLogLines,
      includeLogs,
      pollIntervalMs,
      timeoutMs,
      limit,
      offset,
      query,
      includeDefinition,
    }) => {
      const pk = getProjectKey(projectKey);
      const enc = encodeURIComponent(pk);

      if (action === "list") {
        const jobs = await get<
          Array<{
            def: { id: string; name?: string; initiator?: string };
            state?: string;
            startTime?: number;
          }>
        >(`/public/api/projects/${enc}/jobs/`);
        const filtered = filterByQuery(jobs, query, (job) => [
          job.def.id,
          job.def.name,
          job.def.initiator,
          job.state,
        ]);
        const {
          items: page,
          offset: pageOffset,
          limit: pageLimit,
          hasMore,
        } = paginateItems(filtered, limit, offset);
        const text = formatBulletText(
          page.map((job) => {
            const started = job.startTime ? new Date(job.startTime).toISOString() : "unknown";
            return `${job.def.id} [${job.state ?? "unknown"}] started ${started}${job.def.initiator ? ` by ${job.def.initiator}` : ""}`;
          }),
          emptyListText("jobs"),
        );
        return {
          content: [{ type: "text", text }],
          structuredContent: {
            ok: true,
            total: jobs.length,
            filtered: filtered.length,
            offset: pageOffset,
            limit: pageLimit,
            query: query ?? null,
            items: page,
            hasMore,
          },
        };
      }

      if (action === "build" || action === "buildAndWait") {
        if (!datasetName) {
          return {
            content: [
              {
                type: "text",
                text: "Error: datasetName is required for build.",
              },
            ],
            isError: true,
          };
        }
        const jobDef: Record<string, unknown> = {
          outputs: [{ projectKey: pk, id: datasetName, type: "DATASET" }],
          type: buildMode ?? "RECURSIVE_BUILD",
        };
        if (autoUpdateSchema) {
          jobDef.autoUpdateSchemaBeforeEachRecipeRun = true;
        }
        const job = await post<{ id: string }>(`/public/api/projects/${enc}/jobs/`, jobDef);
        if (action === "build") {
          return {
            content: [
              {
                type: "text",
                text: `Job started: ${job.id}\nUse job(action: "wait") to block until completion or job(action: "get") to poll progress.`,
              },
            ],
            structuredContent: {
              ok: true,
              jobId: job.id,
              mode: jobDef.type,
              datasetName,
            },
          };
        }

        const waitResult = await waitForJob({
          projectEnc: enc,
          jobId: job.id,
          activity,
          includeLogs,
          maxLogLines,
          pollIntervalMs,
          timeoutMs,
        });
        const waitText = waitResult.content[0]?.text ?? "";
        return {
          content: [
            {
              type: "text",
              text: `Job started: ${job.id}\n${waitText}`,
            },
          ],
          structuredContent: {
            ...waitResult.structuredContent,
            startedJobId: job.id,
            mode: jobDef.type,
            datasetName,
          },
          ...(waitResult.isError ? { isError: true } : {}),
        };
      }

      // all remaining actions require jobId
      if (!jobId) {
        return {
          content: [{ type: "text", text: "Error: jobId is required for this action." }],
          isError: true,
        };
      }
      const jobEnc = encodeURIComponent(jobId);

      // Trailing slash required — DSS Cloud proxy misroutes URLs ending in .NNN (job ID timestamps)
      if (action === "get") {
        const j = await get<{
          baseStatus?: {
            activities?: Record<
              string,
              {
                recipeName?: string;
                activityId?: string;
                recipeType?: string;
                state?: string;
                totalTime?: number;
                preparingTime?: number;
                waitingTime?: number;
                runningTime?: number;
              }
            >;
            def?: {
              id?: string;
              type?: string;
              initiator?: string;
              outputs?: Array<{ targetDataset?: string }>;
            };
            state?: string;
            jobStartTime?: number;
            jobEndTime?: number;
          };
          globalState?: {
            done?: number;
            failed?: number;
            running?: number;
            total?: number;
            aborted?: number;
          };
        }>(`/public/api/projects/${enc}/jobs/${jobEnc}/`);

        const bs = j.baseStatus ?? {};
        const def = bs.def ?? {};
        const gs = j.globalState ?? {};

        const state = bs.state ?? "unknown";
        const normalizedState = normalizeJobWaitState(state);
        const parts: string[] = [
          `Job: ${def.id ?? jobId}`,
          `State: ${state} | Type: ${def.type ?? "unknown"}`,
        ];
        if (def.initiator) parts.push(`Initiator: ${def.initiator}`);
        const targets = (def.outputs ?? []).map((o) => o.targetDataset).filter(Boolean);
        if (targets.length > 0) parts.push(`Target: ${targets.join(", ")}`);

        const start = bs.jobStartTime;
        const end = bs.jobEndTime;
        if (start) {
          const dur = end ? `${((end - start) / 1000).toFixed(0)}s` : "running";
          parts.push(`Started: ${new Date(start).toISOString()} | Duration: ${dur}`);
        }

        if (gs.total) {
          parts.push(
            `Progress: ${gs.done ?? 0}/${gs.total} done, ${gs.failed ?? 0} failed, ${gs.running ?? 0} running`,
          );
        }

        const activities = bs.activities ?? {};
        const activityValues = Object.values(activities);
        if (activityValues.length > 0) {
          const maxActivitiesShown = 25;
          parts.push("\nActivities:");
          for (const act of activityValues.slice(0, maxActivitiesShown)) {
            const total = act.totalTime ? `${(act.totalTime / 1000).toFixed(1)}s` : "";
            const details: string[] = [];
            if (act.preparingTime && act.preparingTime > 0)
              details.push(`prep ${(act.preparingTime / 1000).toFixed(1)}s`);
            if (act.waitingTime && act.waitingTime > 0)
              details.push(`wait ${(act.waitingTime / 1000).toFixed(1)}s`);
            if (act.runningTime && act.runningTime > 0)
              details.push(`run ${(act.runningTime / 1000).toFixed(1)}s`);
            const detailStr = details.length > 0 ? ` (${details.join(", ")})` : "";
            parts.push(
              `  • ${act.recipeName ?? act.activityId} [${act.state ?? "?"}] ${total}${detailStr}`,
            );
          }
          if (activityValues.length > maxActivitiesShown) {
            parts.push(
              `  ... (${activityValues.length - maxActivitiesShown} more activities not shown)`,
            );
          }
        }

        const summary = {
          id: def.id ?? jobId,
          type: def.type ?? null,
          initiator: def.initiator ?? null,
          state,
          normalizedState,
          targetDatasets: targets,
          startTime: start ?? null,
          endTime: end ?? null,
          durationMs: start && end ? end - start : null,
          activityCount: activityValues.length,
          progress: {
            done: gs.done ?? 0,
            failed: gs.failed ?? 0,
            running: gs.running ?? 0,
            total: gs.total ?? null,
            aborted: gs.aborted ?? 0,
          },
        };

        if (includeDefinition) {
          parts.push("Definition: included in structuredContent.definition.");
        } else {
          parts.push("Tip: pass includeDefinition=true to include full job definition.");
        }
        return {
          content: [{ type: "text", text: parts.join("\n") }],
          structuredContent: {
            ok: true,
            job: summary,
            state,
            rawState: state,
            normalizedState,
            ...(includeDefinition ? { definition: j } : {}),
          },
        };
      }

      if (action === "wait") {
        return waitForJob({
          projectEnc: enc,
          jobId,
          activity,
          includeLogs,
          maxLogLines,
          pollIntervalMs,
          timeoutMs,
        });
      }

      if (action === "log") {
        const query = activity ? `?activity=${encodeURIComponent(activity)}` : "";
        const log = await getText(`/public/api/projects/${enc}/jobs/${jobEnc}/log/${query}`);

        if (!log) {
          return {
            content: [{ type: "text", text: "No log output available." }],
            structuredContent: { ok: true, lineCount: 0, truncated: false, linesShown: 0 },
          };
        }

        const lines = log.split("\n");
        const limit = maxLogLines ?? 50;
        if (lines.length > limit) {
          return {
            content: [
              {
                type: "text",
                text: `(showing last ${limit} of ${lines.length} lines — use maxLogLines to see more)\n${lines.slice(-limit).join("\n")}`,
              },
            ],
            structuredContent: {
              ok: true,
              lineCount: lines.length,
              truncated: true,
              linesShown: limit,
            },
          };
        }
        return {
          content: [{ type: "text", text: log }],
          structuredContent: {
            ok: true,
            lineCount: lines.length,
            truncated: false,
            linesShown: lines.length,
          },
        };
      }

      // action === "abort"
      await post(`/public/api/projects/${enc}/jobs/${jobEnc}/abort/`);
      return {
        content: [{ type: "text", text: `Job ${jobId} abort requested.` }],
        structuredContent: { ok: true, jobId, aborted: true },
      };
    },
  );
}
