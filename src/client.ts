import { recordApiLatency } from "./debug-latency.js";

let _baseUrl: string | undefined;
let _apiKey: string | undefined;

export type DataikuErrorCategory =
  | "not_found"
  | "forbidden"
  | "validation"
  | "transient"
  | "unknown";

export interface DataikuErrorTaxonomy {
  category: DataikuErrorCategory;
  retryable: boolean;
  retryHint: string;
}

export interface DataikuRetryMetadata {
  method: string;
  enabled: boolean;
  maxAttempts: number;
  attempts: number;
  retries: number;
  delaysMs: number[];
  timedOut: boolean;
}

function classifyDataikuError(status: number, body: string): DataikuErrorTaxonomy {
  if (status === 0) {
    return {
      category: "transient",
      retryable: true,
      retryHint:
        "Network/transport failure. Retry with backoff and verify DATAIKU_URL reachability.",
    };
  }

  const lowerBody = body.toLowerCase();
  const isMissingDatasetRootPath =
    status === 500 &&
    lowerBody.includes("root path of the dataset") &&
    lowerBody.includes("does not exist");

  if (isMissingDatasetRootPath) {
    return {
      category: "validation",
      retryable: false,
      retryHint:
        "Dataset files are missing on storage. Build/materialize the dataset or upstream recipes before preview/download.",
    };
  }
  const isServerNotFoundLike =
    status >= 500 &&
    (lowerBody.includes("not found") || lowerBody.includes("does not exist")) &&
    ["dataset", "recipe", "scenario", "project", "folder"].some((token) =>
      lowerBody.includes(token),
    );
  if (isServerNotFoundLike) {
    return {
      category: "not_found",
      retryable: false,
      retryHint:
        "Requested object was not found. Verify projectKey and object identifiers before retrying.",
    };
  }

  const isServerValidationLike =
    status >= 500 &&
    (lowerBody.includes("invalid") ||
      lowerBody.includes("validation") ||
      lowerBody.includes("bad request") ||
      lowerBody.includes("illegal argument"));
  if (isServerValidationLike) {
    return {
      category: "validation",
      retryable: false,
      retryHint:
        "Request appears invalid for this endpoint. Fix parameters/payload before retrying.",
    };
  }

  if (status === 404) {
    const isHtmlGatewayResponse = lowerBody.includes("<!doctype html>");
    return {
      category: "not_found",
      retryable: false,
      retryHint: isHtmlGatewayResponse
        ? "Resource was not found (gateway returned HTML). Verify DATAIKU_URL, projectKey, and object identifiers."
        : "Verify projectKey and object identifiers (dataset/recipe/scenario/folder IDs).",
    };
  }

  if (status === 401 || status === 403) {
    return {
      category: "forbidden",
      retryable: false,
      retryHint: "Check API key validity and project permissions for the requested action.",
    };
  }

  if (status === 400 || status === 409 || status === 422) {
    return {
      category: "validation",
      retryable: false,
      retryHint: "Fix request parameters/payload and try again (same request will likely fail).",
    };
  }

  if (status === 408 || status === 425 || status === 429 || (status >= 500 && status <= 599)) {
    return {
      category: "transient",
      retryable: true,
      retryHint:
        "Retry with exponential backoff. If it persists, check DSS availability and upstream proxies.",
    };
  }

  return {
    category: "unknown",
    retryable: false,
    retryHint:
      "Inspect the response details and DSS logs to determine whether retry is appropriate.",
  };
}

function getBaseUrl(): string {
  if (!_baseUrl) {
    const url = process.env.DATAIKU_URL;
    if (!url) throw new Error("DATAIKU_URL environment variable is required");
    _baseUrl = url.replace(/\/+$/, "");
  }
  return _baseUrl;
}

function getApiKey(): string {
  if (!_apiKey) {
    const key = process.env.DATAIKU_API_KEY;
    if (!key) throw new Error("DATAIKU_API_KEY environment variable is required");
    _apiKey = key;
  }
  return _apiKey;
}

function getHeaders(): Record<string, string> {
  return {
    Authorization: `Bearer ${getApiKey()}`,
    Accept: "application/json",
    "Content-Type": "application/json",
  };
}

function getAnyHeaders(): Record<string, string> {
  return {
    Authorization: `Bearer ${getApiKey()}`,
    Accept: "*/*",
  };
}

export function getProjectKey(paramValue?: string): string {
  const key = paramValue || process.env.DATAIKU_PROJECT_KEY;
  if (!key) {
    throw new Error(
      "projectKey is required — pass it as a parameter or set DATAIKU_PROJECT_KEY env var",
    );
  }
  return key;
}

export class DataikuError extends Error {
  public category: DataikuErrorCategory;
  public retryable: boolean;
  public retryHint: string;
  public retry?: DataikuRetryMetadata;

  constructor(
    public status: number,
    public statusText: string,
    public body: string,
    retry?: DataikuRetryMetadata,
  ) {
    const details = DataikuError.buildDetails(status, statusText, body, retry);
    super(details.message);
    this.name = "DataikuError";
    this.category = details.category;
    this.retryable = details.retryable;
    this.retryHint = details.retryHint;
    this.retry = retry;
  }

  private static extractSummary(_status: number, _statusText: string, body: string): string {
    try {
      const parsed = JSON.parse(body);
      if (parsed.message) return String(parsed.message);
    } catch {
      // not JSON — use raw body
    }
    // Truncate long non-JSON bodies (HTML error pages, etc.)
    if (!body) return "(empty response body)";
    return body.length > 200 ? `${body.slice(0, 200)}…` : body;
  }

  private static formatRetryMetadata(retry?: DataikuRetryMetadata): string | undefined {
    if (!retry) return undefined;
    const shownDelays = retry.delaysMs.slice(0, 10);
    const delaysSuffix = retry.delaysMs.length > shownDelays.length ? ", …" : "";
    const delaysPart = shownDelays.length > 0 ? `[${shownDelays.join(", ")}${delaysSuffix}]` : "[]";
    return [
      `Retry attempts: ${retry.attempts}/${retry.maxAttempts}`,
      `Retry policy: ${retry.enabled ? "enabled" : "disabled"} for ${retry.method}`,
      `Retries performed: ${retry.retries}`,
      `Backoff delays (ms): ${delaysPart}`,
      `Timed out: ${retry.timedOut ? "yes" : "no"}`,
    ].join(" | ");
  }

  private static buildDetails(
    status: number,
    statusText: string,
    body: string,
    retry?: DataikuRetryMetadata,
  ): { message: string } & DataikuErrorTaxonomy {
    const summary = DataikuError.extractSummary(status, statusText, body);
    const taxonomy = classifyDataikuError(status, body);
    const retrySummary = DataikuError.formatRetryMetadata(retry);
    return {
      ...taxonomy,
      message: [
        `${status} ${statusText}: ${summary}`,
        `Error type: ${taxonomy.category}`,
        `Retryable: ${taxonomy.retryable ? "yes" : "no"}`,
        `Hint: ${taxonomy.retryHint}`,
        ...(retrySummary ? [retrySummary] : []),
      ].join("\n"),
    };
  }
}

const DEFAULT_RETRY_MAX_ATTEMPTS = 4;
const MAX_RETRY_ATTEMPTS_CAP = 10;
const BASE_DELAY_MS = 2000;
const MAX_BACKOFF_DELAY_MS = 30_000;

function readPositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return parsed;
}

function getRequestTimeoutMs(): number {
  return readPositiveIntEnv("DATAIKU_REQUEST_TIMEOUT_MS", 30_000);
}

function getRetryMaxAttempts(): number {
  const configured = readPositiveIntEnv("DATAIKU_RETRY_MAX_ATTEMPTS", DEFAULT_RETRY_MAX_ATTEMPTS);
  return Math.min(configured, MAX_RETRY_ATTEMPTS_CAP);
}

function isTransientError(status: number, body: string): boolean {
  return classifyDataikuError(status, body).category === "transient";
}

function shouldRetryMethod(method: string): boolean {
  return method.toUpperCase() === "GET";
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function computeBackoffDelayMs(retryNumber: number): number {
  const cap = Math.min(MAX_BACKOFF_DELAY_MS, BASE_DELAY_MS * 2 ** Math.max(0, retryNumber - 1));
  return Math.floor(Math.random() * (cap + 1));
}

function buildRetryMetadata(
  method: string,
  enabled: boolean,
  maxAttempts: number,
  attempts: number,
  delaysMs: number[],
  timedOut: boolean,
): DataikuRetryMetadata {
  return {
    method,
    enabled,
    maxAttempts,
    attempts,
    retries: Math.max(0, attempts - 1),
    delaysMs,
    timedOut,
  };
}

function toRequestPath(url: string): string {
  try {
    const parsed = new URL(url);
    return `${parsed.pathname}${parsed.search}`;
  } catch {
    return url;
  }
}

async function fetchWithRetry(url: string, init: RequestInit): Promise<Response> {
  const method = (init.method ?? "GET").toUpperCase();
  const retryEnabled = shouldRetryMethod(method);
  const maxAttempts = retryEnabled ? getRetryMaxAttempts() : 1;
  const delaysMs: number[] = [];
  const startedAt = Date.now();
  const requestPath = toRequestPath(url);

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    let timedOut = false;
    const requestTimeoutMs = getRequestTimeoutMs();
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, requestTimeoutMs);

    try {
      const res = await fetch(url, { ...init, method, signal: controller.signal });
      if (!res.ok) {
        const text = await res.text();
        const canRetry =
          retryEnabled && attempt < maxAttempts && isTransientError(res.status, text);
        if (canRetry) {
          const delayMs = computeBackoffDelayMs(attempt);
          delaysMs.push(delayMs);
          await sleep(delayMs);
          continue;
        }
        recordApiLatency({
          method,
          path: requestPath,
          durationMs: Date.now() - startedAt,
          attempts: attempt,
          retries: Math.max(0, attempt - 1),
          status: res.status,
          outcome: "http_error",
          timedOut: false,
          delaysMs: [...delaysMs],
        });
        throw new DataikuError(
          res.status,
          res.statusText,
          text,
          buildRetryMetadata(method, retryEnabled, maxAttempts, attempt, delaysMs, false),
        );
      }
      recordApiLatency({
        method,
        path: requestPath,
        durationMs: Date.now() - startedAt,
        attempts: attempt,
        retries: Math.max(0, attempt - 1),
        status: res.status,
        outcome: "success",
        timedOut: false,
        delaysMs: [...delaysMs],
      });
      return res;
    } catch (error) {
      if (error instanceof DataikuError) throw error;
      const canRetry = retryEnabled && attempt < maxAttempts;
      if (canRetry) {
        const delayMs = computeBackoffDelayMs(attempt);
        delaysMs.push(delayMs);
        await sleep(delayMs);
        continue;
      }
      const detail = timedOut
        ? `Request timed out after ${requestTimeoutMs}ms`
        : error instanceof Error
          ? error.message
          : "Unknown transport error";
      const statusText = timedOut ? "Request Timeout" : "Network Error";
      recordApiLatency({
        method,
        path: requestPath,
        durationMs: Date.now() - startedAt,
        attempts: attempt,
        retries: Math.max(0, attempt - 1),
        status: 0,
        outcome: "network_error",
        timedOut,
        delaysMs: [...delaysMs],
      });
      throw new DataikuError(
        0,
        statusText,
        detail,
        buildRetryMetadata(method, retryEnabled, maxAttempts, attempt, delaysMs, timedOut),
      );
    } finally {
      clearTimeout(timeout);
    }
  }

  recordApiLatency({
    method,
    path: requestPath,
    durationMs: Date.now() - startedAt,
    attempts: maxAttempts,
    retries: Math.max(0, maxAttempts - 1),
    status: 0,
    outcome: "network_error",
    timedOut: false,
    delaysMs: [...delaysMs],
  });
  throw new DataikuError(
    0,
    "Network Error",
    "Request failed before receiving a response.",
    buildRetryMetadata((init.method ?? "GET").toUpperCase(), false, 1, 1, [], false),
  );
}

async function request<T = unknown>(method: string, path: string, body?: unknown): Promise<T> {
  const url = `${getBaseUrl()}${path}`;
  const res = await fetchWithRetry(url, {
    method,
    headers: getHeaders(),
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

  const text = await res.text();
  if (!text) return undefined as T;
  try {
    return JSON.parse(text) as T;
  } catch {
    const summary = text.length > 300 ? `${text.slice(0, 300)}…` : text;
    throw new DataikuError(
      res.status,
      res.statusText || "Invalid JSON response",
      `Expected JSON response body but got non-JSON content: ${summary}`,
    );
  }
}

export async function get<T = unknown>(path: string): Promise<T> {
  return request<T>("GET", path);
}

export async function getText(path: string): Promise<string> {
  const url = `${getBaseUrl()}${path}`;
  const res = await fetchWithRetry(url, {
    method: "GET",
    headers: getAnyHeaders(),
  });
  return res.text();
}

export async function post<T = unknown>(path: string, body?: unknown): Promise<T> {
  return request<T>("POST", path, body);
}

export async function put<T = unknown>(path: string, body: unknown): Promise<T> {
  return request<T>("PUT", path, body);
}

export async function del(path: string): Promise<void> {
  await request("DELETE", path);
}

export async function putVoid(path: string, body: unknown): Promise<void> {
  const url = `${getBaseUrl()}${path}`;
  await fetchWithRetry(url, {
    method: "PUT",
    headers: getHeaders(),
    body: JSON.stringify(body),
  });
}

export async function upload(path: string, filePath: string): Promise<void> {
  const { openAsBlob } = await import("node:fs");
  const { basename } = await import("node:path");

  const fileBlob = await openAsBlob(filePath);
  const fileName = basename(filePath);

  const formData = new FormData();
  formData.append("file", fileBlob, fileName);

  const url = `${getBaseUrl()}${path}`;
  await fetchWithRetry(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${getApiKey()}` },
    body: formData,
  });
}

export async function stream(
  path: string,
): Promise<{ body: ReadableStream<Uint8Array>; contentType: string }> {
  const url = `${getBaseUrl()}${path}`;
  const res = await fetchWithRetry(url, {
    method: "GET",
    headers: getAnyHeaders(),
  });

  if (!res.body) {
    throw new Error("No response body for stream request");
  }

  return {
    body: res.body,
    contentType: res.headers.get("content-type") ?? "application/octet-stream",
  };
}
