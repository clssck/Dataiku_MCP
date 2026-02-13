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

function classifyDataikuError(
	status: number,
	body: string,
): DataikuErrorTaxonomy {
	if (status === 0) {
		return {
			category: "transient",
			retryable: true,
			retryHint:
				"Network/transport failure. Retry with backoff and verify DATAIKU_URL reachability.",
		};
	}

	const lowerBody = body.toLowerCase();
	const isHtmlNotFound =
		status === 404 && lowerBody.includes("<!doctype html>");

	if (isHtmlNotFound) {
		return {
			category: "transient",
			retryable: true,
			retryHint:
				"Received an HTML gateway/proxy response instead of API JSON. Retry with backoff.",
		};
	}

	if (status === 404) {
		return {
			category: "not_found",
			retryable: false,
			retryHint:
				"Verify projectKey and object identifiers (dataset/recipe/scenario/folder IDs).",
		};
	}

	if (status === 401 || status === 403) {
		return {
			category: "forbidden",
			retryable: false,
			retryHint:
				"Check API key validity and project permissions for the requested action.",
		};
	}

	if (status === 400 || status === 409 || status === 422) {
		return {
			category: "validation",
			retryable: false,
			retryHint:
				"Fix request parameters/payload and try again (same request will likely fail).",
		};
	}

	if (
		status === 408 ||
		status === 425 ||
		status === 429 ||
		(status >= 500 && status <= 599)
	) {
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
		if (!key)
			throw new Error("DATAIKU_API_KEY environment variable is required");
		_apiKey = key;
	}
	return _apiKey;
}

function getHeaders(): Record<string, string> {
	return {
		Authorization: `Bearer ${getApiKey()}`,
		"Content-Type": "application/json",
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

	constructor(
		public status: number,
		public statusText: string,
		public body: string,
	) {
		const details = DataikuError.buildDetails(status, statusText, body);
		super(details.message);
		this.name = "DataikuError";
		this.category = details.category;
		this.retryable = details.retryable;
		this.retryHint = details.retryHint;
	}

	private static extractSummary(
		status: number,
		statusText: string,
		body: string,
	): string {
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

	private static buildDetails(
		status: number,
		statusText: string,
		body: string,
	): { message: string } & DataikuErrorTaxonomy {
		const summary = DataikuError.extractSummary(status, statusText, body);
		const taxonomy = classifyDataikuError(status, body);
		return {
			...taxonomy,
			message: [
				`${status} ${statusText}: ${summary}`,
				`Error type: ${taxonomy.category}`,
				`Retryable: ${taxonomy.retryable ? "yes" : "no"}`,
				`Hint: ${taxonomy.retryHint}`,
			].join("\n"),
		};
	}
}

const MAX_RETRIES = 3;
const BASE_DELAY_MS = 2000;
const MAX_BACKOFF_DELAY_MS = 30_000;

function readPositiveIntEnv(name: string, fallback: number): number {
   const raw = process.env[name];
   if (!raw) return fallback;
   const parsed = Number.parseInt(raw, 10);
   if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
   return parsed;
}

const REQUEST_TIMEOUT_MS = readPositiveIntEnv("DATAIKU_REQUEST_TIMEOUT_MS", 30_000);
function isTransientError(status: number, body: string): boolean {
	return classifyDataikuError(status, body).category === "transient";
}
function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

function computeBackoffDelayMs(attempt: number): number {
   const cap = Math.min(MAX_BACKOFF_DELAY_MS, BASE_DELAY_MS * 2 ** attempt);
   return Math.floor(Math.random() * (cap + 1));
}
async function fetchWithRetry(
	url: string,
	init: RequestInit,
): Promise<Response> {
	for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      let timedOut = false;
      const controller = new AbortController();
      const timeout = setTimeout(() => {
         timedOut = true;
         controller.abort();
      }, REQUEST_TIMEOUT_MS);

      try {
         const res = await fetch(url, { ...init, signal: controller.signal });
			if (!res.ok) {
				const text = await res.text();
				if (attempt < MAX_RETRIES && isTransientError(res.status, text)) {
               await sleep(computeBackoffDelayMs(attempt));
					continue;
				}
				throw new DataikuError(res.status, res.statusText, text);
			}
			return res;
		} catch (error) {
			if (error instanceof DataikuError) throw error;
			if (attempt < MAX_RETRIES) {
            await sleep(computeBackoffDelayMs(attempt));
				continue;
			}
         const detail = timedOut
            ? `Request timed out after ${REQUEST_TIMEOUT_MS}ms`
            : error instanceof Error
               ? error.message
               : "Unknown transport error";
         const statusText = timedOut ? "Request Timeout" : "Network Error";
         throw new DataikuError(0, statusText, detail);
      } finally {
         clearTimeout(timeout);
		}
	}
	throw new Error("Request failed after retries");
}

async function request<T = unknown>(
	method: string,
	path: string,
	body?: unknown,
): Promise<T> {
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
		const summary =
			text.length > 300 ? `${text.slice(0, 300)}…` : text;
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
		headers: getHeaders(),
	});
	return res.text();
}

export async function post<T = unknown>(
	path: string,
	body?: unknown,
): Promise<T> {
	return request<T>("POST", path, body);
}

export async function put<T = unknown>(
	path: string,
	body: unknown,
): Promise<T> {
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
	const { readFile } = await import("node:fs/promises");
	const { basename } = await import("node:path");

	const fileData = await readFile(filePath);
	const fileName = basename(filePath);

	const formData = new FormData();
	formData.append("file", new Blob([fileData]), fileName);

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
		headers: getHeaders(),
	});

	if (!res.body) {
		throw new Error("No response body for stream request");
	}

	return {
		body: res.body,
		contentType: res.headers.get("content-type") ?? "application/octet-stream",
	};
}
