import { describe, expect, it } from "vitest";
import { DataikuError, getProjectKey } from "../../src/client.js";

describe("DataikuError", () => {
	it("extracts message from JSON error body", () => {
		const body = JSON.stringify({
			message: "Dataset my_dataset not found",
			errorType: "com.dataiku.dip.exceptions.NotFoundException",
			detailedMessage:
				"com.dataiku.dip.exceptions.NotFoundException: Dataset...\n\tat java.lang...",
		});
		const err = new DataikuError(404, "Not Found", body);
		expect(err.message).toContain("404 Not Found: Dataset my_dataset not found");
		expect(err.message).toContain("Error type: not_found");
		expect(err.message).toContain("Retryable: no");
		expect(err.message).toContain("Hint:");
		expect(err.status).toBe(404);
		expect(err.body).toBe(body);
		expect(err.category).toBe("not_found");
		expect(err.retryable).toBe(false);
	});

	it("handles JSON body without message field", () => {
		const body = JSON.stringify({ error: "something" });
		const err = new DataikuError(500, "Internal Server Error", body);
		// Falls through to raw body since no `message` field
		expect(err.message).toContain(`500 Internal Server Error: ${body}`);
		expect(err.category).toBe("transient");
		expect(err.retryable).toBe(true);
	});

	it("truncates long non-JSON bodies (HTML error pages)", () => {
		const html = `<!doctype html>${"<p>Error</p>".repeat(100)}`;
		const err = new DataikuError(404, "Not Found", html);
		expect(err.message.length).toBeLessThan(450);
		expect(err.message).toContain("…");
		expect(err.category).toBe("transient");
		expect(err.retryable).toBe(true);
	});

	it("handles short non-JSON body without truncation", () => {
		const err = new DataikuError(400, "Bad Request", "invalid input");
		expect(err.message).toContain("400 Bad Request: invalid input");
		expect(err.category).toBe("validation");
		expect(err.retryable).toBe(false);
	});

	it("handles empty body", () => {
		const err = new DataikuError(204, "No Content", "");
		expect(err.message).toContain("204 No Content: (empty response body)");
		expect(err.category).toBe("unknown");
		expect(err.retryable).toBe(false);
	});

	it("preserves status and body for programmatic access", () => {
		const body = JSON.stringify({ message: "Forbidden" });
		const err = new DataikuError(403, "Forbidden", body);
		expect(err.status).toBe(403);
		expect(err.statusText).toBe("Forbidden");
		expect(err.body).toBe(body);
		expect(err.name).toBe("DataikuError");
		expect(err.category).toBe("forbidden");
		expect(err.retryable).toBe(false);
		expect(err.retryHint).toContain("permissions");
	});

	it("classifies 429 as transient with retry hint", () => {
		const err = new DataikuError(429, "Too Many Requests", "Rate limit");
		expect(err.category).toBe("transient");
		expect(err.retryable).toBe(true);
		expect(err.retryHint).toContain("backoff");
		expect(err.message).toContain("Error type: transient");
		expect(err.message).toContain("Retryable: yes");
	});

	it("classifies status 0 network failures as transient", () => {
		const err = new DataikuError(0, "Network Error", "fetch failed");
		expect(err.category).toBe("transient");
		expect(err.retryable).toBe(true);
		expect(err.retryHint).toContain("Network/transport");
	});
});

describe("getProjectKey", () => {
	it("returns param value when provided", () => {
		expect(getProjectKey("MY_PROJECT")).toBe("MY_PROJECT");
	});

	it("throws when no param and no env var", () => {
		const original = process.env.DATAIKU_PROJECT_KEY;
		delete process.env.DATAIKU_PROJECT_KEY;
		try {
			expect(() => getProjectKey()).toThrow("projectKey is required");
		} finally {
			if (original) process.env.DATAIKU_PROJECT_KEY = original;
		}
	});

	it("falls back to env var when param is empty", () => {
		const original = process.env.DATAIKU_PROJECT_KEY;
		process.env.DATAIKU_PROJECT_KEY = "ENV_PROJECT";
		try {
			expect(getProjectKey()).toBe("ENV_PROJECT");
			expect(getProjectKey("")).toBe("ENV_PROJECT");
		} finally {
			if (original) {
				process.env.DATAIKU_PROJECT_KEY = original;
			} else {
				delete process.env.DATAIKU_PROJECT_KEY;
			}
		}
	});
});
