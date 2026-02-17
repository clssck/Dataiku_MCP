import { describe, expect, it, vi } from "vitest";
import { DataikuError, get, getProjectKey, post, upload } from "../../src/client.js";

async function expectDataikuError(promise: Promise<unknown>): Promise<DataikuError> {
  try {
    await promise;
    throw new Error("Expected request to fail with DataikuError");
  } catch (error) {
    expect(error).toBeInstanceOf(DataikuError);
    return error as DataikuError;
  }
}

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
    expect(err.category).toBe("not_found");
    expect(err.retryable).toBe(false);
  });

  it("classifies missing dataset root path errors as validation", () => {
    const err = new DataikuError(
      500,
      "Server Error",
      "Root path of the dataset tx_prepared does not exist",
    );
    expect(err.category).toBe("validation");
    expect(err.retryable).toBe(false);
    expect(err.retryHint).toContain("Build/materialize");
  });

  it("classifies server-side not-found-like 500 errors as not_found", () => {
    const err = new DataikuError(500, "Server Error", "Recipe DOES_NOT_EXIST not found");
    expect(err.category).toBe("not_found");
    expect(err.retryable).toBe(false);
    expect(err.retryHint).toContain("not found");
  });

  it("classifies server-side validation-like 500 errors as validation", () => {
    const err = new DataikuError(
      500,
      "Server Error",
      "Illegal argument: invalid payload for requested endpoint",
    );
    expect(err.category).toBe("validation");
    expect(err.retryable).toBe(false);
    expect(err.retryHint).toContain("invalid");
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

describe("request JSON handling", () => {
  it("throws DataikuError when a successful response is non-JSON", async () => {
    const originalUrl = process.env.DATAIKU_URL;
    const originalKey = process.env.DATAIKU_API_KEY;
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("<html>ok-but-not-json</html>", {
        status: 200,
        statusText: "OK",
      }),
    );

    process.env.DATAIKU_URL = "https://example.dataiku.io";
    process.env.DATAIKU_API_KEY = "test-token";

    try {
      await expect(get("/public/api/projects/")).rejects.toMatchObject({
        name: "DataikuError",
        status: 200,
        category: "unknown",
      });
      expect(fetchSpy).toHaveBeenCalledWith(
        "https://example.dataiku.io/public/api/projects/",
        expect.objectContaining({
          method: "GET",
          headers: expect.objectContaining({
            Accept: "application/json",
            Authorization: "Bearer test-token",
          }),
        }),
      );
    } finally {
      fetchSpy.mockRestore();
      if (originalUrl) {
        process.env.DATAIKU_URL = originalUrl;
      } else {
        delete process.env.DATAIKU_URL;
      }
      if (originalKey) {
        process.env.DATAIKU_API_KEY = originalKey;
      } else {
        delete process.env.DATAIKU_API_KEY;
      }
    }
  });
});

describe("retry behavior", () => {
  it("retries GET on 429 and succeeds", async () => {
    const originalUrl = process.env.DATAIKU_URL;
    const originalKey = process.env.DATAIKU_API_KEY;
    const originalMaxAttempts = process.env.DATAIKU_RETRY_MAX_ATTEMPTS;
    const randomSpy = vi.spyOn(Math, "random").mockReturnValue(0);
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ message: "rate limited" }), {
          status: 429,
          statusText: "Too Many Requests",
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ ok: true }), {
          status: 200,
          statusText: "OK",
        }),
      );

    process.env.DATAIKU_URL = "https://example.dataiku.io";
    process.env.DATAIKU_API_KEY = "test-token";
    process.env.DATAIKU_RETRY_MAX_ATTEMPTS = "3";

    try {
      const result = await get<{ ok: boolean }>("/public/api/projects/");
      expect(result).toEqual({ ok: true });
      expect(fetchSpy).toHaveBeenCalledTimes(2);
    } finally {
      fetchSpy.mockRestore();
      randomSpy.mockRestore();
      if (originalUrl) {
        process.env.DATAIKU_URL = originalUrl;
      } else {
        delete process.env.DATAIKU_URL;
      }
      if (originalKey) {
        process.env.DATAIKU_API_KEY = originalKey;
      } else {
        delete process.env.DATAIKU_API_KEY;
      }
      if (originalMaxAttempts) {
        process.env.DATAIKU_RETRY_MAX_ATTEMPTS = originalMaxAttempts;
      } else {
        delete process.env.DATAIKU_RETRY_MAX_ATTEMPTS;
      }
    }
  });

  it("retries GET on 502 and includes retry metadata after exhaustion", async () => {
    const originalUrl = process.env.DATAIKU_URL;
    const originalKey = process.env.DATAIKU_API_KEY;
    const originalMaxAttempts = process.env.DATAIKU_RETRY_MAX_ATTEMPTS;
    const randomSpy = vi.spyOn(Math, "random").mockReturnValue(0);
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(
      async () =>
        new Response("upstream unavailable", {
          status: 502,
          statusText: "Bad Gateway",
        }),
    );

    process.env.DATAIKU_URL = "https://example.dataiku.io";
    process.env.DATAIKU_API_KEY = "test-token";
    process.env.DATAIKU_RETRY_MAX_ATTEMPTS = "2";

    try {
      const error = await expectDataikuError(get("/public/api/projects/"));
      expect(error.status).toBe(502);
      expect(error.retry).toMatchObject({
        method: "GET",
        enabled: true,
        maxAttempts: 2,
        attempts: 2,
        retries: 1,
        timedOut: false,
      });
      expect(error.message).toContain("Retry attempts: 2/2");
      expect(fetchSpy).toHaveBeenCalledTimes(2);
    } finally {
      fetchSpy.mockRestore();
      randomSpy.mockRestore();
      if (originalUrl) {
        process.env.DATAIKU_URL = originalUrl;
      } else {
        delete process.env.DATAIKU_URL;
      }
      if (originalKey) {
        process.env.DATAIKU_API_KEY = originalKey;
      } else {
        delete process.env.DATAIKU_API_KEY;
      }
      if (originalMaxAttempts) {
        process.env.DATAIKU_RETRY_MAX_ATTEMPTS = originalMaxAttempts;
      } else {
        delete process.env.DATAIKU_RETRY_MAX_ATTEMPTS;
      }
    }
  });

  it("retries GET timeouts and reports timeout metadata", async () => {
    const originalUrl = process.env.DATAIKU_URL;
    const originalKey = process.env.DATAIKU_API_KEY;
    const originalTimeoutMs = process.env.DATAIKU_REQUEST_TIMEOUT_MS;
    const originalMaxAttempts = process.env.DATAIKU_RETRY_MAX_ATTEMPTS;
    const randomSpy = vi.spyOn(Math, "random").mockReturnValue(0);
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(
      async (_input: RequestInfo | URL, init?: RequestInit) =>
        await new Promise<Response>((_resolve, reject) => {
          const signal = init?.signal;
          if (!signal) {
            reject(new Error("missing signal"));
            return;
          }
          signal.addEventListener(
            "abort",
            () => {
              reject(new Error("aborted"));
            },
            { once: true },
          );
        }),
    );

    process.env.DATAIKU_URL = "https://example.dataiku.io";
    process.env.DATAIKU_API_KEY = "test-token";
    process.env.DATAIKU_REQUEST_TIMEOUT_MS = "5";
    process.env.DATAIKU_RETRY_MAX_ATTEMPTS = "2";

    try {
      const error = await expectDataikuError(get("/public/api/projects/"));
      expect(error.status).toBe(0);
      expect(error.statusText).toBe("Request Timeout");
      expect(error.retry).toMatchObject({
        method: "GET",
        enabled: true,
        maxAttempts: 2,
        attempts: 2,
        retries: 1,
        timedOut: true,
      });
      expect(error.message).toContain("Request timed out after 5ms");
      expect(fetchSpy).toHaveBeenCalledTimes(2);
    } finally {
      fetchSpy.mockRestore();
      randomSpy.mockRestore();
      if (originalUrl) {
        process.env.DATAIKU_URL = originalUrl;
      } else {
        delete process.env.DATAIKU_URL;
      }
      if (originalKey) {
        process.env.DATAIKU_API_KEY = originalKey;
      } else {
        delete process.env.DATAIKU_API_KEY;
      }
      if (originalTimeoutMs) {
        process.env.DATAIKU_REQUEST_TIMEOUT_MS = originalTimeoutMs;
      } else {
        delete process.env.DATAIKU_REQUEST_TIMEOUT_MS;
      }
      if (originalMaxAttempts) {
        process.env.DATAIKU_RETRY_MAX_ATTEMPTS = originalMaxAttempts;
      } else {
        delete process.env.DATAIKU_RETRY_MAX_ATTEMPTS;
      }
    }
  });

  it("does not retry POST on transient HTTP failures by default", async () => {
    const originalUrl = process.env.DATAIKU_URL;
    const originalKey = process.env.DATAIKU_API_KEY;
    const originalMaxAttempts = process.env.DATAIKU_RETRY_MAX_ATTEMPTS;
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("gateway unavailable", {
        status: 502,
        statusText: "Bad Gateway",
      }),
    );

    process.env.DATAIKU_URL = "https://example.dataiku.io";
    process.env.DATAIKU_API_KEY = "test-token";
    process.env.DATAIKU_RETRY_MAX_ATTEMPTS = "4";

    try {
      const error = await expectDataikuError(post("/public/api/projects/", { ping: true }));
      expect(error.retry).toMatchObject({
        method: "POST",
        enabled: false,
        maxAttempts: 1,
        attempts: 1,
        retries: 0,
      });
      expect(fetchSpy).toHaveBeenCalledTimes(1);
    } finally {
      fetchSpy.mockRestore();
      if (originalUrl) {
        process.env.DATAIKU_URL = originalUrl;
      } else {
        delete process.env.DATAIKU_URL;
      }
      if (originalKey) {
        process.env.DATAIKU_API_KEY = originalKey;
      } else {
        delete process.env.DATAIKU_API_KEY;
      }
      if (originalMaxAttempts) {
        process.env.DATAIKU_RETRY_MAX_ATTEMPTS = originalMaxAttempts;
      } else {
        delete process.env.DATAIKU_RETRY_MAX_ATTEMPTS;
      }
    }
  });
});

describe("upload", () => {
  it("uses multipart FormData without buffering file contents in test code", async () => {
    const { mkdtemp, writeFile } = await import("node:fs/promises");
    const { join } = await import("node:path");
    const { tmpdir } = await import("node:os");

    const originalUrl = process.env.DATAIKU_URL;
    const originalKey = process.env.DATAIKU_API_KEY;
    const tempDir = await mkdtemp(join(tmpdir(), "dataiku-upload-test-"));
    const filePath = join(tempDir, "sample.txt");
    await writeFile(filePath, "hello world", "utf8");

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response("", { status: 200, statusText: "OK" }));

    process.env.DATAIKU_URL = "https://example.dataiku.io";
    process.env.DATAIKU_API_KEY = "test-token";

    try {
      await upload("/public/api/projects/PROJ/managedfolders/F/contents/file.txt", filePath);
      expect(fetchSpy).toHaveBeenCalledTimes(1);
      const [, init] = fetchSpy.mock.calls[0] ?? [];
      expect(init?.method).toBe("POST");
      expect(init?.headers).toEqual({ Authorization: "Bearer test-token" });
      expect(init?.body).toBeInstanceOf(FormData);
    } finally {
      fetchSpy.mockRestore();
      if (originalUrl) {
        process.env.DATAIKU_URL = originalUrl;
      } else {
        delete process.env.DATAIKU_URL;
      }
      if (originalKey) {
        process.env.DATAIKU_API_KEY = originalKey;
      } else {
        delete process.env.DATAIKU_API_KEY;
      }
    }
  });
});
