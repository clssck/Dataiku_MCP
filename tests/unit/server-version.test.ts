import { readFile } from "node:fs/promises";
import { describe, expect, it, vi } from "vitest";
import { createServer } from "../../src/server.js";

describe("server version resolution", () => {
	it("uses package-relative version instead of process.cwd()", async () => {
		const pkgRaw = await readFile(new URL("../../package.json", import.meta.url), "utf8");
		const pkg = JSON.parse(pkgRaw) as { version?: string };

		const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue("/tmp");
		try {
			const server = createServer() as unknown as {
				server?: { _serverInfo?: { version?: string } };
			};
			expect(server.server?._serverInfo?.version).toBe(pkg.version);
		} finally {
			cwdSpy.mockRestore();
		}
	});
});
