import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { beforeEach, describe, expect, it, vi } from "vitest";

const clientMocks = vi.hoisted(() => ({
	del: vi.fn(),
	get: vi.fn(),
	getProjectKey: vi.fn((projectKey?: string) => projectKey ?? "TEST_PROJECT"),
	post: vi.fn(),
	put: vi.fn(),
	putVoid: vi.fn(),
	stream: vi.fn(),
	getText: vi.fn(),
	upload: vi.fn(),
}));

vi.mock("../../src/client.js", () => clientMocks);

import { register as registerDatasets } from "../../src/tools/datasets.js";
import { register as registerProjects } from "../../src/tools/projects.js";
import { register as registerRecipes } from "../../src/tools/recipes.js";

async function callTool(
	registerTool: (server: McpServer) => void,
	name: string,
	args: Record<string, unknown>,
): Promise<{ text: string; isError: boolean | undefined }> {
	const server = new McpServer({ name: "test", version: "0.0.1" });
	registerTool(server);

	const [clientTransport, serverTransport] =
		InMemoryTransport.createLinkedPair();
	await server.connect(serverTransport);

	const client = new Client({ name: "test-client", version: "0.0.1" });
	await client.connect(clientTransport);

	try {
		const result = await client.callTool({ name, arguments: args });
		const content = result.content as Array<{ text?: string }> | undefined;
		return {
			text: content?.[0]?.text ?? "",
			isError: result.isError,
		};
	} finally {
		await client.close();
		await server.close();
	}
}

describe("Tool Behavior Coverage", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		clientMocks.getProjectKey.mockImplementation(
			(projectKey?: string) => projectKey ?? "TEST_PROJECT",
		);
	});

	it("dataset create infers type from existing datasets on same connection", async () => {
		clientMocks.get.mockResolvedValue([
			{ type: "Filesystem", params: { connection: "managed_conn" } },
		]);
		clientMocks.post.mockResolvedValue({});

		const { text, isError } = await callTool(registerDatasets, "dataset", {
			action: "create",
			projectKey: "PROJ",
			datasetName: "new_ds",
			connection: "managed_conn",
		});

		expect(isError).toBeFalsy();
		expect(text).toContain('Dataset "new_ds" created');
		expect(clientMocks.post).toHaveBeenCalledWith(
			"/public/api/projects/PROJ/datasets/",
			expect.objectContaining({
				name: "new_ds",
				type: "Filesystem",
				params: expect.objectContaining({ connection: "managed_conn" }),
			}),
		);
	});

	it("dataset preview converts TSV stream to CSV and respects row limit", async () => {
		const body = new Response("a\tb\n1\t2\n3\t4\n").body;
		clientMocks.stream.mockResolvedValue({ body });

		const { text, isError } = await callTool(registerDatasets, "dataset", {
			action: "preview",
			projectKey: "PROJ",
			datasetName: "sample_ds",
			limit: 1,
		});

		expect(isError).toBeFalsy();
		expect(text).toBe("a,b\n1,2");
		expect(clientMocks.stream).toHaveBeenCalledWith(
			"/public/api/projects/PROJ/datasets/sample_ds/data/?format=tsv-excel-header",
		);
	});

	it("recipe get includes payload body and truncation when requested", async () => {
		clientMocks.get.mockResolvedValue({
			recipe: {
				name: "compute_ass",
				type: "python",
				inputs: { main: { items: [{ ref: "in_ds" }] } },
				outputs: { main: { items: [{ ref: "out_ds" }] } },
			},
			payload: "line1\nline2\nline3\nline4",
		});

		const { text, isError } = await callTool(registerRecipes, "recipe", {
			action: "get",
			projectKey: "PROJ",
			recipeName: "compute_ass",
			includePayload: true,
			payloadMaxLines: 2,
		});

		expect(isError).toBeFalsy();
		expect(text).toContain("Recipe: compute_ass (python)");
		expect(text).toContain("Payload Body:");
		expect(text).toContain("line1\nline2");
		expect(text).toContain("... (2 more lines not shown)");
	});

	it("recipe update deep-merges nested recipe fields", async () => {
		clientMocks.get.mockResolvedValue({
			recipe: {
				name: "r1",
				type: "python",
				inputs: { main: { items: [{ ref: "in_ds" }] } },
			},
			payload: "old payload",
			meta: { owner: "alice" },
		});
		clientMocks.put.mockResolvedValue({});

		const { text, isError } = await callTool(registerRecipes, "recipe", {
			action: "update",
			projectKey: "PROJ",
			recipeName: "r1",
			data: {
				recipe: { outputs: { main: { items: [{ ref: "out_ds" }] } } },
				payload: "new payload",
			},
		});

		expect(isError).toBeFalsy();
		expect(text).toContain('Recipe "r1" updated.');
		expect(clientMocks.put).toHaveBeenCalledWith(
			"/public/api/projects/PROJ/recipes/r1",
			expect.objectContaining({
				payload: "new payload",
				recipe: expect.objectContaining({
					name: "r1",
					type: "python",
					inputs: { main: { items: [{ ref: "in_ds" }] } },
					outputs: { main: { items: [{ ref: "out_ds" }] } },
				}),
			}),
		);
	});

	it("project map returns raw graph when includeRaw is true", async () => {
		clientMocks.get.mockImplementation(async (path: string) => {
			if (path.endsWith("/flow/graph/")) {
				return {
					nodes: {
						ds_a: {
							type: "DATASET",
							ref: "ds_a",
							predecessors: [],
							successors: ["prep_a"],
						},
						prep_a: {
							type: "RECIPE",
							subType: "python",
							ref: "prep_a",
							predecessors: ["ds_a"],
							successors: ["ds_b"],
						},
						ds_b: {
							type: "DATASET",
							ref: "ds_b",
							predecessors: ["prep_a"],
							successors: [],
						},
					},
					datasets: ["ds_a", "ds_b"],
					recipes: ["prep_a"],
					folders: [],
				};
			}
			if (path.endsWith("/managedfolders/")) return [];
			if (path.endsWith("/datasets/")) return [{ name: "ds_a" }, { name: "ds_b" }];
			if (path.endsWith("/recipes/")) return [{ name: "prep_a" }];
			throw new Error(`Unexpected get path: ${path}`);
		});

		const { text, isError } = await callTool(registerProjects, "project", {
			action: "map",
			projectKey: "PROJ",
			includeRaw: true,
		});

		expect(isError).toBeFalsy();
		const parsed = JSON.parse(text) as {
			raw?: unknown;
			nodes?: unknown[];
			edges?: unknown[];
		};
		expect(parsed.raw).toBeDefined();
		expect(Array.isArray(parsed.nodes)).toBe(true);
		expect(Array.isArray(parsed.edges)).toBe(true);
	});

	it("project map omits raw graph by default", async () => {
		clientMocks.get.mockImplementation(async (path: string) => {
			if (path.endsWith("/flow/graph/")) {
				return {
					nodes: {},
					datasets: [],
					recipes: [],
					folders: [],
				};
			}
			if (path.endsWith("/managedfolders/")) return [];
			if (path.endsWith("/datasets/")) return [];
			if (path.endsWith("/recipes/")) return [];
			throw new Error(`Unexpected get path: ${path}`);
		});

		const { text, isError } = await callTool(registerProjects, "project", {
			action: "map",
			projectKey: "PROJ",
		});

		expect(isError).toBeFalsy();
		const parsed = JSON.parse(text) as { raw?: unknown };
		expect(parsed.raw).toBeUndefined();
	});
});
