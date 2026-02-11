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
import { register as registerCodeEnvs } from "../../src/tools/code-envs.js";
import { register as registerConnections } from "../../src/tools/connections.js";
import { register as registerFolders } from "../../src/tools/folders.js";
import { register as registerJobs } from "../../src/tools/jobs.js";
import { register as registerProjects } from "../../src/tools/projects.js";
import { register as registerRecipes } from "../../src/tools/recipes.js";
import { register as registerScenarios } from "../../src/tools/scenarios.js";
import { register as registerVariables } from "../../src/tools/variables.js";

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

	it("dataset update deep-merges nested params without dropping existing keys", async () => {
		clientMocks.get.mockResolvedValue({
			name: "orders_ds",
			params: {
				connection: "snowflake_main",
				table: "ORDERS",
				schema: "PUBLIC",
			},
			formatParams: {
				separator: ",",
				charset: "utf8",
			},
		});
		clientMocks.put.mockResolvedValue({});

		const { text, isError } = await callTool(registerDatasets, "dataset", {
			action: "update",
			projectKey: "PROJ",
			datasetName: "orders_ds",
			data: {
				params: {
					schema: "ANALYTICS",
				},
			},
		});

		expect(isError).toBeFalsy();
		expect(text).toContain('Dataset "orders_ds" updated.');
		expect(clientMocks.put).toHaveBeenCalledWith(
			"/public/api/projects/PROJ/datasets/orders_ds",
			expect.objectContaining({
				params: {
					connection: "snowflake_main",
					table: "ORDERS",
					schema: "ANALYTICS",
				},
				formatParams: {
					separator: ",",
					charset: "utf8",
				},
			}),
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

	it("recipe create auto-creates outputs using inferred connection dataset type", async () => {
		clientMocks.get.mockResolvedValue([
			{
				name: "orders_input",
				type: "Snowflake",
				managed: false,
				params: { connection: "snowflake_main", schema: "PUBLIC" },
			},
		]);
		clientMocks.post.mockResolvedValue({});

		const { text, isError } = await callTool(registerRecipes, "recipe", {
			action: "create",
			projectKey: "PROJ",
			type: "sql_query",
			inputDatasets: ["orders_input"],
			outputDataset: "orders_enriched",
			outputConnection: "snowflake_main",
		});

		expect(isError).toBeFalsy();
		expect(text).toContain('Recipe "sql_query_orders_enriched" created.');
		expect(clientMocks.post).toHaveBeenCalledWith(
			"/public/api/projects/PROJ/datasets/",
			expect.objectContaining({
				name: "orders_enriched",
				type: "Snowflake",
				params: expect.objectContaining({
					connection: "snowflake_main",
					mode: "table",
					table: "orders_enriched",
					schema: "PUBLIC",
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

	it("connection infer marks connection as managed if any dataset is managed", async () => {
		clientMocks.get.mockResolvedValue([
			{
				name: "inbound_a",
				type: "Snowflake",
				managed: false,
				params: { connection: "warehouse", schema: "RAW" },
			},
			{
				name: "inbound_b",
				type: "Snowflake",
				managed: true,
				params: { connection: "warehouse", schema: "CURATED" },
			},
		]);

		const { text, isError } = await callTool(registerConnections, "connection", {
			action: "infer",
			projectKey: "PROJ",
		});

		expect(isError).toBeFalsy();
		expect(text).toContain("warehouse");
		expect(text).toContain("managed");
		expect(text).toContain("RAW");
		expect(text).toContain("CURATED");
	});

	it("variable set read-merges standard/local keys before writing", async () => {
		clientMocks.get.mockResolvedValue({
			standard: { keep: 1, replace: "old" },
			local: { localKeep: "x" },
		});
		clientMocks.putVoid.mockResolvedValue({});

		const { isError } = await callTool(registerVariables, "variable", {
			action: "set",
			projectKey: "PROJ",
			standard: { replace: "new", added: true },
			local: { localAdded: 42 },
		});

		expect(isError).toBeFalsy();
		expect(clientMocks.putVoid).toHaveBeenCalledWith(
			"/public/api/projects/PROJ/variables/",
			{
				standard: { keep: 1, replace: "new", added: true },
				local: { localKeep: "x", localAdded: 42 },
			},
		);
	});

	it("scenario update deep-merges nested params", async () => {
		clientMocks.get.mockResolvedValue({
			id: "nightly",
			active: true,
			params: {
				steps: [{ type: "build_flowitem" }],
				triggers: [{ type: "time" }],
				reporters: [{ type: "mail-scenario" }],
			},
		});
		clientMocks.put.mockResolvedValue({});

		const { text, isError } = await callTool(registerScenarios, "scenario", {
			action: "update",
			projectKey: "PROJ",
			scenarioId: "nightly",
			data: {
				params: {
					triggers: [{ type: "dataset-change" }],
				},
			},
		});

		expect(isError).toBeFalsy();
		expect(text).toContain('Scenario "nightly" updated.');
		expect(clientMocks.put).toHaveBeenCalledWith(
			"/public/api/projects/PROJ/scenarios/nightly/",
			expect.objectContaining({
				params: {
					steps: [{ type: "build_flowitem" }],
					triggers: [{ type: "dataset-change" }],
					reporters: [{ type: "mail-scenario" }],
				},
			}),
		);
	});

	it("job log returns only tail when maxLogLines is set", async () => {
		clientMocks.getText.mockResolvedValue("l1\nl2\nl3\nl4\nl5");

		const { text, isError } = await callTool(registerJobs, "job", {
			action: "log",
			projectKey: "PROJ",
			jobId: "job_123",
			maxLogLines: 2,
		});

		expect(isError).toBeFalsy();
		expect(text).toContain("showing last 2 of 5 lines");
		expect(text).toContain("l4\nl5");
		expect(text).not.toContain("l1");
	});

	it("managed_folder contents reports file count and metadata", async () => {
		clientMocks.get.mockResolvedValue({
			items: [
				{ path: "a.csv", size: 1536, lastModified: Date.UTC(2026, 0, 5) },
				{ path: "b.csv", size: 12, lastModified: Date.UTC(2026, 0, 6) },
			],
		});

		const { text, isError } = await callTool(registerFolders, "managed_folder", {
			action: "contents",
			projectKey: "PROJ",
			folderId: "folder_1",
		});

		expect(isError).toBeFalsy();
		expect(text).toContain("2 files:");
		expect(text).toContain("a.csv");
		expect(text).toContain("b.csv");
	});

	it("code_env get summarizes requested and installed packages", async () => {
		clientMocks.get.mockResolvedValue({
			envName: "py39",
			envLang: "PYTHON",
			desc: { pythonInterpreter: "PYTHON39" },
			specPackageList: "pandas\nnumpy",
			actualPackageList: "numpy==2.0.0\npandas==2.2.1\n",
		});

		const { text, isError } = await callTool(registerCodeEnvs, "code_env", {
			action: "get",
			envLang: "PYTHON",
			envName: "py39",
		});

		expect(isError).toBeFalsy();
		expect(text).toContain("py39 (PYTHON, PYTHON39)");
		expect(text).toContain("Requested packages:");
		expect(text).toContain("Installed packages:");
	});
});
