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

import { register as registerCodeEnvs } from "../../src/tools/code-envs.js";
import { register as registerConnections } from "../../src/tools/connections.js";
import { register as registerDatasets } from "../../src/tools/datasets.js";
import { register as registerFolders } from "../../src/tools/folders.js";
import { register as registerJobs } from "../../src/tools/jobs.js";
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

describe("Tool Errors And Summaries", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		clientMocks.getProjectKey.mockImplementation(
			(projectKey?: string) => projectKey ?? "TEST_PROJECT",
		);
	});

	it("variable set errors when no standard/local patch is provided", async () => {
		const { text, isError } = await callTool(registerVariables, "variable", {
			action: "set",
			projectKey: "PROJ",
		});

		expect(isError).toBe(true);
		expect(text).toContain("at least one of standard or local");
	});

	it("job build errors when datasetName is missing", async () => {
		const { text, isError } = await callTool(registerJobs, "job", {
			action: "build",
			projectKey: "PROJ",
		});

		expect(isError).toBe(true);
		expect(text).toContain("datasetName is required for build");
	});

	it("job get errors when jobId is missing", async () => {
		const { text, isError } = await callTool(registerJobs, "job", {
			action: "get",
			projectKey: "PROJ",
		});

		expect(isError).toBe(true);
		expect(text).toContain("jobId is required");
	});

	it("job get includes progress and activity timing summary", async () => {
		clientMocks.get.mockResolvedValue({
			baseStatus: {
				def: { id: "job_001", type: "RECURSIVE_BUILD", initiator: "tester" },
				state: "RUNNING",
				jobStartTime: Date.UTC(2026, 0, 1, 10, 0, 0),
				activities: {
					"a1": {
						recipeName: "prep_orders",
						state: "RUNNING",
						runningTime: 5000,
						totalTime: 5500,
					},
				},
			},
			globalState: {
				done: 1,
				total: 3,
				failed: 0,
				running: 1,
			},
		});

		const { text, isError } = await callTool(registerJobs, "job", {
			action: "get",
			projectKey: "PROJ",
			jobId: "job_001",
		});

		expect(isError).toBeFalsy();
		expect(text).toContain("Job: job_001");
		expect(text).toContain("Progress: 1/3 done");
		expect(text).toContain("Activities:");
		expect(text).toContain("prep_orders");
	});

	it("connection infer returns no-connection message when none are discoverable", async () => {
		clientMocks.get.mockResolvedValue([
			{ name: "ds_without_connection", managed: false },
		]);

		const { text, isError } = await callTool(registerConnections, "connection", {
			action: "infer",
			projectKey: "PROJ",
		});

		expect(isError).toBeFalsy();
		expect(text).toContain("No connections found");
	});

	it("connection infer output is deterministically sorted", async () => {
		clientMocks.get.mockResolvedValue([
			{
				name: "b1",
				type: "TypeB",
				managed: true,
				params: { connection: "z_conn", schema: "ZETA" },
			},
			{
				name: "a1",
				type: "TypeA",
				managed: true,
				params: { connection: "a_conn", schema: "BETA" },
			},
			{
				name: "a2",
				type: "TypeZ",
				managed: true,
				params: { connection: "a_conn", schema: "ALPHA" },
			},
		]);

		const { text, isError } = await callTool(registerConnections, "connection", {
			action: "infer",
			projectKey: "PROJ",
		});

		expect(isError).toBeFalsy();
		const lines = text
			.split("\n")
			.filter((l) => l.startsWith("• "));
		expect(lines[0]).toContain("• a_conn");
		expect(lines[1]).toContain("• z_conn");
		expect(lines[0]).toContain("TypeA/TypeZ");
		expect(lines[0]).toContain("schemas: ALPHA, BETA");
	});

	it("managed_folder download errors when path is missing", async () => {
		const { text, isError } = await callTool(registerFolders, "managed_folder", {
			action: "download",
			projectKey: "PROJ",
			folderId: "folder_1",
		});

		expect(isError).toBe(true);
		expect(text).toContain("path is required for this action");
	});

	it("managed_folder get errors when folderId is missing", async () => {
		const { text, isError } = await callTool(registerFolders, "managed_folder", {
			action: "get",
			projectKey: "PROJ",
		});

		expect(isError).toBe(true);
		expect(text).toContain("folderId is required for this action");
	});

	it("code_env get errors when envName/envLang are missing", async () => {
		const { text, isError } = await callTool(registerCodeEnvs, "code_env", {
			action: "get",
			envLang: "PYTHON",
		});

		expect(isError).toBe(true);
		expect(text).toContain("envLang and envName are required");
	});

	it("code_env get with full=true includes full package lines", async () => {
		clientMocks.get.mockResolvedValue({
			envName: "py39",
			envLang: "PYTHON",
			actualPackageList: "numpy==2.0.0\npandas==2.2.1\n",
			specPackageList: "numpy\npandas",
		});

		const { text, isError } = await callTool(registerCodeEnvs, "code_env", {
			action: "get",
			envLang: "PYTHON",
			envName: "py39",
			full: true,
		});

		expect(isError).toBeFalsy();
		expect(text).toContain("Installed packages:");
		expect(text).toContain("numpy==2.0.0");
		expect(text).toContain("pandas==2.2.1");
	});

	it("dataset create errors when required fields are missing", async () => {
		const { text, isError } = await callTool(registerDatasets, "dataset", {
			action: "create",
			projectKey: "PROJ",
			datasetName: "only_name",
		});

		expect(isError).toBe(true);
		expect(text).toContain("datasetName and connection are required");
	});

	it("recipe create errors when required fields are missing", async () => {
		const { text, isError } = await callTool(registerRecipes, "recipe", {
			action: "create",
			projectKey: "PROJ",
			type: "python",
		});

		expect(isError).toBe(true);
		expect(text).toContain("type and (inputDatasets + outputDataset)");
	});

	it("scenario get shows definition tip by default and full definition when requested", async () => {
		clientMocks.get.mockResolvedValue({
			id: "nightly",
			name: "Nightly Build",
			type: "step_based",
			active: true,
			params: {
				steps: [{ type: "build_flowitem" }],
				triggers: [{ type: "time" }],
			},
		});

		const defaultView = await callTool(registerScenarios, "scenario", {
			action: "get",
			projectKey: "PROJ",
			scenarioId: "nightly",
		});

		expect(defaultView.isError).toBeFalsy();
		expect(defaultView.text).toContain("Definition: present");
		expect(defaultView.text).toContain("includeDefinition=true");

		const detailedView = await callTool(registerScenarios, "scenario", {
			action: "get",
			projectKey: "PROJ",
			scenarioId: "nightly",
			includeDefinition: true,
			definitionMaxLines: 2,
		});

		expect(detailedView.isError).toBeFalsy();
		expect(detailedView.text).toContain("Definition:");
		expect(detailedView.text).toContain("more lines not shown");
	});

	it("scenario run errors when scenarioId is missing", async () => {
		const { text, isError } = await callTool(registerScenarios, "scenario", {
			action: "run",
			projectKey: "PROJ",
		});

		expect(isError).toBe(true);
		expect(text).toContain("scenarioId is required");
	});

	it("variable get shows key-list truncation after max summary items", async () => {
		const standard = Object.fromEntries(
			Array.from({ length: 25 }, (_, i) => [`k_${i.toString().padStart(2, "0")}`, i]),
		);
		clientMocks.get.mockResolvedValue({
			standard,
			local: {},
		});

		const { text, isError } = await callTool(registerVariables, "variable", {
			action: "get",
			projectKey: "PROJ",
		});

		expect(isError).toBeFalsy();
		expect(text).toContain("Standard keys: 25");
		expect(text).toContain("(+5 more)");
	});
});
