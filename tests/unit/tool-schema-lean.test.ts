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
	stream: vi.fn(),
}));

vi.mock("../../src/client.js", () => clientMocks);

import { register as registerDatasets } from "../../src/tools/datasets.js";
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

describe("Lean Schema Passthrough", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		clientMocks.getProjectKey.mockImplementation(
			(projectKey?: string) => projectKey ?? "TEST_PROJECT",
		);
	});

	it("dataset create accepts advanced fields via passthrough", async () => {
		clientMocks.post.mockResolvedValue({});

		const { text, isError } = await callTool(registerDatasets, "dataset", {
			action: "create",
			projectKey: "PROJ",
			datasetName: "orders_stage",
			connection: "snowflake_conn",
			type: "Snowflake",
			table: "ORDERS_STAGE",
			dbSchema: "PUBLIC",
			catalog: "ANALYTICS",
			managed: false,
		});

		expect(isError).toBeFalsy();
		expect(text).toContain('Dataset "orders_stage" created');
		expect(text).toContain("Table: ORDERS_STAGE");
		expect(clientMocks.post).toHaveBeenCalledTimes(1);
		expect(clientMocks.post).toHaveBeenCalledWith(
			"/public/api/projects/PROJ/datasets/",
			expect.objectContaining({
				name: "orders_stage",
				type: "Snowflake",
				managed: false,
				params: expect.objectContaining({
					connection: "snowflake_conn",
					mode: "table",
					table: "ORDERS_STAGE",
					schema: "PUBLIC",
					catalog: "ANALYTICS",
				}),
			}),
		);
	});

	it("recipe create accepts join options via passthrough", async () => {
		clientMocks.post.mockResolvedValue({});
		clientMocks.put.mockResolvedValue({});
		clientMocks.get.mockImplementation(async (path: string) => {
			if (path.endsWith("/datasets/")) {
				return [
					{ name: "left_ds", managed: true, params: { connection: "fs_managed" } },
					{ name: "right_ds", managed: true, params: { connection: "fs_managed" } },
					{ name: "join_out", managed: true, params: { connection: "fs_managed" } },
				];
			}
			if (path.includes("/recipes/")) {
				return {
					recipe: { name: "join_join_out", type: "join" },
					payload: "",
				};
			}
			throw new Error(`Unexpected get path: ${path}`);
		});

		const { text, isError } = await callTool(registerRecipes, "recipe", {
			action: "create",
			projectKey: "PROJ",
			type: "join",
			inputDatasets: ["left_ds", "right_ds"],
			outputDataset: "join_out",
			outputConnection: "fs_managed",
			joinOn: "customer_id",
			joinType: "INNER",
		});

		expect(isError).toBeFalsy();
		expect(text).toContain('Recipe "join_join_out" created.');
		expect(text).toContain("Configured INNER join on: customer_id");
		expect(clientMocks.get).toHaveBeenCalledTimes(2);
		expect(clientMocks.post).toHaveBeenCalledTimes(1);
		expect(clientMocks.put).toHaveBeenCalledTimes(1);

		const putPayload = clientMocks.put.mock.calls[0]?.[1] as
			| { payload?: string }
			| undefined;
		expect(typeof putPayload?.payload).toBe("string");

		const parsed = JSON.parse(putPayload?.payload ?? "{}") as {
			virtualInputs?: Array<{
				joinType?: string;
				on?: Array<{ column?: string; related?: string }>;
			}>;
		};
		expect(parsed.virtualInputs?.[1]?.joinType).toBe("INNER");
		expect(parsed.virtualInputs?.[1]?.on?.[0]?.column).toBe("customer_id");
		expect(parsed.virtualInputs?.[1]?.on?.[0]?.related).toBe("customer_id");
	});
});
