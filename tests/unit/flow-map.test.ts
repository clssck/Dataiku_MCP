import { describe, expect, it } from "vitest";
import { normalizeFlowGraph } from "../../src/tools/flow-map.js";

describe("normalizeFlowGraph", () => {
	it("builds connectivity and extracts recipe subtype", () => {
		const map = normalizeFlowGraph(
			{
				nodes: {
					customers: {
						type: "DATASET",
						ref: "customers",
						predecessors: [],
						successors: ["prepare_customers"],
					},
					prepare_customers: {
						type: "RECIPE",
						subType: "python",
						ref: "prepare_customers",
						predecessors: ["customers"],
						successors: ["customers_clean"],
					},
					customers_clean: {
						type: "DATASET",
						ref: "customers_clean",
						predecessors: ["prepare_customers"],
						successors: [],
					},
				},
				datasets: ["customers", "customers_clean"],
				recipes: ["prepare_customers"],
				folders: [],
			},
			"PROJ_A",
		);

		expect(map.projectKey).toBe("PROJ_A");
		expect(map.nodes).toEqual(
			expect.arrayContaining([
				expect.objectContaining({ id: "customers", kind: "dataset" }),
				expect.objectContaining({
					id: "prepare_customers",
					kind: "recipe",
					subtype: "python",
				}),
				expect.objectContaining({ id: "customers_clean", kind: "dataset" }),
			]),
		);
		expect(map.edges).toEqual(
			expect.arrayContaining([
				{ from: "customers", to: "prepare_customers", relation: "reads" },
				{
					from: "prepare_customers",
					to: "customers_clean",
					relation: "writes",
				},
			]),
		);
		expect(map.roots).toContain("customers");
		expect(map.leaves).toContain("customers_clean");
		expect(map.stats.datasets).toBe(2);
		expect(map.stats.recipes).toBe(1);
	});

	it("handles empty graph safely", () => {
		const map = normalizeFlowGraph(
			{ nodes: {}, datasets: [], recipes: [], folders: [] },
			"EMPTY",
		);
		expect(map.nodes).toEqual([]);
		expect(map.edges).toEqual([]);
		expect(map.stats.nodeCount).toBe(0);
		expect(map.stats.edgeCount).toBe(0);
		expect(map.warnings).toEqual([]);
	});

	it("adds placeholder nodes for missing references", () => {
		const map = normalizeFlowGraph(
			{
				nodes: {
					r_join: {
						type: "RECIPE",
						subType: "join",
						ref: "r_join",
						predecessors: ["missing_ds"],
						successors: [],
					},
				},
				datasets: [],
				recipes: ["r_join"],
				folders: [],
			},
			"MISSING",
		);

		expect(map.nodes).toEqual(
			expect.arrayContaining([
				expect.objectContaining({ id: "missing_ds", kind: "other" }),
			]),
		);
		expect(map.edges).toContainEqual({
			from: "missing_ds",
			to: "r_join",
			relation: "unknown",
		});
		expect(map.warnings.some((w) => w.includes("placeholder node"))).toBe(true);
	});

	it("maps implicit recipe nodes to subtype implicit", () => {
		const map = normalizeFlowGraph(
			{
				nodes: {
					"FilesInFolder->PROJ.tx": {
						type: "RUNNABLE_IMPLICIT_RECIPE",
						ref: "FilesInFolder->PROJ.tx",
						predecessors: ["folder_source"],
						successors: ["tx"],
					},
				},
				datasets: ["tx"],
				recipes: ["FilesInFolder->PROJ.tx"],
				folders: [],
			},
			"IMPLICIT",
		);

		expect(map.nodes).toEqual(
			expect.arrayContaining([
				expect.objectContaining({
					id: "FilesInFolder->PROJ.tx",
					kind: "recipe",
					subtype: "implicit",
				}),
			]),
		);
	});

	it("classifies folders by type and resolves folder names from lookup", () => {
		const map = normalizeFlowGraph(
			{
				nodes: {
					fld_123: {
						type: "COMPUTABLE_FOLDER",
						ref: "fld_123",
						predecessors: [],
						successors: ["FilesInFolder->PROJ.tx"],
					},
					"FilesInFolder->PROJ.tx": {
						type: "RUNNABLE_IMPLICIT_RECIPE",
						ref: "FilesInFolder->PROJ.tx",
						predecessors: ["fld_123"],
						successors: ["tx"],
					},
					download_tx: {
						type: "RUNNABLE_RECIPE",
						subType: "download",
						ref: "download_tx",
						predecessors: [],
						successors: ["fld_123"],
					},
					tx: {
						type: "DATASET",
						ref: "tx",
						predecessors: ["FilesInFolder->PROJ.tx"],
						successors: [],
					},
				},
				datasets: ["tx"],
				recipes: ["FilesInFolder->PROJ.tx", "download_tx"],
				folders: ["fld_123"],
			},
			"FOLDER_LOOKUP",
			{ folderNamesById: { fld_123: "tx files" } },
		);

		expect(map.nodes).toEqual(
			expect.arrayContaining([
				expect.objectContaining({
					id: "fld_123",
					kind: "folder",
					name: "tx files",
				}),
			]),
		);
		expect(map.edges).toEqual(
			expect.arrayContaining([
				{
					from: "fld_123",
					to: "FilesInFolder->PROJ.tx",
					relation: "reads",
				},
				{
					from: "download_tx",
					to: "fld_123",
					relation: "writes",
				},
			]),
		);
	});

	it("includes disconnected inventory items from options", () => {
		const map = normalizeFlowGraph(
			{
				nodes: {},
				datasets: [],
				recipes: [],
				folders: [],
			},
			"INVENTORY",
			{
				allDatasetNames: ["ds_isolated"],
				allRecipeNames: ["r_isolated"],
				allFolderIds: ["fld_isolated"],
				folderNamesById: { fld_isolated: "landing zone" },
			},
		);

		expect(map.nodes).toEqual(
			expect.arrayContaining([
				expect.objectContaining({ id: "ds_isolated", kind: "dataset" }),
				expect.objectContaining({ id: "r_isolated", kind: "recipe" }),
				expect.objectContaining({
					id: "fld_isolated",
					kind: "folder",
					name: "landing zone",
				}),
			]),
		);
		expect(map.edges).toEqual([]);
		expect(map.roots).toEqual(
			expect.arrayContaining(["ds_isolated", "r_isolated", "fld_isolated"]),
		);
		expect(map.leaves).toEqual(
			expect.arrayContaining(["ds_isolated", "r_isolated", "fld_isolated"]),
		);
	});

	it("handles cycles without roots or leaves", () => {
		const map = normalizeFlowGraph(
			{
				nodes: {
					ds_a: {
						type: "DATASET",
						ref: "ds_a",
						predecessors: ["r_b"],
						successors: ["r_a"],
					},
					r_a: {
						type: "RECIPE",
						subType: "python",
						ref: "r_a",
						predecessors: ["ds_a"],
						successors: ["ds_b"],
					},
					ds_b: {
						type: "DATASET",
						ref: "ds_b",
						predecessors: ["r_a"],
						successors: ["r_b"],
					},
					r_b: {
						type: "RECIPE",
						subType: "sql_query",
						ref: "r_b",
						predecessors: ["ds_b"],
						successors: ["ds_a"],
					},
				},
				datasets: ["ds_a", "ds_b"],
				recipes: ["r_a", "r_b"],
				folders: [],
			},
			"CYCLE",
		);

		expect(map.roots).toEqual([]);
		expect(map.leaves).toEqual([]);
		expect(map.stats.nodeCount).toBe(4);
		expect(map.stats.edgeCount).toBeGreaterThan(0);
	});

	it("returns a warning for non-object graph payloads", () => {
		const map = normalizeFlowGraph("not-an-object", "BAD_PAYLOAD");
		expect(map.nodes).toEqual([]);
		expect(map.warnings).toContain("Flow graph response was not an object.");
	});

	it("returns deterministically sorted nodes, edges, roots, and leaves", () => {
		const map = normalizeFlowGraph(
			{
				nodes: {
					r2: {
						type: "RECIPE",
						subType: "python",
						ref: "r2",
						predecessors: ["ds2"],
						successors: ["ds3"],
					},
					ds1: {
						type: "DATASET",
						ref: "ds1",
						predecessors: [],
						successors: ["r1"],
					},
					r1: {
						type: "RECIPE",
						subType: "python",
						ref: "r1",
						predecessors: ["ds1"],
						successors: ["ds2"],
					},
					ds3: {
						type: "DATASET",
						ref: "ds3",
						predecessors: ["r2"],
						successors: [],
					},
					ds2: {
						type: "DATASET",
						ref: "ds2",
						predecessors: ["r1"],
						successors: ["r2"],
					},
				},
				datasets: ["ds3", "ds1", "ds2"],
				recipes: ["r2", "r1"],
				folders: [],
			},
			"SORTED",
		);

		expect(map.nodes.map((n) => n.id)).toEqual(["ds1", "ds2", "ds3", "r1", "r2"]);
		expect(map.edges).toEqual([
			{ from: "ds1", to: "r1", relation: "reads" },
			{ from: "ds2", to: "r2", relation: "reads" },
			{ from: "r1", to: "ds2", relation: "writes" },
			{ from: "r2", to: "ds3", relation: "writes" },
		]);
		expect(map.roots).toEqual(["ds1"]);
		expect(map.leaves).toEqual(["ds3"]);
	});
});
