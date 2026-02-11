import { describe, expect, it } from "vitest";
import { deepMerge } from "../../src/tools/deep-merge.js";

describe("deepMerge", () => {
	it("merges nested objects without dropping existing keys", () => {
		const base = {
			params: {
				connection: "snowflake_main",
				schema: "PUBLIC",
				table: "ORDERS",
			},
			formatParams: {
				separator: ",",
			},
		};
		const patch = {
			params: {
				schema: "ANALYTICS",
			},
		};

		const merged = deepMerge(base, patch);
		expect(merged).toEqual({
			params: {
				connection: "snowflake_main",
				schema: "ANALYTICS",
				table: "ORDERS",
			},
			formatParams: {
				separator: ",",
			},
		});
	});

	it("replaces arrays and primitive leaves from patch", () => {
		const base = {
			params: {
				steps: [{ type: "a" }],
				flag: true,
			},
		};
		const patch = {
			params: {
				steps: [{ type: "b" }, { type: "c" }],
				flag: false,
			},
		};

		const merged = deepMerge(base, patch);
		expect(merged).toEqual({
			params: {
				steps: [{ type: "b" }, { type: "c" }],
				flag: false,
			},
		});
	});
});
