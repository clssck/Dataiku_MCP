import { describe, expect, it } from "vitest";
import { tsvLineToCSV } from "../../src/tools/datasets.js";

describe("tsvLineToCSV", () => {
	it("converts simple TSV fields to CSV", () => {
		expect(tsvLineToCSV("alice\t30\tnew york")).toBe("alice,30,new york");
	});

	it("quotes fields containing commas", () => {
		expect(tsvLineToCSV("alice\t30\tnew york, ny")).toBe(
			'alice,30,"new york, ny"',
		);
	});

	it("quotes and escapes fields containing double quotes", () => {
		expect(tsvLineToCSV('alice\t"""Bob"""\t30')).toBe('alice,"""Bob""",30');
	});

	it("quotes fields containing newlines", () => {
		expect(tsvLineToCSV("alice\tline1\nline2\t30")).toBe(
			'alice,"line1\nline2",30',
		);
	});

	it("handles empty fields", () => {
		expect(tsvLineToCSV("\t\t")).toBe(",,");
	});

	it("handles single field", () => {
		expect(tsvLineToCSV("hello")).toBe("hello");
	});

	it("handles fields with all special characters", () => {
		const input = 'has,comma\thas"quote\thas\nnewline';
		const result = tsvLineToCSV(input);
		// Each field should be quoted
		expect(result).toBe('"has,comma","has""quote","has\nnewline"');
	});

	it("preserves tabs inside quoted TSV fields", () => {
		const input = 'left\t"has\ttab"\tright';
		expect(tsvLineToCSV(input)).toBe('left,"has\ttab",right');
	});
});
