import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { createServer } from "./server.js";

const transport = new StdioServerTransport();

async function main() {
	const server = createServer();
	await server.connect(transport);
}

void main().catch((error: unknown) => {
	console.error("Failed to start MCP server:", error);
	process.exit(1);
});
