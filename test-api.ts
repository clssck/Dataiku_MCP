import { readFileSync } from "node:fs";

// Load .env
const env: Record<string, string> = {};
for (const line of readFileSync(".env", "utf-8").split("\n")) {
  const match = line.match(/^([^=]+)=(.*)$/);
  if (match) env[match[1]] = match[2];
}

const DATAIKU_URL = env.DATAIKU_URL;
const DATAIKU_API_KEY = env.DATAIKU_API_KEY;

if (!DATAIKU_URL || !DATAIKU_API_KEY) {
  console.error("Missing DATAIKU_URL or DATAIKU_API_KEY in .env file");
  process.exit(1);
}

const PROJECT = "TUT_VARIABLES";

interface TestEndpoint {
  method: string;
  url: string;
  description: string;
}

const endpoints: TestEndpoint[] = [
  {
    method: "GET",
    url: `/public/api/projects/${PROJECT}/recipes/compute_tx_prepared`,
    description: "Get recipe definition",
  },
  {
    method: "GET",
    url: `/public/api/projects/${PROJECT}/datasets/tx/data/?format=tsv-excel-header&limit=5`,
    description: "Export dataset as TSV with headers",
  },
  {
    method: "GET",
    url: `/public/api/projects/${PROJECT}/datasets/tx/data/?limit=5`,
    description: "Export dataset (default format)",
  },
  {
    method: "GET",
    url: `/public/api/projects/${PROJECT}/datasets/tx/data/?format=csv&limit=5`,
    description: "Export dataset as CSV",
  },
  {
    method: "GET",
    url: `/public/api/projects/${PROJECT}/datasets/tx_prepared/data/?format=csv&limit=5`,
    description: "Export tx_prepared dataset as CSV",
  },
];

async function testEndpoint(endpoint: TestEndpoint): Promise<void> {
  const fullUrl = `${DATAIKU_URL}${endpoint.url}`;
  console.log(`\n${"=".repeat(80)}`);
  console.log(`${endpoint.method} ${endpoint.url}`);
  console.log(`Description: ${endpoint.description}`);
  console.log(`${"=".repeat(80)}`);

  try {
    const response = await fetch(fullUrl, {
      method: endpoint.method,
      headers: {
        Authorization: `Bearer ${DATAIKU_API_KEY}`,
        Accept: "*/*",
      },
    });

    console.log(`Status: ${response.status} ${response.statusText}`);
    console.log(`Content-Type: ${response.headers.get("content-type") || "not set"}`);

    const text = await response.text();
    const preview = text.length > 500 ? text.slice(0, 500) + "\n... (truncated)" : text;
    
    console.log(`\nResponse body (${text.length} chars):`);
    console.log(preview);

    // Try to parse as JSON if applicable
    if (response.headers.get("content-type")?.includes("application/json")) {
      try {
        const json = JSON.parse(text);
        console.log("\nParsed JSON keys:", Object.keys(json).join(", "));
      } catch (e) {
        console.log("\nCould not parse as JSON");
      }
    }
  } catch (error) {
    console.error(`ERROR: ${error instanceof Error ? error.message : String(error)}`);
  }
}

async function main() {
  console.log(`Testing Dataiku DSS API at: ${DATAIKU_URL}`);
  console.log(`Project: ${PROJECT}\n`);

  for (const endpoint of endpoints) {
    await testEndpoint(endpoint);
  }

  console.log(`\n${"=".repeat(80)}`);
  console.log("Testing complete!");
  console.log(`${"=".repeat(80)}`);
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
