import { readFileSync, writeFileSync } from "node:fs";

// Load .env
const env: Record<string, string> = {};
for (const line of readFileSync(".env", "utf-8").split("\n")) {
  const match = line.match(/^([^=]+)=(.*)$/);
  if (match) env[match[1]] = match[2];
}

const DATAIKU_URL = env.DATAIKU_URL;
const DATAIKU_API_KEY = env.DATAIKU_API_KEY;

if (!DATAIKU_URL || !DATAIKU_API_KEY) {
  console.error("Missing DATAIKU_URL or DATAIKU_API_KEY in .env");
  process.exit(1);
}

const PROJECT = "TUT_VARIABLES";

async function main() {
  // Get full recipe definition
  const recipeUrl = `${DATAIKU_URL}/public/api/projects/${PROJECT}/recipes/compute_tx_prepared`;
  
  console.log(`Fetching recipe definition from: ${recipeUrl}`);
  
  const response = await fetch(recipeUrl, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${DATAIKU_API_KEY}`,
    },
  });

  console.log(`Status: ${response.status} ${response.statusText}`);
  
  const json = await response.json();
  
  // Save to file for inspection
  writeFileSync(
    "/Users/clssck/Projects/dataiku_mcp_skill/recipe-sample.json",
    JSON.stringify(json, null, 2)
  );
  
  console.log(`\nSaved full recipe definition to recipe-sample.json`);
  console.log(`\nTop-level keys:`);
  console.log(Object.keys(json));
  
  if (json.recipe) {
    console.log(`\nRecipe object keys:`);
    console.log(Object.keys(json.recipe));
  }
  
  if (json.payload) {
    console.log(`\nPayload type: ${typeof json.payload}`);
    console.log(`Payload length: ${json.payload.length} chars`);
  }
}

main().catch(console.error);
