import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(SCRIPT_DIR, "../..");
const OUTPUT_FILE = resolve(REPO_ROOT, "examples/output/recipe-definition.json");

// Load .env
const env: Record<string, string> = {};
for (const line of readFileSync(resolve(REPO_ROOT, ".env"), "utf-8").split("\n")) {
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

async function main() {
  const url = `${DATAIKU_URL}/public/api/projects/${PROJECT}/recipes/compute_tx_prepared`;
  
  console.log(`Fetching recipe definition from: ${url}\n`);

  const response = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${DATAIKU_API_KEY}`,
      Accept: "application/json",
    },
  });

  console.log(`Status: ${response.status} ${response.statusText}`);
  
  const json = await response.json();
  
  // Save full response to file for inspection
  mkdirSync(resolve(REPO_ROOT, "examples/output"), { recursive: true });
  writeFileSync(OUTPUT_FILE, JSON.stringify(json, null, 2));
  console.log(`\nFull response saved to: ${OUTPUT_FILE}`);
  
  // Print structure summary
  console.log(`\nTop-level keys: ${Object.keys(json).join(", ")}`);
  
  if (json.recipe) {
    console.log(`\nRecipe object keys: ${Object.keys(json.recipe).join(", ")}`);
    console.log(`\nRecipe type: ${json.recipe.type}`);
    console.log(`Recipe name: ${json.recipe.name}`);
    console.log(`Recipe project: ${json.recipe.projectKey}`);
    
    if (json.recipe.inputs) {
      console.log(`\nInputs:`, JSON.stringify(json.recipe.inputs, null, 2));
    }
    
    if (json.recipe.outputs) {
      console.log(`\nOutputs:`, JSON.stringify(json.recipe.outputs, null, 2));
    }
    
    if (json.recipe.params) {
      console.log(`\nParams keys:`, Object.keys(json.recipe.params).join(", "));
    }
  }
  
  if (json.payload) {
    console.log(`\nPayload type:`, typeof json.payload);
    if (typeof json.payload === "object") {
      console.log(`Payload keys:`, Object.keys(json.payload).join(", "));
    }
  }
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
