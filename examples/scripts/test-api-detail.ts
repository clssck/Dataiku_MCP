import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(SCRIPT_DIR, "../..");
const OUTPUT_FILE = resolve(REPO_ROOT, "examples/fixtures/recipe-sample.json");

// Load .env
const env: Record<string, string> = {};
for (const line of readFileSync(resolve(REPO_ROOT, ".env"), "utf-8").split("\n")) {
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
  mkdirSync(resolve(REPO_ROOT, "examples/fixtures"), { recursive: true });
  writeFileSync(OUTPUT_FILE, JSON.stringify(json, null, 2));
  
  console.log(`\nSaved full recipe definition to: ${OUTPUT_FILE}`);
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
