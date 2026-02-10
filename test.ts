import { readFileSync, existsSync, unlinkSync } from "node:fs";
import { spawn } from "node:child_process";

// Load .env
const env: Record<string, string> = { ...process.env } as Record<string, string>;
for (const line of readFileSync(".env", "utf-8").split("\n")) {
  const match = line.match(/^([^=]+)=(.*)$/);
  if (match) env[match[1]] = match[2];
}

const server = spawn("npx", ["tsx", "src/index.ts"], {
  cwd: "/Users/clssck/Projects/dataiku_mcp_skill",
  env,
  stdio: ["pipe", "pipe", "pipe"],
});

const PROJECT = env.DATAIKU_PROJECT_KEY || "TUT_VARIABLES";
const TEST_RECIPE = "__mcp_test_recipe__";

// Cleanup leftover test files
for (const f of ["compute_tx_prepared.json", `cards.csv.gz`]) {
  if (existsSync(f)) unlinkSync(f);
}

// Phases: each phase waits for all responses before sending the next
type Call = { id: number; name: string; arguments: Record<string, unknown> };
const phases: Call[][] = [
  // Phase 1: Read-only tools (all independent)
  [
    { id: 2, name: "project", arguments: { action: "list" } },
    { id: 3, name: "project", arguments: { action: "get", projectKey: PROJECT } },
    { id: 4, name: "project", arguments: { action: "flow", projectKey: PROJECT } },
    { id: 5, name: "dataset", arguments: { action: "list", projectKey: PROJECT } },
    { id: 6, name: "dataset", arguments: { action: "list" } }, // default project key (env var)
    { id: 7, name: "recipe", arguments: { action: "list", projectKey: PROJECT } },
    { id: 8, name: "job", arguments: { action: "list", projectKey: PROJECT } },
    { id: 9, name: "scenario", arguments: { action: "list", projectKey: PROJECT } },
  ],
  // Phase 2: Download tools
  [
    { id: 10, name: "recipe", arguments: { action: "download", projectKey: PROJECT, recipeName: "compute_tx_prepared" } },
    { id: 11, name: "dataset", arguments: { action: "download", projectKey: PROJECT, datasetName: "cards", limit: 50 } },
  ],
  // Phase 3: Create recipe
  [
    {
      id: 12,
      name: "recipe",
      arguments: {
        action: "create",
        projectKey: PROJECT,
        type: "python",
        name: TEST_RECIPE,
        inputs: { main: { items: [{ ref: "cards", deps: [] }] } },
        outputs: { main: { items: [{ ref: "cards", appendMode: false }] } },
        payload: "# MCP test recipe\nimport dataiku\nprint('hello from MCP')",
      },
    },
  ],
  // Phase 4: Get recipe we just created
  [
    { id: 13, name: "recipe", arguments: { action: "get", projectKey: PROJECT, recipeName: TEST_RECIPE } },
  ],
  // Phase 5: Delete the test recipe
  [
    { id: 14, name: "recipe", arguments: { action: "delete", projectKey: PROJECT, recipeName: TEST_RECIPE } },
  ],
];

let currentPhase = 0;
let pendingInPhase = 0;
const allCallIds = new Set(phases.flat().map((c) => c.id));

function sendPhase(phaseIndex: number) {
  if (phaseIndex >= phases.length) {
    server.kill();
    return;
  }
  const calls = phases[phaseIndex];
  pendingInPhase = calls.length;
  for (const call of calls) {
    server.stdin.write(
      JSON.stringify({
        jsonrpc: "2.0",
        id: call.id,
        method: "tools/call",
        params: { name: call.name, arguments: call.arguments },
      }) + "\n",
    );
  }
}

let buffer = "";
server.stdout.on("data", (chunk: Buffer) => {
  buffer += chunk.toString();
  const lines = buffer.split("\n");
  buffer = lines.pop() ?? "";
  for (const line of lines) {
    if (!line.trim()) continue;
    const msg = JSON.parse(line);

    if (msg.id === 1) {
      console.log("✅ Initialize OK\n");
      // Start first phase
      sendPhase(0);
      continue;
    }

    if (!allCallIds.has(msg.id) || !msg.result) continue;

    // Find the call across all phases
    const call = phases.flat().find((t) => t.id === msg.id);
    const action = (call?.arguments as Record<string, unknown>)?.action ?? "";
    const label = `${call?.name ?? "?"}(${action})`;

    if (msg.result.isError) {
      console.log(`❌ ${label}: ${msg.result.content?.[0]?.text ?? "error"}\n`);
    } else {
      const text = msg.result.content?.[0]?.text ?? "";
      const display = text.length > 500 ? text.slice(0, 500) + "\n  ... (truncated)" : text;
      console.log(`✅ ${label}:\n  ${display.replace(/\n/g, "\n  ")}\n`);
    }

    // Post-checks for file downloads
    if (msg.id === 10 && !msg.result.isError) {
      console.log(
        existsSync("compute_tx_prepared.json")
          ? "  📁 Verified: compute_tx_prepared.json exists\n"
          : "  ⚠️  compute_tx_prepared.json NOT found\n",
      );
    }
    if (msg.id === 11 && !msg.result.isError) {
      console.log(
        existsSync("cards.csv.gz")
          ? "  📁 Verified: cards.csv.gz exists\n"
          : "  ⚠️  cards.csv.gz NOT found\n",
      );
    }

    // Phase progression
    pendingInPhase--;
    if (pendingInPhase <= 0) {
      currentPhase++;
      sendPhase(currentPhase);
    }
  }
});

server.stderr.on("data", (chunk: Buffer) => {
  const text = chunk.toString().trim();
  if (text) console.error("stderr:", text);
});

server.on("close", (code: number | null) => {
  console.log(`\nServer exited (code ${code})`);
});

// Send init, then wait for response before starting phases
const initMessages = [
  {
    jsonrpc: "2.0",
    id: 1,
    method: "initialize",
    params: {
      protocolVersion: "2025-03-26",
      capabilities: {},
      clientInfo: { name: "test", version: "1.0.0" },
    },
  },
  { jsonrpc: "2.0", method: "notifications/initialized" },
];

for (const msg of initMessages) {
  server.stdin.write(JSON.stringify(msg) + "\n");
}
