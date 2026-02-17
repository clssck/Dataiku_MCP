import { AsyncLocalStorage } from "node:async_hooks";

export interface ApiLatencyRecord {
  method: string;
  path: string;
  durationMs: number;
  attempts: number;
  retries: number;
  status: number;
  outcome: "success" | "http_error" | "network_error";
  timedOut: boolean;
  delaysMs: number[];
}

interface ToolLatencyStore {
  apiCalls: ApiLatencyRecord[];
}

const toolLatencyStore = new AsyncLocalStorage<ToolLatencyStore>();

function envEnabled(name: string): boolean {
  const raw = process.env[name]?.trim().toLowerCase();
  return raw === "1" || raw === "true" || raw === "yes" || raw === "on";
}

export function isToolLatencyDebugEnabled(): boolean {
  return envEnabled("DATAIKU_DEBUG_LATENCY");
}

export function recordApiLatency(record: ApiLatencyRecord): void {
  const store = toolLatencyStore.getStore();
  if (!store) return;
  store.apiCalls.push(record);
}

export async function runWithToolLatency<T>(
  enabled: boolean,
  operation: () => Promise<T>,
): Promise<{ result: T; totalMs: number; apiCalls: ApiLatencyRecord[] }> {
  const startedAt = Date.now();
  if (!enabled) {
    const result = await operation();
    return { result, totalMs: Date.now() - startedAt, apiCalls: [] };
  }

  const store: ToolLatencyStore = { apiCalls: [] };
  const result = await toolLatencyStore.run(store, operation);
  return {
    result,
    totalMs: Date.now() - startedAt,
    apiCalls: store.apiCalls,
  };
}
