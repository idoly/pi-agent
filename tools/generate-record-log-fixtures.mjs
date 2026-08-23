#!/usr/bin/env node

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";

const npmRoot = process.env.PI_NPM_ROOT
  ?? "/usr/local/node/lib/node_modules/@earendil-works/pi-coding-agent/node_modules";
const agentRoot = `${npmRoot}/@earendil-works/pi-agent-core`;
const { validateRecordLog } = await import(pathToFileURL(
  `${agentRoot}/dist/harness/reducer.js`,
).href);
const packageJson = JSON.parse(await readFile(`${agentRoot}/package.json`, "utf8"));
const run = (id = "run", seq = 1) => ({
  type: "operation_started", id, lane: "main", seq, timestamp: seq,
  sourceLeafId: null,
  intent: { kind: "run", originalPrompt: [], initialMessages: [] },
});
const base = (records, entries = [], openOperations = [records.find((r) => r.type === "operation_started")].filter(Boolean)) => ({
  lane: "main", records, entries, openOperations,
});
const capture = (input) => {
  try {
    validateRecordLog(input);
    return { valid: true };
  } catch (error) {
    return { reason: error.reason, message: error.message };
  }
};
const assistant = {
  type: "message", id: "assistant", parentId: null, seq: 2, timestamp: 2,
  message: {
    role: "assistant",
    content: [{ type: "toolCall", id: "call", name: "read", arguments: { path: "a.ts" } }],
    api: "openai-responses", provider: "openai", model: "gpt-5",
    usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    stopReason: "toolUse", timestamp: 2,
  },
};
const toolStart = (id, seq, index = 0) => ({
  type: "tool_started", id, lane: "main", seq, timestamp: seq, runId: "run",
  assistantEntryId: "assistant", toolIndex: index, toolCallId: "call",
  toolName: "read", effectiveArgs: { path: "a.ts" }, resultEntryId: `result-${id}`,
  replay: "safe",
});
const scenarios = {
  valid: base([
    run(),
    { type: "step_attempt", id: "attempt", lane: "main", seq: 2, timestamp: 2,
      runId: "run", step: "assistant", attempt: 1, resultEntryId: "response" },
  ]),
  multipleOpenOperations: base([run("one", 1), run("two", 2)], [], [run("one", 1), run("two", 2)]),
  unknownOperation: base([
    { type: "abort_requested", id: "abort", lane: "main", seq: 1, timestamp: 1, runId: "missing" },
  ], [], []),
  recordAfterFinish: base([
    run(),
    { type: "operation_finished", id: "finish", lane: "main", seq: 2, timestamp: 2,
      runId: "run", outcome: "completed" },
    { type: "abort_requested", id: "abort", lane: "main", seq: 3, timestamp: 3, runId: "run" },
  ], [], []),
  nonConsecutiveAttempt: base([
    run(),
    { type: "step_attempt", id: "attempt", lane: "main", seq: 2, timestamp: 2,
      runId: "run", step: "assistant", attempt: 2, resultEntryId: "response" },
  ]),
  queueAfterAbort: base([
    run(),
    { type: "abort_requested", id: "abort", lane: "main", seq: 2, timestamp: 2, runId: "run" },
    { type: "queue_enqueued", id: "queue", lane: "main", seq: 3, timestamp: 3,
      runId: "run", queue: "steer", target: { type: "message", id: "queued",
        message: { role: "user", content: [{ type: "text", text: "late" }], timestamp: 1 } } },
  ]),
  invalidQueueCancellation: base([
    run(),
    { type: "queue_cancelled", id: "cancel", lane: "main", seq: 2, timestamp: 2,
      runId: "run", entryId: "missing" },
  ]),
  toolCallMismatch: base([run(), toolStart("tool", 2, 1)], [assistant]),
  duplicateToolInvocation: base([run(), toolStart("first", 2), toolStart("second", 3)], [assistant]),
};
const results = Object.fromEntries(Object.entries(scenarios).map(([name, input]) => [name, capture(input)]));
const fixture = {
  upstream: { package: packageJson.name, version: packageJson.version },
  results,
};
const output = process.argv[2] ?? "compat-fixtures/record-log-0.84.2.json";
await mkdir(dirname(output), { recursive: true });
await writeFile(output, `${JSON.stringify(fixture, null, 2)}\n`);
