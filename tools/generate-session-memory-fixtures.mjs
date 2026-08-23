#!/usr/bin/env node

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";

const npmRoot = process.env.PI_NPM_ROOT
  ?? "/usr/local/node/lib/node_modules/@earendil-works/pi-coding-agent/node_modules";
const agentRoot = `${npmRoot}/@earendil-works/pi-agent-core`;
const sessionApi = await import(pathToFileURL(
  `${agentRoot}/dist/harness/session/index.js`,
).href);
const packageJson = JSON.parse(await readFile(`${agentRoot}/package.json`, "utf8"));
const { InMemorySessionRepo, buildSessionContext } = sessionApi;

const user = (text, timestamp = 1) => ({
  role: "user", content: [{ type: "text", text }], timestamp,
});
const usage = {
  input: 10, output: 5, cacheRead: 3, cacheWrite: 2, totalTokens: 20,
  cost: { input: 1, output: 2, cacheRead: 3, cacheWrite: 4, total: 10 },
};
const assistant = (text) => ({
  role: "assistant", content: [{ type: "text", text }],
  api: "openai-responses", provider: "openai", model: "gpt-5",
  usage, stopReason: "stop", timestamp: 1,
});
const entryShape = (entry) => ({
  id: entry.id,
  type: entry.type,
  parentId: entry.parentId,
  seq: entry.seq,
  ...(entry.type === "custom" && { customType: entry.customType, data: entry.data }),
});

const repo = new InMemorySessionRepo();
const session = await repo.create({ id: "source" });
const root = await session.appendEntry(
  { type: "message", id: "root", message: user("root") }, "main",
);
await session.createLane("thread", root.id);
await session.appendEntry(
  { type: "custom", id: "old-note", customType: "note", data: { value: 1 } }, "main",
);
await session.appendEntry({
  type: "compaction", id: "compact", summary: "summary",
  retainedTail: [user("retained", 2), assistant("answer")], tokensBefore: 100,
}, "main");
await session.appendEntry(
  { type: "model_change", id: "model", provider: "openai", modelId: "gpt-5" }, "main",
);
await session.appendEntry(
  { type: "thinking_level_change", id: "thinking", thinkingLevel: "high" }, "main",
);
await session.appendEntry(
  { type: "active_tools_change", id: "tools", activeToolNames: ["read", "bash"] }, "main",
);
await session.appendEntry(
  { type: "custom", id: "new-note", customType: "note", data: { value: 2 } }, "main",
);
await session.appendEntry(
  { type: "message", id: "main-tail", message: user("tail", 3) }, "main",
);
await session.appendEntry(
  { type: "message", id: "thread-tail", message: user("thread", 4) }, "thread",
);
await session.setName("Source");
await session.setLabel("compact", "checkpoint");
await session.appendRecord({
  type: "operation_started", id: "run", lane: "main", sourceLeafId: "main-tail",
  intent: { kind: "run", originalPrompt: [], initialMessages: [] },
});
await session.appendRecord({
  type: "step_attempt", id: "attempt", lane: "main", runId: "run",
  step: "assistant", attempt: 0, resultEntryId: "future-assistant",
});
await session.appendRecord({
  type: "operation_finished", id: "finish", lane: "main", runId: "run",
  outcome: "completed",
});
await session.appendRecord({
  type: "usage", id: "usage", lane: "main", cause: "adjustment", usage,
});
await session.appendRecord({
  type: "operation_started", id: "thread-run", lane: "thread", sourceLeafId: "thread-tail",
  intent: { kind: "navigation", targetId: "root", summarize: false },
});

const mainPath = await session.findEntriesOnBranch({ order: "oldestFirst" });
const context = buildSessionContext(mainPath);
const branchFork = await repo.fork(await session.getMetadata(), {
  scope: "branch", entryId: "main-tail", position: "at", id: "branch-fork",
});
const treeFork = await repo.fork(await session.getMetadata(), {
  scope: "tree", id: "tree-fork",
});

const fixture = {
  upstream: { package: packageJson.name, version: packageJson.version },
  scenario: {
    entries: (await session.findEntries({ order: "oldestFirst" })).map(entryShape),
    lanes: await session.getLanes(),
    log: (await session.getLog()).map((item) => ({ kind: item.kind, seq: item.seq })),
    noteIds: (await session.findEntries({ customType: "note" })).map((entry) => entry.id),
    cursorIds: (await session.findEntries({
      order: "oldestFirst", cursor: { afterSeq: 3 }, limit: 2,
    })).map((entry) => entry.id),
    boundedMessageIds: (await session.findEntriesOnBranch({
      stopAtType: "compaction", type: "message",
    })).map((entry) => entry.id),
    recordIds: (await session.findRecords()).map((record) => record.id),
    runRecordIds: (await session.findRecords({
      runId: "run", order: "oldestFirst", afterSeq: 13,
    })).map((record) => record.id),
    openMainIds: (await session.findOpenOperations("main", { limit: 2 })).map((record) => record.id),
    openThreadIds: (await session.findOpenOperations("thread", { limit: 2 })).map((record) => record.id),
    context: {
      roles: context.messages.map((message) => message.role),
      thinkingLevel: context.thinkingLevel,
      model: context.model,
      activeToolNames: context.activeToolNames,
    },
    stats: await session.getStats(),
    name: await session.getName(),
    label: await session.getLabel("compact"),
    branchFork: {
      entries: (await branchFork.findEntries({ order: "oldestFirst" })).map((entry) => entry.id),
      lanes: await branchFork.getLanes(),
      stats: await branchFork.getStats(),
      metadata: {
        id: (await branchFork.getMetadata()).id,
        parentSessionId: (await branchFork.getMetadata()).parentSessionId,
      },
    },
    treeFork: {
      entries: (await treeFork.findEntries({ order: "oldestFirst" })).map((entry) => entry.id),
      lanes: await treeFork.getLanes(),
      stats: await treeFork.getStats(),
    },
  },
};

const output = process.argv[2] ?? "compat-fixtures/session-memory-0.84.2.json";
await mkdir(dirname(output), { recursive: true });
await writeFile(output, `${JSON.stringify(fixture, null, 2)}\n`);
