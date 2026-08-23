#!/usr/bin/env node

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";

const npmRoot = process.env.PI_NPM_ROOT
  ?? "/usr/local/node/lib/node_modules/@earendil-works/pi-coding-agent/node_modules";
const agentRoot = `${npmRoot}/@earendil-works/pi-agent-core`;
const compaction = await import(pathToFileURL(
  `${agentRoot}/dist/harness/compaction/compaction.js`,
).href);
const branch = await import(pathToFileURL(
  `${agentRoot}/dist/harness/compaction/branch-summarization.js`,
).href);
const utils = await import(pathToFileURL(
  `${agentRoot}/dist/harness/compaction/utils.js`,
).href);
const { buildSessionContext } = await import(pathToFileURL(
  `${agentRoot}/dist/harness/session/context.js`,
).href);
const packageJson = JSON.parse(await readFile(`${agentRoot}/package.json`, "utf8"));
const usage = (input, output, cacheRead = 0, cacheWrite = 0) => ({
  input, output, cacheRead, cacheWrite,
  totalTokens: input + output + cacheRead + cacheWrite,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
});
const user = (text, timestamp = 1) => ({
  role: "user", content: [{ type: "text", text }], timestamp,
});
const assistant = (content, value = usage(100, 50), timestamp = 1) => ({
  role: "assistant", content, api: "openai-responses", provider: "openai",
  model: "gpt-5", usage: value, stopReason: "stop", timestamp,
});
const messageEntry = (id, parentId, message, seq) => ({
  type: "message", id, parentId, seq, timestamp: 1, message,
});

const estimateMessages = [
  user("12345678"),
  assistant([
    { type: "thinking", thinking: "1234" },
    { type: "toolCall", id: "call", name: "read", arguments: { path: "a.ts" } },
  ], usage(10, 5, 3, 2)),
  {
    role: "toolResult", toolCallId: "call", toolName: "read",
    content: [{ type: "text", text: "ok" }, { type: "image", data: "abc", mimeType: "image/png" }],
    isError: false, timestamp: 1,
  },
];
const cutEntries = [
  messageEntry("u1", null, user("first request"), 1),
  messageEntry("a1", "u1", assistant([{ type: "text", text: "first answer" }]), 2),
  messageEntry("tr1", "a1", {
    role: "toolResult", toolCallId: "call", toolName: "read",
    content: [{ type: "text", text: "result" }], isError: false, timestamp: 1,
  }, 3),
  messageEntry("u2", "tr1", user("second request"), 4),
  messageEntry("a2", "u2", assistant([{ type: "text", text: "second answer" }]), 5),
];
const retainedUser = user("retained user");
const retainedAssistant = assistant([
  { type: "toolCall", id: "write", name: "write", arguments: { path: "written.ts" } },
]);
const previous = {
  type: "compaction", id: "compact", parentId: null, seq: 1, timestamp: 1,
  summary: "previous summary", retainedTail: [retainedUser, retainedAssistant],
  tokensBefore: 1000,
  details: { readFiles: ["old-read.ts"], modifiedFiles: ["old-edit.ts"] },
};
const newUser = messageEntry("new-user", "compact", user("new user"), 2);
const newAssistant = messageEntry(
  "new-assistant", "new-user",
  assistant([{ type: "text", text: "new assistant response" }], usage(200, 50)), 3,
);
const preparationResult = compaction.prepareCompaction(
  [previous, newUser, newAssistant],
  { enabled: true, reserveTokens: 100, keepRecentTokens: 1 },
);
if (!preparationResult.ok || !preparationResult.value) throw new Error("Expected preparation");
const preparation = preparationResult.value;
const lists = utils.computeFileLists(preparation.fileOps);
const branchPreparation = branch.prepareBranchEntries(cutEntries, 8);

const fixture = {
  upstream: { package: packageJson.name, version: packageJson.version },
  estimates: {
    messageTokens: estimateMessages.map(compaction.estimateTokens),
    context: compaction.estimateContextTokens(estimateMessages),
    shouldCompact: [
      compaction.shouldCompact(95, 100, { enabled: true, reserveTokens: 10, keepRecentTokens: 20 }),
      compaction.shouldCompact(90, 100, { enabled: true, reserveTokens: 10, keepRecentTokens: 20 }),
    ],
  },
  cutPoint: compaction.findCutPoint(cutEntries, 0, cutEntries.length, 1),
  preparation: {
    previousSummary: preparation.previousSummary,
    isSplitTurn: preparation.isSplitTurn,
    tokensBefore: preparation.tokensBefore,
    messagesToSummarize: preparation.messagesToSummarize.map((message) => message.role),
    turnPrefixMessages: preparation.turnPrefixMessages.map((message) => message.role),
    retainedTail: preparation.retainedTail.map((message) => message.role),
    fileLists: lists,
    contextRoles: buildSessionContext([previous, newUser, newAssistant]).messages.map((message) => message.role),
  },
  branchPreparation: {
    roles: branchPreparation.messages.map((message) => message.role),
    totalTokens: branchPreparation.totalTokens,
  },
  serialized: utils.serializeConversation(estimateMessages),
  formattedFiles: utils.formatFileOperations(["a.ts"], ["b.ts"]),
};
const output = process.argv[2] ?? "compat-fixtures/compaction-0.84.2.json";
await mkdir(dirname(output), { recursive: true });
await writeFile(output, `${JSON.stringify(fixture, null, 2)}\n`);
