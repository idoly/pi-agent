#!/usr/bin/env node

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";

const npmRoot = process.env.PI_NPM_ROOT
  ?? "/usr/local/node/lib/node_modules/@earendil-works/pi-coding-agent/node_modules";
const agentRoot = `${npmRoot}/@earendil-works/pi-agent-core`;
const { encodeHeader, encodeMutation } = await import(pathToFileURL(
  `${agentRoot}/dist/harness/session/jsonl/codec.js`,
).href);
const packageJson = JSON.parse(await readFile(`${agentRoot}/package.json`, "utf8"));
const usage = {
  input: 10, output: 2, cacheRead: 3, cacheWrite: 1, totalTokens: 13,
  cost: { input: 1, output: 2, cacheRead: 3, cacheWrite: 4, total: 10 },
};
const header = {
  kind: "header", version: 4, id: "oracle", createdAt: 1767225600000,
  cwd: "/workspace", parentSessionId: "parent", metadata: { owner: "test" },
};
const mutations = [
  {
    kind: "entry", lane: "main", entry: {
      id: "message", type: "message", parentId: null, seq: 1,
      timestamp: 1767225600000,
      message: {
        role: "user", content: [{ type: "text", text: "hello" }],
        timestamp: 1,
      },
    },
  },
  { kind: "lane", seq: 2, lane: "thread", leafId: "message" },
  {
    kind: "record", record: {
      type: "operation_started", id: "run", lane: "thread",
      seq: 3, timestamp: 1767225600000, sourceLeafId: "message",
      intent: { kind: "run", originalPrompt: [], initialMessages: [] },
    },
  },
  { kind: "fact", seq: 4, fact: "name", name: "Oracle" },
  {
    kind: "record", record: {
      type: "usage", id: "usage", lane: "thread", seq: 5,
      timestamp: 1767225600000, cause: "adjustment", usage,
      details: { source: "oracle" },
    },
  },
];
const lines = [
  JSON.parse(encodeHeader(header)),
  ...mutations.map((mutation) => JSON.parse(encodeMutation(mutation))),
];
const fixture = {
  upstream: { package: packageJson.name, version: packageJson.version },
  lines,
};
const output = process.argv[2] ?? "compat-fixtures/session-jsonl-0.84.2.json";
await mkdir(dirname(output), { recursive: true });
await writeFile(output, `${JSON.stringify(fixture, null, 2)}\n`);
