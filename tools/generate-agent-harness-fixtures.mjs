#!/usr/bin/env node

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";

const npmRoot = process.env.PI_NPM_ROOT
  ?? "/usr/local/node/lib/node_modules/@earendil-works/pi-coding-agent/node_modules";
const agentRoot = `${npmRoot}/@earendil-works/pi-agent-core`;
const aiRoot = `${npmRoot}/@earendil-works/pi-ai`;
const [{ AgentHarness }, { InMemorySessionStorage, Session }, { getModel }] = await Promise.all([
  import(pathToFileURL(`${agentRoot}/dist/harness/agent-harness.js`).href),
  import(pathToFileURL(`${agentRoot}/dist/harness/session/index.js`).href),
  import(pathToFileURL(`${aiRoot}/dist/compat.js`).href),
]);
const packageJson = JSON.parse(await readFile(`${agentRoot}/package.json`, "utf8"));
const createSession = (id) => new Session(new InMemorySessionStorage({ id, createdAt: 1 }));
const model = getModel("openai", "gpt-5.2");
const capture = async (operation) => {
  try {
    await operation();
    return { resolved: true };
  } catch (error) {
    return { name: error.name, operation: error.operation, message: error.message };
  }
};

const session = createSession("empty");
const { harness, suspended } = await AgentHarness.create({ session, models: {}, model });
const defaults = {
  name: harness.name,
  leafId: await harness.getLeafId(),
  suspended,
  thinkingLevel: await harness.getThinkingLevel(),
  activeToolNames: await harness.getActiveTools(),
  retryPolicy: await harness.getRetryPolicy(),
  compactionSettings: await harness.getCompactionSettings(),
  steeringMode: await harness.getSteeringMode(),
  followUpMode: await harness.getFollowUpMode(),
};
const unavailable = {};
for (const [name, operation] of [
  ["prompt", () => harness.prompt("hello")],
  ["compact", () => harness.compact()],
  ["resume", () => harness.resume()],
  ["waitForIdle", () => harness.waitForIdle()],
  ["watchSession", () => harness.watchSession()],
]) unavailable[name] = await capture(operation);

const recorded = createSession("recorded");
await recorded.appendRecord({
  type: "operation_started", id: "run", lane: "main", sourceLeafId: null,
  intent: { kind: "run", originalPrompt: [], initialMessages: [] },
});
const restore = await capture(() => AgentHarness.create({ session: recorded, models: {}, model }));
await harness.close();
const closed = {
  prompt: await capture(() => harness.prompt("hello")),
  waitForIdle: await capture(() => harness.waitForIdle()),
};

const fixture = {
  upstream: { package: packageJson.name, version: packageJson.version },
  defaults,
  unavailable,
  restore,
  closed,
};
const output = process.argv[2] ?? "compat-fixtures/agent-harness-0.84.2.json";
await mkdir(dirname(output), { recursive: true });
await writeFile(output, `${JSON.stringify(fixture, null, 2)}\n`);
