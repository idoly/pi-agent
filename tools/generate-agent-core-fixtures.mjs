#!/usr/bin/env node

import { readFile, writeFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";

const npmRoot = process.env.PI_NPM_ROOT
  ?? "/usr/local/node/lib/node_modules/@earendil-works/pi-coding-agent/node_modules";
const agentRoot = `${npmRoot}/@earendil-works/pi-agent-core`;
const aiRoot = `${npmRoot}/@earendil-works/pi-ai`;
const typeboxRoot = `${npmRoot}/typebox`;

const [{ Agent, runAgentLoop, runAgentLoopContinue }, { EventStream }, { Type }] = await Promise.all([
  import(pathToFileURL(`${agentRoot}/dist/index.js`).href),
  import(pathToFileURL(`${aiRoot}/dist/compat.js`).href),
  import(pathToFileURL(`${typeboxRoot}/build/index.mjs`).href),
]);
const packageJson = JSON.parse(await readFile(`${agentRoot}/package.json`, "utf8"));

class MockAssistantStream extends EventStream {
  constructor() {
    super(
      (event) => event.type === "done" || event.type === "error",
      (event) => event.type === "done" ? event.message : event.error,
    );
  }
}

const usage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};
const model = {
  id: "fixture-model",
  name: "Fixture Model",
  api: "openai-responses",
  provider: "fixture",
  baseUrl: "https://example.invalid",
  reasoning: false,
  input: ["text"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 8192,
  maxTokens: 2048,
};

function assistant(content, stopReason) {
  return {
    role: "assistant",
    content,
    api: model.api,
    provider: model.provider,
    model: model.id,
    usage,
    stopReason,
    timestamp: 1,
  };
}

function mockStream(producer) {
  const stream = new MockAssistantStream();
  queueMicrotask(() => producer(stream));
  return stream;
}

function normalizeEvent(event) {
  const normalized = { type: event.type };
  if ("message" in event && event.message?.role) normalized.role = event.message.role;
  if ("toolName" in event) normalized.toolName = event.toolName;
  if ("toolCallId" in event) normalized.toolCallId = event.toolCallId;
  if ("isError" in event) normalized.isError = event.isError;
  if (event.type === "message_update") {
    normalized.deltaType = event.assistantMessageEvent.type;
  }
  if (event.type === "turn_end") {
    normalized.toolResults = event.toolResults.map((result) => result.toolName);
  }
  return normalized;
}

function normalizeMessage(message) {
  const normalized = { role: message.role };
  if (message.role === "user") {
    normalized.text = typeof message.content === "string"
      ? message.content
      : message.content.filter((item) => item.type === "text").map((item) => item.text).join("\n");
  } else if (message.role === "assistant") {
    normalized.stopReason = message.stopReason;
    normalized.content = message.content.map((block) => {
      if (block.type === "text") return { type: "text", text: block.text };
      if (block.type === "thinking") return { type: "thinking", thinking: block.thinking };
      return { type: "toolCall", id: block.id, name: block.name, arguments: block.arguments };
    });
  } else if (message.role === "toolResult") {
    normalized.toolCallId = message.toolCallId;
    normalized.toolName = message.toolName;
    normalized.isError = message.isError;
    normalized.text = message.content
      .filter((item) => item.type === "text")
      .map((item) => item.text)
      .join("\n");
  }
  return normalized;
}

async function runTextScenario() {
  const events = [];
  const streamFn = () => mockStream((stream) => {
    const partial = assistant([], "pending");
    stream.push({ type: "start", partial });
    partial.content.push({ type: "text", text: "" });
    stream.push({ type: "text_start", contentIndex: 0, partial });
    partial.content[0].text = "hello";
    stream.push({ type: "text_delta", contentIndex: 0, delta: "hello", partial });
    stream.push({ type: "text_end", contentIndex: 0, content: "hello", partial });
    const complete = assistant([{ type: "text", text: "hello" }], "stop");
    stream.push({ type: "done", reason: "stop", message: complete });
    stream.end();
  });
  const agent = new Agent({ initialState: { model }, streamFn });
  agent.subscribe((event) => events.push(normalizeEvent(event)));
  await agent.prompt("hi");
  return { events, messages: agent.state.messages.map(normalizeMessage) };
}

async function runBlockedToolScenario() {
  const events = [];
  let turn = 0;
  const streamFn = () => mockStream((stream) => {
    const message = turn++ === 0
      ? assistant([{ type: "toolCall", id: "call-blocked", name: "dangerous", arguments: {} }], "toolUse")
      : assistant([{ type: "text", text: "continued" }], "stop");
    stream.push({ type: "done", reason: message.stopReason, message });
    stream.end();
  });
  const tool = {
    name: "dangerous",
    label: "dangerous",
    description: "dangerous",
    parameters: Type.Object({}),
    async execute() {
      throw new Error("blocked tool must not execute");
    },
  };
  const agent = new Agent({
    initialState: { model, tools: [tool] },
    streamFn,
    beforeToolCall: async () => ({ block: true, reason: "Blocked by policy" }),
  });
  agent.subscribe((event) => events.push(normalizeEvent(event)));
  await agent.prompt("blocked");
  return { events, messages: agent.state.messages.map(normalizeMessage) };
}

async function runLengthTruncatedToolScenario() {
  const events = [];
  let executions = 0;
  let turn = 0;
  const streamFn = () => mockStream((stream) => {
    const message = turn++ === 0
      ? assistant([{ type: "toolCall", id: "call-length", name: "echo", arguments: { value: "hel" } }], "length")
      : assistant([{ type: "text", text: "recovered" }], "stop");
    stream.push({ type: "done", reason: message.stopReason, message });
    stream.end();
  });
  const tool = {
    name: "echo",
    label: "echo",
    description: "echo",
    parameters: Type.Object({ value: Type.String() }),
    async execute() {
      executions++;
      return { content: [{ type: "text", text: "must not execute" }], details: {} };
    },
  };
  const agent = new Agent({ initialState: { model, tools: [tool] }, streamFn });
  agent.subscribe((event) => events.push(normalizeEvent(event)));
  await agent.prompt("truncate");
  return { events, messages: agent.state.messages.map(normalizeMessage), executions };
}

async function runPrepareNextTurnScenario() {
  const events = [];
  const providerCalls = [];
  let turn = 0;
  const nextModel = { ...model, id: "next-model", name: "Next Model", reasoning: true };
  const streamFn = (activeModel, context, options) => mockStream((stream) => {
    providerCalls.push({
      model: activeModel.id,
      systemPrompt: context.systemPrompt,
      thinkingLevel: options?.reasoning ?? "off",
      userTexts: context.messages
        .filter((message) => message.role === "user")
        .map((message) => typeof message.content === "string"
          ? message.content
          : message.content.filter((item) => item.type === "text").map((item) => item.text).join("\n")),
    });
    const message = turn++ === 0
      ? assistant([{ type: "toolCall", id: "call-prepare", name: "prepare", arguments: {} }], "toolUse")
      : { ...assistant([{ type: "text", text: "prepared" }], "stop"), model: nextModel.id };
    stream.push({ type: "done", reason: message.stopReason, message });
    stream.end();
  });
  const tool = {
    name: "prepare",
    label: "prepare",
    description: "prepare",
    parameters: Type.Object({}),
    async execute() {
      return { content: [{ type: "text", text: "prepared tool" }], details: {} };
    },
  };
  let prepared = false;
  const agent = new Agent({
    initialState: { model, systemPrompt: "first prompt", tools: [tool] },
    streamFn,
    prepareNextTurnWithContext: async ({ context }) => {
      if (prepared) return undefined;
      prepared = true;
      return {
        context: {
          systemPrompt: "second prompt",
          messages: [
            ...context.messages,
            { role: "user", content: [{ type: "text", text: "prepared context" }], timestamp: 4 },
          ],
          tools: context.tools,
        },
        model: nextModel,
        thinkingLevel: "high",
      };
    },
  });
  agent.subscribe((event) => events.push(normalizeEvent(event)));
  await agent.prompt("start");
  return { events, messages: agent.state.messages.map(normalizeMessage), providerCalls };
}

async function runFollowUpQueueScenario(mode) {
  const events = [];
  const invocationContexts = [];
  let turn = 0;
  const streamFn = (_model, context) => mockStream((stream) => {
    invocationContexts.push(context.messages
      .filter((message) => message.role === "user")
      .map((message) => typeof message.content === "string"
        ? message.content
        : message.content.filter((item) => item.type === "text").map((item) => item.text).join("\n")));
    const message = assistant([{ type: "text", text: `turn ${++turn}` }], "stop");
    stream.push({ type: "done", reason: "stop", message });
    stream.end();
  });
  const agent = new Agent({ initialState: { model }, streamFn, followUpMode: mode });
  agent.followUp({ role: "user", content: [{ type: "text", text: "follow one" }], timestamp: 2 });
  agent.followUp({ role: "user", content: [{ type: "text", text: "follow two" }], timestamp: 3 });
  agent.subscribe((event) => events.push(normalizeEvent(event)));
  await agent.prompt("start");
  return {
    events,
    messages: agent.state.messages.map(normalizeMessage),
    invocationContexts,
  };
}

async function runAfterToolTerminationScenario() {
  const events = [];
  let invocations = 0;
  const streamFn = () => mockStream((stream) => {
    invocations++;
    const message = assistant([
      { type: "toolCall", id: "call-terminate", name: "terminator", arguments: {} },
    ], "toolUse");
    stream.push({ type: "done", reason: "toolUse", message });
    stream.end();
  });
  const tool = {
    name: "terminator",
    label: "terminator",
    description: "terminator",
    parameters: Type.Object({}),
    async execute() {
      return { content: [{ type: "text", text: "terminated" }], details: {} };
    },
  };
  const agent = new Agent({
    initialState: { model, tools: [tool] },
    streamFn,
    afterToolCall: async () => ({ terminate: true }),
  });
  agent.subscribe((event) => events.push(normalizeEvent(event)));
  await agent.prompt("terminate");
  return { events, messages: agent.state.messages.map(normalizeMessage), invocations };
}

async function runSteeringScenario() {
  const events = [];
  let turn = 0;
  const streamFn = () => mockStream((stream) => {
    const message = turn++ === 0
      ? assistant([{ type: "toolCall", id: "call-steer", name: "steerer", arguments: {} }], "toolUse")
      : assistant([{ type: "text", text: "redirected" }], "stop");
    stream.push({ type: "done", reason: message.stopReason, message });
    stream.end();
  });
  let agent;
  const tool = {
    name: "steerer",
    label: "steerer",
    description: "steerer",
    parameters: Type.Object({}),
    async execute() {
      agent.steer({ role: "user", content: [{ type: "text", text: "change direction" }], timestamp: 2 });
      return { content: [{ type: "text", text: "tool result" }], details: {} };
    },
  };
  agent = new Agent({ initialState: { model, tools: [tool] }, streamFn });
  agent.subscribe((event) => events.push(normalizeEvent(event)));
  await agent.prompt("start");
  return { events, messages: agent.state.messages.map(normalizeMessage) };
}

async function runAbortScenario() {
  const events = [];
  const streamFn = (_model, _context, options) => mockStream((stream) => {
    const partial = assistant([], "pending");
    stream.push({ type: "start", partial });
    options.signal.addEventListener("abort", () => {
      const error = { ...assistant([], "aborted"), errorMessage: "Request was aborted" };
      stream.push({ type: "error", reason: "aborted", error });
      stream.end();
    }, { once: true });
  });
  const agent = new Agent({ initialState: { model }, streamFn });
  agent.subscribe((event) => {
    events.push(normalizeEvent(event));
    if (event.type === "message_start" && event.message.role === "assistant") agent.abort();
  });
  await agent.prompt("abort me");
  return { events, messages: agent.state.messages.map(normalizeMessage) };
}

async function runProviderErrorScenario() {
  const events = [];
  const streamFn = () => mockStream((stream) => {
    const error = { ...assistant([], "error"), errorMessage: "provider failed" };
    stream.push({ type: "error", reason: "error", error });
    stream.end();
  });
  const agent = new Agent({ initialState: { model }, streamFn });
  agent.subscribe((event) => events.push(normalizeEvent(event)));
  await agent.prompt("fail");
  return { events, messages: agent.state.messages.map(normalizeMessage) };
}

async function runHookFailureScenario(kind) {
  const events = [];
  let turn = 0;
  let executions = 0;
  const streamFn = () => mockStream((stream) => {
    const message = turn++ === 0
      ? assistant([{ type: "toolCall", id: `call-${kind}`, name: "hooked", arguments: {} }], "toolUse")
      : assistant([{ type: "text", text: "recovered" }], "stop");
    stream.push({ type: "done", reason: message.stopReason, message });
    stream.end();
  });
  const tool = {
    name: "hooked",
    label: "hooked",
    description: "hooked",
    parameters: Type.Object({}),
    async execute() {
      executions++;
      return { content: [{ type: "text", text: "executed" }], details: {} };
    },
  };
  const options = {
    initialState: { model, tools: [tool] },
    streamFn,
    ...(kind === "before"
      ? { beforeToolCall() { throw new Error("before failed"); } }
      : { afterToolCall() { throw new Error("after failed"); } }),
  };
  const agent = new Agent(options);
  agent.subscribe((event) => events.push(normalizeEvent(event)));
  await agent.prompt(kind);
  return {
    result: normalizeMessage(agent.state.messages.find((message) => message.role === "toolResult")),
    executions,
    invocations: turn,
    events,
  };
}

async function runLowLevelPromptScenario() {
  const events = [];
  const providerContexts = [];
  let turn = 0;
  let steeringCalls = 0;
  let followUpCalls = 0;
  const streamFn = (_model, context) => mockStream((stream) => {
    providerContexts.push(context.messages
      .filter((message) => message.role === "user")
      .map((message) => typeof message.content === "string"
        ? message.content
        : message.content.filter((item) => item.type === "text").map((item) => item.text).join("\n")));
    const message = assistant([{ type: "text", text: `low-level ${++turn}` }], "stop");
    stream.push({ type: "done", reason: "stop", message });
    stream.end();
  });
  const messages = await runAgentLoop(
    [{ role: "user", content: [{ type: "text", text: "prompt" }], timestamp: 1 }],
    { systemPrompt: "system", messages: [], tools: [] },
    {
      model,
      convertToLlm: (messages) => messages,
      getSteeringMessages: async () => steeringCalls++ === 0
        ? [{ role: "user", content: [{ type: "text", text: "initial steering" }], timestamp: 2 }]
        : [],
      getFollowUpMessages: async () => followUpCalls++ === 0
        ? [{ role: "user", content: [{ type: "text", text: "follow up" }], timestamp: 3 }]
        : [],
    },
    (event) => events.push(normalizeEvent(event)),
    undefined,
    streamFn,
  );
  return {
    events,
    messages: messages.map(normalizeMessage),
    providerContexts,
    steeringCalls,
    followUpCalls,
  };
}

async function runLowLevelContinueScenario() {
  const events = [];
  const existing = { role: "user", content: [{ type: "text", text: "existing" }], timestamp: 1 };
  const streamFn = () => mockStream((stream) => {
    const message = assistant([{ type: "text", text: "continued" }], "stop");
    stream.push({ type: "done", reason: "stop", message });
    stream.end();
  });
  const messages = await runAgentLoopContinue(
    { systemPrompt: "", messages: [existing], tools: [] },
    { model, convertToLlm: (messages) => messages },
    (event) => events.push(normalizeEvent(event)),
    undefined,
    streamFn,
  );
  return { events, messages: messages.map(normalizeMessage) };
}

async function runParallelToolScenario() {
  const events = [];
  let turn = 0;
  const streamFn = () => mockStream((stream) => {
    if (turn++ === 0) {
      const message = assistant([
        { type: "toolCall", id: "call-a", name: "first", arguments: {} },
        { type: "toolCall", id: "call-b", name: "second", arguments: {} },
      ], "toolUse");
      stream.push({ type: "done", reason: "toolUse", message });
    } else {
      const message = assistant([{ type: "text", text: "finished" }], "stop");
      stream.push({ type: "done", reason: "stop", message });
    }
    stream.end();
  });
  const schema = Type.Object({});
  const tool = (name, delay) => ({
    name,
    label: name,
    description: name,
    parameters: schema,
    async execute() {
      await new Promise((resolve) => setTimeout(resolve, delay));
      return { content: [{ type: "text", text: `${name} result` }], details: {} };
    },
  });
  const agent = new Agent({
    initialState: { model, tools: [tool("first", 10), tool("second", 0)] },
    streamFn,
  });
  agent.subscribe((event) => events.push(normalizeEvent(event)));
  await agent.prompt("tools");
  return { events, messages: agent.state.messages.map(normalizeMessage) };
}

const fixture = {
  upstream: {
    package: packageJson.name,
    version: packageJson.version,
  },
  scenarios: {
    streamingText: await runTextScenario(),
    parallelTools: await runParallelToolScenario(),
    blockedTool: await runBlockedToolScenario(),
    lengthTruncatedTool: await runLengthTruncatedToolScenario(),
    prepareNextTurn: await runPrepareNextTurnScenario(),
    followUpAll: await runFollowUpQueueScenario("all"),
    followUpOneAtATime: await runFollowUpQueueScenario("one-at-a-time"),
    afterToolTermination: await runAfterToolTerminationScenario(),
    steering: await runSteeringScenario(),
    abort: await runAbortScenario(),
    providerError: await runProviderErrorScenario(),
    lowLevelPrompt: await runLowLevelPromptScenario(),
    lowLevelContinue: await runLowLevelContinueScenario(),
    beforeHookFailure: await runHookFailureScenario("before"),
    afterHookFailure: await runHookFailureScenario("after"),
  },
};

const output = process.argv[2];
const json = `${JSON.stringify(fixture, null, 2)}\n`;
if (output) {
  await mkdir(dirname(output), { recursive: true });
  await writeFile(output, json);
} else {
  process.stdout.write(json);
}
