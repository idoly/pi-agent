#!/usr/bin/env node

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";

const outputPath = process.argv[2] ?? "compat-fixtures/openai-chat-0.84.2.json";
const npmRoot = process.env.PI_NPM_ROOT
  ?? "/usr/local/node/lib/node_modules/@earendil-works/pi-coding-agent/node_modules";
const aiRoot = `${npmRoot}/@earendil-works/pi-ai`;
const [{ stream }, packageJson] = await Promise.all([
  import(pathToFileURL(`${aiRoot}/dist/api/openai-completions.js`).href),
  readFile(`${aiRoot}/package.json`, "utf8").then(JSON.parse),
]);

const model = {
  id: "chat-fixture",
  name: "Chat Fixture",
  api: "openai-completions",
  provider: "openai",
  baseUrl: "https://api.openai.com/v1",
  reasoning: true,
  input: ["text"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 128000,
  maxTokens: 1024,
  compat: {
    supportsDeveloperRole: true,
    supportsReasoningEffort: true,
    supportsUsageInStreaming: true,
    maxTokensField: "max_completion_tokens",
    thinkingFormat: "openai",
  },
};
const context = {
  systemPrompt: "Be precise.",
  messages: [{ role: "user", content: "calculate", timestamp: 1 }],
  tools: [{
    name: "lookup",
    description: "Lookup",
    parameters: { type: "object", properties: { id: { type: "integer" } }, required: ["id"] },
  }],
};
const chunks = [
  { id: "chatcmpl-1", model: model.id, choices: [{ index: 0, delta: { role: "assistant", reasoning_content: "think" }, finish_reason: null }] },
  { id: "chatcmpl-1", model: model.id, choices: [{ index: 0, delta: { content: "hello" }, finish_reason: null }] },
  { id: "chatcmpl-1", model: model.id, choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: "call_1", type: "function", function: { name: "lookup", arguments: "{\"id\":" } }] }, finish_reason: null }] },
  { id: "chatcmpl-1", model: model.id, choices: [{ index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: "7}" } }] }, finish_reason: null }] },
  { id: "chatcmpl-1", model: model.id, choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] },
  { id: "chatcmpl-1", model: model.id, choices: [], usage: { prompt_tokens: 10, completion_tokens: 4, total_tokens: 14, prompt_tokens_details: { cached_tokens: 3 }, completion_tokens_details: { reasoning_tokens: 2 } } },
];
const sse = `${chunks.map((chunk) => `data: ${JSON.stringify(chunk)}`).join("\n\n")}\n\ndata: [DONE]\n\n`;
let payload;
const assistantStream = stream(model, context, {
  apiKey: "fixture-key",
  reasoningEffort: "high",
  maxTokens: model.maxTokens,
  onPayload(value) {
    payload = value;
  },
  fetch: async () => new Response(sse, {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  }),
});
const events = [];
for await (const event of assistantStream) {
  const normalized = { type: event.type };
  if ("contentIndex" in event) normalized.contentIndex = event.contentIndex;
  if ("delta" in event) normalized.delta = event.delta;
  events.push(normalized);
}
const output = await assistantStream.result();
async function terminalScenario(scenarioChunks, includeDone) {
  const body = `${scenarioChunks.map((chunk) => `data: ${JSON.stringify(chunk)}`).join("\n\n")}\n\n${
    includeDone ? "data: [DONE]\n\n" : ""
  }`;
  const resultStream = stream(model, {
    messages: [{ role: "user", content: "test", timestamp: 1 }],
  }, {
    apiKey: "fixture-key",
    fetch: async () => new Response(body, {
      status: 200,
      headers: { "content-type": "text/event-stream" },
    }),
  });
  const eventTypes = [];
  for await (const event of resultStream) eventTypes.push(event.type);
  const result = await resultStream.result();
  return {
    chunks: scenarioChunks,
    includeDone,
    events: eventTypes,
    message: {
      stopReason: result.stopReason,
      errorMessage: result.errorMessage,
      responseId: result.responseId,
      rawStopReason: result.rawStopReason,
    },
  };
}
async function capturePayload(captureContext, captureModel = model) {
  let captured;
  const captureStream = stream(captureModel, captureContext, {
    apiKey: "fixture-key",
    onPayload(value) { captured = value; },
    fetch: async () => new Response(
      `data: ${JSON.stringify({ id: "chatcmpl-capture", model: captureModel.id, choices: [{ index: 0, delta: { content: "ok" }, finish_reason: "stop" }] })}\n\ndata: [DONE]\n\n`,
      { status: 200, headers: { "content-type": "text/event-stream" } },
    ),
  });
  await captureStream.result();
  return captured;
}
const constrainedPayload = await capturePayload({
  messages: [{ role: "user", content: "edit", timestamp: 1 }],
  tools: [{
    name: "edit",
    description: "Edit",
    parameters: {
      type: "object",
      properties: { path: { type: "string" }, offset: { type: "integer" } },
      required: ["path"],
    },
    constrainedSampling: { type: "json_schema", strict: "prefer" },
  }],
});

const grammarModel = {
  ...model,
  compat: { ...model.compat, supportsOpenAIGrammarTools: true },
};
const grammarPayload = await capturePayload({
  messages: [{ role: "user", content: "sample", timestamp: 1 }],
  tools: [{
    name: "sample_tool",
    description: "Sample tool",
    parameters: {
      type: "object",
      properties: { payload: { type: "string" } },
      required: ["payload"],
      additionalProperties: false,
    },
    constrainedSampling: {
      type: "grammar",
      variants: { openai_lark: "start: /[a-z]+/" },
    },
  }],
}, grammarModel);

const terminalScenarios = {
  earlyEof: await terminalScenario([
    { id: "chatcmpl-early", model: model.id, choices: [{ index: 0, delta: { content: "partial" }, finish_reason: null }] },
  ], false),
  contentFilter: await terminalScenario([
    { id: "chatcmpl-filter", model: model.id, choices: [{ index: 0, delta: { content: "blocked" }, finish_reason: null }] },
    { id: "chatcmpl-filter", model: model.id, choices: [{ index: 0, delta: {}, finish_reason: "content_filter" }] },
  ], true),
};

const fixture = {
  upstream: { package: packageJson.name, version: packageJson.version },
  request: {
    model: payload.model,
    messages: payload.messages,
    tools: payload.tools,
    stream: payload.stream,
    stream_options: payload.stream_options,
    max_completion_tokens: payload.max_completion_tokens,
    reasoning_effort: payload.reasoning_effort,
  },
  constrainedTool: constrainedPayload.tools[0],
  grammarTool: grammarPayload.tools[0],
  terminalScenarios,
  stream: {
    chunks,
    events,
    message: {
      content: output.content.map((block) => block.type === "toolCall"
        ? { type: "toolCall", id: block.id, name: block.name, arguments: block.arguments }
        : block.type === "thinking"
          ? { type: "thinking", thinking: block.thinking }
          : { type: "text", text: block.text }),
      stopReason: output.stopReason,
      responseId: output.responseId,
      rawStopReason: output.rawStopReason,
      usage: {
        input: output.usage.input,
        output: output.usage.output,
        cacheRead: output.usage.cacheRead,
        cacheWrite: output.usage.cacheWrite,
        reasoning: output.usage.reasoning,
        totalTokens: output.usage.totalTokens,
      },
    },
  },
};
await mkdir(dirname(outputPath), { recursive: true });
await writeFile(outputPath, `${JSON.stringify(fixture, null, 2)}\n`);
