#!/usr/bin/env node

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";

const outputPath = process.argv[2] ?? "compat-fixtures/openai-responses-0.84.2.json";
const npmRoot = process.env.PI_NPM_ROOT
  ?? "/usr/local/node/lib/node_modules/@earendil-works/pi-coding-agent/node_modules";
const aiRoot = `${npmRoot}/@earendil-works/pi-ai`;
const [shared, responsesApi] = await Promise.all([
  import(pathToFileURL(`${aiRoot}/dist/api/openai-responses-shared.js`).href),
  import(pathToFileURL(`${aiRoot}/dist/api/openai-responses.js`).href),
]);
const packageJson = JSON.parse(await readFile(`${aiRoot}/package.json`, "utf8"));

const usage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};
const model = {
  id: "gpt-fixture",
  name: "GPT Fixture",
  api: "openai-responses",
  provider: "openai",
  baseUrl: "https://api.openai.com/v1",
  reasoning: true,
  input: ["text", "image"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 128000,
  maxTokens: 1024,
};
const reasoningSignature = JSON.stringify({
  type: "reasoning",
  id: "rs_history",
  summary: [{ type: "summary_text", text: "history thought" }],
  encrypted_content: "history-encrypted",
});
const context = {
  systemPrompt: "Be precise.",
  messages: [
    {
      role: "user",
      content: [
        { type: "text", text: "inspect" },
        { type: "image", data: "aGVsbG8=", mimeType: "image/png" },
      ],
      timestamp: 1,
    },
    {
      role: "assistant",
      content: [
        { type: "thinking", thinking: "history thought", thinkingSignature: reasoningSignature },
        { type: "text", text: "calling" },
        { type: "toolCall", id: "call_1|fc_1", name: "lookup", arguments: { id: 7 } },
      ],
      api: model.api,
      provider: model.provider,
      model: model.id,
      usage,
      stopReason: "toolUse",
      timestamp: 2,
    },
    {
      role: "toolResult",
      toolCallId: "call_1|fc_1",
      toolName: "lookup",
      content: [{ type: "text", text: "" }],
      isError: false,
      timestamp: 3,
    },
  ],
};
const allowedToolCallProviders = new Set(["openai", "openai-codex", "opencode"]);
const requestInput = shared.convertResponsesMessages(model, context, allowedToolCallProviders);
const foreignToolId = "call.bad|foreign/item+==";
const foreignToolInput = shared.convertResponsesMessages(model, {
  messages: [
    {
      role: "assistant",
      content: [{ type: "toolCall", id: foreignToolId, name: "lookup", arguments: { id: 7 } }],
      api: model.api,
      provider: "github-copilot",
      model: model.id,
      usage,
      stopReason: "toolUse",
      timestamp: 1,
    },
    {
      role: "toolResult",
      toolCallId: foreignToolId,
      toolName: "lookup",
      content: [{ type: "text", text: "ok" }],
      isError: false,
      timestamp: 2,
    },
  ],
}, allowedToolCallProviders);

const grammarTool = {
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
};
const grammarProperties = new Map([["sample_tool", "payload"]]);
const grammarDeclaration = shared.convertResponsesTools([grammarTool], {
  supportsOpenAIGrammarTools: true,
})[0];
const grammarReplay = shared.convertResponsesMessages(model, {
  messages: [
    {
      role: "assistant",
      content: [{ type: "toolCall", id: "call_1|ctc_1", name: "sample_tool", arguments: { payload: "abc" } }],
      api: model.api,
      provider: model.provider,
      model: model.id,
      usage,
      stopReason: "toolUse",
      timestamp: 1,
    },
    {
      role: "toolResult",
      toolCallId: "call_1|ctc_1",
      toolName: "sample_tool",
      content: [{ type: "text", text: "done" }],
      isError: false,
      timestamp: 2,
    },
  ],
}, allowedToolCallProviders, { grammarToolInputProperties: grammarProperties });
const grammarEvents = [
  { type: "response.output_item.added", output_index: 0, item: { type: "custom_tool_call", call_id: "call_1", id: "ctc_1", name: "sample_tool", input: "" } },
  { type: "response.custom_tool_call_input.delta", output_index: 0, item_id: "ctc_1", delta: "ab" },
  { type: "response.custom_tool_call_input.done", output_index: 0, item_id: "ctc_1", input: "abc" },
  { type: "response.output_item.done", output_index: 0, item: { type: "custom_tool_call", call_id: "call_1", id: "ctc_1", name: "sample_tool", input: "abc" } },
  { type: "response.completed", response: { status: "completed", usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 } } },
];
const grammarOutput = {
  role: "assistant", content: [], api: model.api, provider: model.provider,
  model: model.id, usage: structuredClone(usage), stopReason: "pending", timestamp: 10,
};
const grammarDeltas = [];
await shared.processResponsesStream((async function* () { yield* grammarEvents; })(), grammarOutput, {
  push(event) { if (event.type === "toolcall_delta") grammarDeltas.push(event.delta); },
}, model, { grammarToolInputProperties: grammarProperties });
const grammar = {
  declaration: grammarDeclaration,
  replay: grammarReplay,
  frames: grammarEvents,
  deltas: grammarDeltas,
  content: grammarOutput.content.map((block) => ({
    type: "toolCall", id: block.id, name: block.name, arguments: block.arguments,
  })),
  stopReason: grammarOutput.stopReason,
};

const responseEvents = [
  { type: "response.output_item.added", output_index: 0, item: { type: "reasoning", id: "rs_1", summary: [] } },
  { type: "response.reasoning_summary_text.delta", output_index: 0, delta: "live thought" },
  { type: "response.output_item.done", output_index: 0, item: { type: "reasoning", id: "rs_1", summary: [{ type: "summary_text", text: "final thought" }] } },
  { type: "response.output_item.added", output_index: 1, item: { type: "message", id: "msg_1", role: "assistant", status: "in_progress", content: [] } },
  { type: "response.output_text.delta", output_index: 1, delta: "hel" },
  { type: "response.output_item.done", output_index: 1, item: { type: "message", id: "msg_1", role: "assistant", status: "completed", content: [{ type: "output_text", text: "hello", annotations: [] }] } },
  { type: "response.output_item.added", output_index: 2, item: { type: "function_call", id: "fc_2", call_id: "call_2", name: "lookup", arguments: "" } },
  { type: "response.function_call_arguments.delta", output_index: 2, delta: "{\"id\":" },
  { type: "response.function_call_arguments.done", output_index: 2, arguments: "{\"id\":9}" },
  { type: "response.output_item.done", output_index: 2, item: { type: "function_call", id: "fc_2", call_id: "call_2", name: "lookup", arguments: "{\"id\":9}" } },
  {
    type: "response.completed",
    response: {
      id: "resp_1",
      status: "completed",
      output: [{ type: "reasoning", id: "rs_1", summary: [], encrypted_content: "terminal-encrypted" }],
      usage: {
        input_tokens: 8,
        output_tokens: 3,
        total_tokens: 11,
        input_tokens_details: { cached_tokens: 2, cache_write_tokens: 1 },
        output_tokens_details: { reasoning_tokens: 1 },
      },
    },
  },
];
async function* responseStream() {
  yield* responseEvents;
}
const output = {
  role: "assistant",
  content: [],
  api: model.api,
  provider: model.provider,
  model: model.id,
  usage: structuredClone(usage),
  stopReason: "pending",
  timestamp: 10,
};
const events = [];
await shared.processResponsesStream(responseStream(), output, {
  push(event) {
    const normalized = { type: event.type, contentIndex: event.contentIndex };
    if ("delta" in event) normalized.delta = event.delta;
    events.push(normalized);
  },
}, model);

function normalizeContent(block) {
  if (block.type === "thinking") {
    return { type: "thinking", thinking: block.thinking, signature: JSON.parse(block.thinkingSignature) };
  }
  if (block.type === "text") return { type: "text", text: block.text };
  return { type: "toolCall", id: block.id, name: block.name, arguments: block.arguments };
}
async function terminalScenario(frames) {
  const sse = `${frames.map((frame) => `data: ${JSON.stringify(frame)}`).join("\n\n")}\n\n`;
  const resultStream = responsesApi.stream(model, {
    messages: [{ role: "user", content: "test", timestamp: 1 }],
  }, {
    apiKey: "fixture-key",
    fetch: async () => new Response(sse, {
      status: 200,
      headers: { "content-type": "text/event-stream" },
    }),
  });
  const eventTypes = [];
  for await (const event of resultStream) eventTypes.push(event.type);
  const result = await resultStream.result();
  return {
    frames,
    events: eventTypes,
    message: {
      stopReason: result.stopReason,
      errorMessage: result.errorMessage,
      responseId: result.responseId,
      rawStopReason: result.rawStopReason,
    },
  };
}
const terminalScenarios = {
  earlyEof: await terminalScenario([
    { type: "response.created", response: { id: "resp_early" } },
  ]),
  contentFilter: await terminalScenario([
    { type: "response.incomplete", response: {
      id: "resp_filter",
      status: "incomplete",
      incomplete_details: { reason: "content_filter" },
    } },
  ]),
  failed: await terminalScenario([
    { type: "response.failed", response: {
      id: "resp_failed",
      status: "failed",
      error: { code: "server_error", message: "boom" },
    } },
  ]),
};

const fixture = {
  upstream: { package: packageJson.name, version: packageJson.version },
  requestInput,
  foreignToolInput,
  grammar,
  terminalScenarios,
  stream: {
    frames: responseEvents,
    events,
    message: {
      content: output.content.map(normalizeContent),
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
