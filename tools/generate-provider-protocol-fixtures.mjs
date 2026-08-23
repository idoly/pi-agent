#!/usr/bin/env node

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

const outputPath = process.argv[2] ?? "compat-fixtures/provider-protocols-0.84.2.json";
const npmRoot = process.env.PI_NPM_ROOT
  ?? "/usr/local/node/lib/node_modules/@earendil-works/pi-coding-agent/node_modules";
const aiRoot = `${npmRoot}/@earendil-works/pi-ai`;
const load = (path) => import(pathToFileURL(`${aiRoot}/dist/api/${path}.js`).href);
const [anthropic, google, mistral, bedrock, packageJson] = await Promise.all([
  load("anthropic-messages"), load("google-shared"),
  load("mistral-conversations"), load("bedrock-converse-stream"),
  readFile(`${aiRoot}/package.json`, "utf8").then(JSON.parse),
]);

const zeroUsage = {
  input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};
const tool = {
  name: "lookup", description: "Lookup",
  parameters: { type: "object", properties: { q: { type: "string" } }, required: ["q"] },
};
const secondTool = {
  name: "fetch", description: "Fetch",
  parameters: { type: "object", properties: { url: { type: "string" } }, required: ["url"] },
};
function normalizeEvent(event) {
  const value = { type: event.type };
  if ("contentIndex" in event) value.contentIndex = event.contentIndex;
  if ("delta" in event) value.delta = event.delta;
  return value;
}
function normalizeMessage(message) {
  return {
    content: message.content.map((block) => block.type === "thinking"
      ? { type: "thinking", text: block.thinking, signature: block.thinkingSignature }
      : block.type === "text"
        ? { type: "text", text: block.text, signature: block.textSignature }
        : { type: "toolCall", id: block.id, name: block.name, arguments: block.arguments,
          signature: block.thoughtSignature }),
    stopReason: message.stopReason,
    errorMessage: message.errorMessage,
    responseId: message.responseId,
    rawStopReason: message.rawStopReason,
    usage: {
      input: message.usage.input, output: message.usage.output,
      cacheRead: message.usage.cacheRead, cacheWrite: message.usage.cacheWrite,
      reasoning: message.usage.reasoning, totalTokens: message.usage.totalTokens,
    },
  };
}
async function consume(stream) {
  const events = [];
  for await (const event of stream) events.push(normalizeEvent(event));
  return { events, message: normalizeMessage(await stream.result()) };
}

const anthropicModel = {
  id: "claude-fixture", name: "Claude Fixture", api: "anthropic-messages",
  provider: "anthropic", baseUrl: "https://api.anthropic.com", reasoning: true,
  input: ["text", "image"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 200000, maxTokens: 16384,
  compat: { supportsLongCacheRetention: true, supportsEagerToolInputStreaming: true },
};
const anthropicContext = {
  systemPrompt: "system",
  messages: [
    { role: "user", content: "hello", timestamp: 1 },
    { role: "assistant", api: anthropicModel.api, provider: anthropicModel.provider,
      model: anthropicModel.id, usage: zeroUsage, stopReason: "toolUse", timestamp: 2,
      content: [
        { type: "thinking", thinking: "old thought", thinkingSignature: "old-signature" },
        { type: "toolCall", id: "old-call", name: "lookup", arguments: { q: "old" } },
      ] },
    { role: "toolResult", toolCallId: "old-call", toolName: "lookup",
      content: [{ type: "text", text: "old result" }], isError: false, timestamp: 3 },
  ],
  tools: [tool],
};
const anthropicFrames = [
  ["message_start", { type: "message_start", message: { id: "msg-fixture", role: "assistant", content: [], model: anthropicModel.id, stop_reason: null, stop_sequence: null, usage: { input_tokens: 10, cache_read_input_tokens: 3, cache_creation_input_tokens: 2, output_tokens: 0 } } }],
  ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "thinking", thinking: "", signature: "" } }],
  ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "thinking_delta", thinking: "why" } }],
  ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "signature_delta", signature: "new-signature" } }],
  ["content_block_stop", { type: "content_block_stop", index: 0 }],
  ["content_block_start", { type: "content_block_start", index: 1, content_block: { type: "tool_use", id: "call-fixture", name: "lookup", input: {} } }],
  ["content_block_delta", { type: "content_block_delta", index: 1, delta: { type: "input_json_delta", partial_json: "{\"q\":\"x\"}" } }],
  ["content_block_stop", { type: "content_block_stop", index: 1 }],
  ["message_delta", { type: "message_delta", delta: { stop_reason: "tool_use", stop_sequence: null }, usage: { output_tokens: 7 } }],
  ["message_stop", { type: "message_stop" }],
];
let anthropicPayload;
const anthropicSse = `${anthropicFrames.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}`).join("\n\n")}\n\n`;
const anthropicResult = await consume(anthropic.streamSimple(anthropicModel, anthropicContext, {
  apiKey: "fixture-key", maxTokens: anthropicModel.maxTokens,
  reasoning: "medium", onPayload(value) { anthropicPayload = value; },
  fetch: async () => new Response(anthropicSse, { status: 200, headers: { "content-type": "text/event-stream" } }),
}));
const anthropicErrorPayload = {
  type: "error", error: { type: "overloaded_error", message: "busy" },
};
const anthropicErrorSse = `event: error\ndata: ${JSON.stringify(anthropicErrorPayload)}\n\n`;
const anthropicErrorResult = await consume(anthropic.streamSimple(
  anthropicModel,
  { systemPrompt: "", messages: [{ role: "user", content: "hello", timestamp: 1 }], tools: [] },
  {
    apiKey: "fixture-key", maxTokens: anthropicModel.maxTokens,
    fetch: async () => new Response(anthropicErrorSse, { status: 200, headers: { "content-type": "text/event-stream" } }),
  },
));
let anthropicCachePlacementPayload;
await consume(anthropic.streamSimple(
  anthropicModel,
  {
    systemPrompt: "system",
    messages: [
      { role: "user", content: "hello", timestamp: 1 },
      { role: "assistant", api: anthropicModel.api, provider: anthropicModel.provider,
        model: anthropicModel.id, usage: zeroUsage, stopReason: "toolUse", timestamp: 2,
        content: [
          { type: "toolCall", id: "call-lookup", name: "lookup", arguments: { q: "x" } },
          { type: "toolCall", id: "call-fetch", name: "fetch", arguments: { url: "https://example.test" } },
        ] },
      { role: "toolResult", toolCallId: "call-lookup", toolName: "lookup",
        content: [{ type: "text", text: "lookup result" }], isError: false, timestamp: 3 },
      { role: "toolResult", toolCallId: "call-fetch", toolName: "fetch",
        content: [{ type: "text", text: "fetch result" }], isError: false, timestamp: 4 },
    ],
    tools: [tool, secondTool],
  },
  {
    apiKey: "fixture-key", maxTokens: anthropicModel.maxTokens, reasoning: "medium",
    onPayload(value) { anthropicCachePlacementPayload = value; },
    fetch: async () => new Response(anthropicErrorSse, { status: 200, headers: { "content-type": "text/event-stream" } }),
  },
));

const googleModel = {
  id: "gemini-3-pro", name: "Gemini Fixture", api: "google-generative-ai",
  provider: "google", baseUrl: "https://generativelanguage.googleapis.com/v1beta",
  reasoning: true, input: ["text", "image"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 1000000, maxTokens: 64000,
};
const googleContext = {
  systemPrompt: "system",
  messages: [
    { role: "user", content: [{ type: "text", text: "hello" }, { type: "image", data: "aGVsbG8=", mimeType: "image/png" }], timestamp: 1 },
    { role: "assistant", api: googleModel.api, provider: googleModel.provider,
      model: googleModel.id, usage: zeroUsage, stopReason: "toolUse", timestamp: 2,
      content: [
        { type: "thinking", thinking: "why", thinkingSignature: "c2ln" },
        { type: "text", text: "answer", textSignature: "dGV4dA==" },
        { type: "toolCall", id: "call.id", name: "lookup", arguments: { q: "x" }, thoughtSignature: "dG9vbA==" },
      ] },
    { role: "toolResult", toolCallId: "call.id", toolName: "lookup",
      content: [{ type: "text", text: "result" }], isError: false, timestamp: 3 },
  ], tools: [tool],
};
const googleFixture = {
  contents: google.convertMessages(googleModel, googleContext),
  tools: google.convertTools([tool]),
  finishReasons: Object.fromEntries(["STOP", "MAX_TOKENS", "SAFETY", "MALFORMED_FUNCTION_CALL"].map((reason) => [reason, google.mapStopReasonString(reason)])),
};

const mistralModel = {
  id: "mistral-fixture", name: "Mistral Fixture", api: "mistral-conversations",
  provider: "mistral", baseUrl: "https://api.mistral.ai", reasoning: true,
  input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 128000, maxTokens: 32000,
};
const mistralContext = {
  systemPrompt: "system",
  messages: [
    { role: "user", content: "hello", timestamp: 1 },
    { role: "assistant", api: "openai-responses", provider: "openai",
      model: "foreign-model", usage: zeroUsage, stopReason: "toolUse", timestamp: 2,
      content: [{ type: "toolCall", id: "call.with-invalid/id", name: "lookup", arguments: { q: "x" } }] },
    { role: "toolResult", toolCallId: "call.with-invalid/id", toolName: "lookup",
      content: [{ type: "text", text: "result" }], isError: false, timestamp: 3 },
  ], tools: [tool],
};
const mistralChunks = [
  { id: "mistral-response", choices: [{ index: 0, delta: { content: [{ type: "thinking", thinking: [{ type: "text", text: "reason" }] }, { type: "text", text: "answer" }] }, finish_reason: null }] },
  { id: "mistral-response", choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: "Abc123xyz", type: "function", function: { name: "lookup", arguments: "{\"q\":\"x\"}" } }] }, finish_reason: null }] },
  { id: "mistral-response", choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }], usage: { prompt_tokens: 8, completion_tokens: 4, total_tokens: 12 } },
];
let mistralPayload;
let mistralWirePayload;
const mistralSse = `${mistralChunks.map((chunk) => `data: ${JSON.stringify(chunk)}`).join("\n\n")}\n\ndata: [DONE]\n\n`;
const mistralResult = await consume(mistral.streamSimple(mistralModel, mistralContext, {
  apiKey: "fixture-key", reasoning: "high", sessionId: "session",
  onPayload(value) { mistralPayload = value; },
  fetch: async (url, init) => {
    mistralWirePayload = JSON.parse(init.body);
    return new Response(mistralSse, { status: 200, headers: { "content-type": "text/event-stream" } });
  },
}));
let mistralReasoningEffortRequest;
await consume(mistral.streamSimple({
  ...mistralModel,
  id: "mistral-small-2603",
  thinkingLevelMap: { low: "low" },
}, { systemPrompt: "", messages: [{ role: "user", content: "hello", timestamp: 1 }], tools: [] }, {
  apiKey: "fixture-key", reasoning: "low",
  fetch: async (url, init) => {
    mistralReasoningEffortRequest = JSON.parse(init.body);
    return new Response(mistralSse, { status: 200, headers: { "content-type": "text/event-stream" } });
  },
}));

const require = createRequire(`${aiRoot}/dist/api/bedrock-converse-stream.js`);
const aws = require("@aws-sdk/client-bedrock-runtime");
const originalSend = aws.BedrockRuntimeClient.prototype.send;
let bedrockPayload;
const bedrockFrames = [
  { messageStart: { role: "assistant" } },
  { contentBlockStart: { contentBlockIndex: 0, start: { toolUse: { toolUseId: "bedrock-call", name: "lookup" } } } },
  { contentBlockDelta: { contentBlockIndex: 0, delta: { toolUse: { input: "{\"q\":\"x\"}" } } } },
  { contentBlockStop: { contentBlockIndex: 0 } },
  { messageStop: { stopReason: "tool_use" } },
  { metadata: { usage: { inputTokens: 9, outputTokens: 5, cacheReadInputTokens: 2, cacheWriteInputTokens: 1, totalTokens: 14 } } },
];
aws.BedrockRuntimeClient.prototype.send = async function(command) {
  bedrockPayload = command.input;
  return { $metadata: { requestId: "bedrock-response", httpStatusCode: 200 }, stream: (async function* () { yield* bedrockFrames; })() };
};
const bedrockModel = {
  id: "anthropic.claude-fixture", name: "Bedrock Fixture", api: "bedrock-converse-stream",
  provider: "amazon-bedrock", baseUrl: "https://bedrock-runtime.us-east-1.amazonaws.com",
  reasoning: true, input: ["text"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 200000, maxTokens: 16000,
};
let bedrockResult;
let bedrockErrorResult;
try {
  bedrockResult = await consume(bedrock.stream(bedrockModel, {
    systemPrompt: "system", messages: [{ role: "user", content: "hello", timestamp: 1 }], tools: [tool],
  }, { bearerToken: "fixture-token", maxTokens: bedrockModel.maxTokens, reasoning: "medium" }));
  aws.BedrockRuntimeClient.prototype.send = async function() {
    const error = new aws.ThrottlingException({
      message: "slow down", $metadata: {},
    });
    return {
      $metadata: { requestId: "bedrock-error", httpStatusCode: 200 },
      stream: (async function* () { yield { throttlingException: error }; })(),
    };
  };
  bedrockErrorResult = await consume(bedrock.stream(bedrockModel, {
    systemPrompt: "", messages: [{ role: "user", content: "hello", timestamp: 1 }], tools: [],
  }, { bearerToken: "fixture-token", maxTokens: bedrockModel.maxTokens }));
} finally {
  aws.BedrockRuntimeClient.prototype.send = originalSend;
}

const fixture = {
  upstream: { package: packageJson.name, version: packageJson.version },
  anthropic: {
    request: anthropicPayload,
    cachePlacementRequest: anthropicCachePlacementPayload,
    frames: anthropicFrames.map(([, data]) => data),
    ...anthropicResult,
    streamError: { payload: anthropicErrorPayload, ...anthropicErrorResult },
  },
  google: googleFixture,
  mistral: {
    request: mistralPayload, wireRequest: mistralWirePayload,
    reasoningEffortRequest: mistralReasoningEffortRequest,
    chunks: mistralChunks, ...mistralResult,
  },
  bedrock: {
    request: bedrockPayload, frames: bedrockFrames, ...bedrockResult,
    modeledError: {
      eventType: "throttlingException", payload: { message: "slow down" },
      ...bedrockErrorResult,
    },
  },
};
await mkdir(dirname(outputPath), { recursive: true });
await writeFile(outputPath, `${JSON.stringify(fixture, null, 2)}\n`);
