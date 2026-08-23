#!/usr/bin/env node

import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const npmRoot = process.env.PI_NPM_ROOT
  ?? "/usr/local/node/lib/node_modules/@earendil-works/pi-coding-agent/node_modules";
const packageRoot = `${npmRoot}/@earendil-works/pi-ai`;
const output = resolve(process.argv[2] ??
  "vertx/src/main/resources/io/github/idoly/pi/vertx/provider-model-catalog-0.84.2.json");
const packageJson = JSON.parse(await readFile(`${packageRoot}/package.json`, "utf8"));
const all = await import(pathToFileURL(`${packageRoot}/dist/providers/all.js`).href);
const providers = [
  "openai",
  "openai-codex",
  "azure-openai-responses",
  "anthropic",
  "google",
  "google-vertex",
  "amazon-bedrock",
  "mistral",
];
const entries = providers.flatMap(provider =>
  all.getBuiltinModels(provider).map(model => ({
    model: {
      id: model.id,
      name: model.name,
      api: model.api,
      provider: model.provider,
      baseUrl: model.baseUrl,
      reasoning: model.reasoning,
      input: model.input,
      contextWindow: model.contextWindow,
      maxTokens: model.maxTokens,
      ...(model.thinkingLevelMap && { thinkingLevelMap: model.thinkingLevelMap }),
    },
    cost: model.cost ?? {},
    compat: model.compat ?? {},
  })),
);
entries.sort((left, right) =>
  left.model.provider.localeCompare(right.model.provider)
  || left.model.api.localeCompare(right.model.api)
  || left.model.id.localeCompare(right.model.id));
const payload = {
  schemaVersion: 1,
  upstreamPackage: "@earendil-works/pi-ai",
  upstreamVersion: packageJson.version,
  providers,
  entries,
};
const content = `${JSON.stringify(payload, null, 2)}\n`;
await mkdir(dirname(output), { recursive: true });
await writeFile(output, content);
console.log(`${entries.length} models ${createHash("sha256").update(content).digest("hex")}`);
