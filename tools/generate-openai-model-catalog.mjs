#!/usr/bin/env node

import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const npmRoot = process.env.PI_NPM_ROOT
  ?? "/usr/local/node/lib/node_modules/@earendil-works/pi-coding-agent/node_modules";
const packageRoot = `${npmRoot}/@earendil-works/pi-ai`;
const output = resolve(process.argv[2] ??
  "vertx/src/main/resources/io/github/idoly/pi/vertx/openai/model-catalog-0.84.2.json");
const packageJson = JSON.parse(await readFile(`${packageRoot}/package.json`, "utf8"));
const providers = [
  ["openai", "OPENAI_MODELS"],
  ["openai-codex", "OPENAI_CODEX_MODELS"],
  ["azure-openai-responses", "AZURE_OPENAI_RESPONSES_MODELS"],
];
const entries = [];
for (const [provider, exportName] of providers) {
  const module = await import(pathToFileURL(
    `${packageRoot}/dist/providers/${provider}.models.js`,
  ).href);
  for (const model of Object.values(module[exportName])) {
    entries.push({
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
    });
  }
}
entries.sort((left, right) =>
  left.model.provider.localeCompare(right.model.provider) ||
  left.model.api.localeCompare(right.model.api) ||
  left.model.id.localeCompare(right.model.id));
const payload = {
  schemaVersion: 1,
  upstreamPackage: "@earendil-works/pi-ai",
  upstreamVersion: packageJson.version,
  entries,
};
const content = `${JSON.stringify(payload, null, 2)}\n`;
await mkdir(dirname(output), { recursive: true });
await writeFile(output, content);
console.log(`${entries.length} models ${createHash("sha256").update(content).digest("hex")}`);
