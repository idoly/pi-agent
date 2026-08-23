# 0.1.0 release notes

Initial Java 25 port targeting pi `0.84.2` at upstream commit
`cffe4d776c8fad2b36b4fe6062ebb72c428e0f0f`.

## Modules

- `io.github.idoly:pi-agent-ai`
- `io.github.idoly:pi-agent-core`
- `io.github.idoly:pi-agent-vertx`

## Compatibility

The release includes differential fixtures for agent core, the harness
scaffold, memory and JSONL sessions, compaction, record logs, OpenAI Chat,
OpenAI Responses, and the model catalog. The public upstream `AgentHarness` in
pi `0.84.2` is still a scaffold; this release does not claim an upstream
executable harness contract.

## Java extensions

The headless SDK includes native Java extension lifecycle/tool/context/provider/command middleware, trusted JAR discovery and reload, Agent Skills discovery with progressive disclosure, host-orchestrated resource/input/session-transition/compaction/model events, awaited provider request/response middleware, and append-only durable extension state. Extension session lifecycle is idempotent with reverse shutdown, and a real compiled ServiceLoader JAR covers reload behavior. Skill discovery accepts UTF-8 BOM and Windows line endings and is bounded by depth, filesystem-entry count, and per-file size. TypeScript loading and TUI rendering are intentionally excluded.

Durable run, tool, queue, navigation, compaction, recovery, checkpoint,
maintenance, remote abort, and cleanup APIs are Java session-level extensions.
They use the upstream-compatible JSONL v4 mutation format but are marked or
documented as experimental before 1.0.

Polling-based abort marker observation debounces filesystem stamps across two observations so one truncate/write transition cannot cause duplicate durable verification; JSONL records remain the only abort authority.

Checkpoint administration supports immutable per-generation inventories,
full and bounded verification, independent scan/detail cursors, resumable
orchestration, and cross-JVM mutation diagnostics. It does not claim a globally
simultaneous repository snapshot.

## Runtime boundaries

Credential-gated Failsafe smoke tests cover OpenAI, Anthropic, Google AI Studio/Vertex, Mistral, Bedrock, and local OpenAI-compatible endpoints without running in the default lifecycle. A compile-checked headless host example demonstrates extension command dispatch, `/skill:name` invocation metadata and permissions, session replacement arbitration, and compaction hooks. The reviewed Java 25 `javap` declaration set is checked in as the `0.1.0` public API baseline and verified by CI; a japicmp profile is ready for artifact comparison after publication. Release tooling also verifies repeatable binary/source/Javadoc JAR hashes.

Core exposes JDK `Flow.Publisher` streaming and has no Vert.x, Mutiny, or Netty
runtime dependency. Vert.x 5.1.6 and SmallRye Mutiny 3.3.0 are isolated in
`pi-agent-vertx`. OpenAI, Anthropic, Google/Vertex, Mistral, and Bedrock protocols use the Vert.x transport directly without official provider SDKs, OkHttp, or Kotlin runtime. The bundled generated catalog contains 276 models.

## Requirements

- JDK 25
- Maven 3.9 or newer for source builds
