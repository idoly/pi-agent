# Upstream capability matrix

Baseline: `@earendil-works/pi` and `@earendil-works/pi-ai` 0.84.2 at commit `cffe4d776c8fad2b36b4fe6062ebb72c428e0f0f`.

Status meanings:

- `COMPATIBLE`: Java behavior is compared with a versioned upstream oracle.
- `HEADLESS_EQUIVALENT`: Native Java contract provides the same runtime purpose without CLI/TUI types.
- `HOST_ORCHESTRATED`: SDK exposes arbitration or notification APIs; the embedding application owns the workflow.
- `EXPERIMENTAL`: Implemented Java session extension without an upstream stable API guarantee.
- `NON_GOAL`: Intentionally excluded from the headless SDK.
- `BLOCKED_UPSTREAM`: Upstream 0.84.2 remains a scaffold or has no stable executable contract.

## AI and providers

| Capability | Status | Java surface | Verification |
| --- | --- | --- | --- |
| Messages, content, tools, usage, thinking | `COMPATIBLE` | `pi-agent-ai` records | Provider and session fixtures |
| Streaming and cancellation | `HEADLESS_EQUIVALENT` | `ModelStream`, `CancellationSignal`, JDK `Flow` | Backpressure/cancellation tests |
| Provider discovery and refresh | `HEADLESS_EQUIVALENT` | `ModelProvider`, `ProviderRegistry`, `ProviderContext` | Registry tests |
| Provider header middleware | `HEADLESS_EQUIVALENT` | `ProviderRequestHooks.beforeHeaders` | Real Vert.x HTTP test |
| Provider payload middleware | `HEADLESS_EQUIVALENT` | `ProviderRequestHooks.beforeRequest` with JDK JSON values | Real Vert.x HTTP test |
| Provider response middleware | `HEADLESS_EQUIVALENT` | `ProviderRequestHooks.afterResponse` | Awaited before SSE consumption |
| OpenAI Chat/Responses/Codex/Azure | `COMPATIBLE` | Vert.x OpenAI family codecs | Versioned upstream fixtures |
| Anthropic Messages | `COMPATIBLE` | `AnthropicMessagesModelStream` | Upstream protocol fixture |
| Google AI Studio/Vertex | `COMPATIBLE` | `GoogleGenerativeModelStream` | Upstream protocol fixture |
| Mistral Conversations | `COMPATIBLE` | `MistralConversationsModelStream` | Final wire payload fixture |
| Bedrock ConverseStream | `COMPATIBLE` | `BedrockConverseModelStream` | Command/event fixture and CRC tests |
| Live cloud credential flows | `HOST_ORCHESTRATED` | `ProviderAuth`, resolver and interaction SPI | Opt-in `provider-live-tests` profile; credential-gated |
| TUI login menu | `NON_GOAL` | Host supplies interaction UI | Not applicable |

## Agent and sessions

| Capability | Status | Java surface | Verification |
| --- | --- | --- | --- |
| Multi-turn agent loop and tools | `COMPATIBLE` | `Agent`, `AgentLoop` | Agent core fixture and tests |
| Steering and follow-up queues | `COMPATIBLE` | Agent queue modes | Ordered event tests |
| Memory session tree | `COMPATIBLE` | `InMemorySessionRepository` | Upstream memory fixture |
| JSONL v4 | `COMPATIBLE` | `JsonlSessionRepository` | Upstream JSONL fixture |
| Context projection and compaction | `COMPATIBLE` | `SessionContextBuilder`, compaction package | Upstream compaction fixture |
| Durable runs/tools/queues | `EXPERIMENTAL` | Session operation APIs | Cross-JVM and crash recovery tests |
| Remote abort and watches | `EXPERIMENTAL` | JSONL abort notifier and watch APIs | Independent JVM tests |
| Recovery checkpoints | `EXPERIMENTAL` | Checkpoint verifier | Cursor/race/stress tests |
| Repository-wide point-in-time snapshot | `NON_GOAL` | Use filesystem/storage snapshot | Design decision document |
| Executable AgentHarness | `BLOCKED_UPSTREAM` | Java scaffold mirrors upstream | Scaffold fixture |

## Agent Skills

| Capability | Status | Java surface | Verification |
| --- | --- | --- | --- |
| Agent Skills frontmatter | `HEADLESS_EQUIVALENT` | `SkillRegistry`, safe SnakeYAML | Validation tests |
| Global/project/ancestor discovery | `HEADLESS_EQUIVALENT` | `SkillDiscoveryOptions` | Trust and collision tests |
| Package and explicit roots | `HOST_ORCHESTRATED` | `packagePaths`, `explicitPaths` | Discovery tests |
| Extension-contributed skill roots | `HEADLESS_EQUIVALENT` | `ExtensionResources`, `discoverSkills` | Extension integration test |
| Progressive disclosure XML | `HEADLESS_EQUIVALENT` | `systemPromptXml`, `contributeToSystemPrompt` | Escaping/invocation tests |
| `/skill:name` command dispatch | `HEADLESS_EQUIVALENT` | `SkillCommandDispatcher`, `SkillInvocation` | Parser, metadata, provenance, and policy tests |
| Settings/package manifest resolution | `HOST_ORCHESTRATED` | Host resolves paths into discovery options | Full package manager not embedded |
| Skill executable scripts | `HOST_ORCHESTRATED` | Paths are preserved | Host/tool policy owns execution |

## Java extensions

| Capability | Status | Java surface | Verification |
| --- | --- | --- | --- |
| Async initialization and lifecycle | `HEADLESS_EQUIVALENT` | `AgentExtension`, `ExtensionRuntime` | Ordering/idempotency tests |
| Service discovery and reload | `HEADLESS_EQUIVALENT` | `ExtensionLoader` | Real compiled ServiceLoader JAR |
| Resource discovery | `HEADLESS_EQUIVALENT` | `onResourcesDiscover` | Skill contribution test |
| Input transform/handle | `HEADLESS_EQUIVALENT` | `onInput`, `processInput` | Chaining/short-circuit tests |
| Before-agent and context middleware | `HEADLESS_EQUIVALENT` | Existing extension hooks | Runtime tests |
| Tool gates and result middleware | `HEADLESS_EQUIVALENT` | Before/after tool hooks | Fail-safe and patch tests |
| Dynamic tools and active set | `HEADLESS_EQUIVALENT` | `getAllTools`, `setActiveTools` | Existing-agent refresh test |
| Session switch/fork arbitration | `HOST_ORCHESTRATED` | `beforeSessionTransition` | Arbitration tests |
| Compaction customization | `HOST_ORCHESTRATED` | Before/after compaction hooks | Decision tests |
| Model selection notification | `HOST_ORCHESTRATED` | `modelChanged` | Ordered passive hook test |
| Provider request lifecycle | `HEADLESS_EQUIVALENT` | Provider hooks composed into Agent stream | Core and Vert.x tests |
| Durable extension state | `HEADLESS_EQUIVALENT` | `ExtensionStateStore` over custom session entries | History/latest tests |
| Commands | `HOST_ORCHESTRATED` | Registry and handlers | Host dispatches syntax |
| Session replacement execution | `HOST_ORCHESTRATED` | Transition result plus session APIs | Host owns atomic replacement |
| TypeScript source loading | `NON_GOAL` | Rewrite against Java SPI | Not applicable |
| Shortcuts, flags and dialogs | `NON_GOAL` | Application-specific host surface | Not in SDK |
| Themes, renderers, editor and overlays | `NON_GOAL` | TUI-specific | Not in SDK |

## Release gaps

The remaining work before a stable `0.1.0` is operational rather than another module split:

1. Add artifact-to-artifact binary compatibility comparison after publishing `0.1.0`.
2. Run each credentialed live-provider test against an authorized account.
3. Verify Sonatype Central publication with external credentials and signing keys.
