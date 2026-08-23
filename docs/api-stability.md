# API stability

The project has two compatibility layers. They must not be conflated.

## SDK scope

`pi-agent` is a headless Java Agent SDK. Native extensions use `AgentExtension`, `ExtensionApi`, `ExtensionRuntime`, and `ExtensionLoader` for host-neutral lifecycle, agent/context/tool middleware, providers, commands, shared events, trusted JAR discovery, ServiceLoader loading, and classloader reload. Session start/shutdown are idempotent, shutdown order is reversed, and a shut-down runtime cannot be restarted. Existing TypeScript extensions are not loaded or executed and must be rewritten against the Java SPI. TUI components, terminal rendering hooks, themes, custom editors, overlays, and TypeScript renderer callbacks are outside the SDK scope. Project trust decisions, command dispatch, and session replacement are host responsibilities. Skills use `SkillRegistry` for Agent Skills frontmatter, trust-aware bounded discovery, lenient validation, progressive disclosure, and explicit invocation.

## Upstream compatibility

The AI message/model contracts, agent loop behavior, session v4 records/codecs,
in-memory repository, JSONL repository basics, compaction, and the public
`AgentHarness` scaffold target pi `0.84.2`. Differential fixtures are the
behavioral authority for this layer. The upstream harness is still a scaffold;
Java durable execution APIs do not imply a compatible upstream executable
harness.

Changes to this layer require regenerated upstream fixtures or a documented bug
fix where the published TypeScript behavior is not a usable contract.

## Experimental session extensions

Types annotated with `ExperimentalSessionApi` are Java session-level extensions
and may evolve before 1.0. This includes durable run/tool/queue execution,
operation recovery inspection, checkpoint orchestration, and advisory abort
notification. These APIs preserve durable wire compatibility but are not part of
the pi `0.84.2` public surface.

The annotation has `CLASS` retention so API tooling can inspect compiled
artifacts without introducing runtime behavior.

## Durable repository administration

Recovery, maintenance, cleanup, checkpoint, cursor, report, and marker
diagnostic types nested under `JsonlSessionRepository` are experimental
administrative APIs. They remain nested for 0.x: moving them now would create a
source break without changing behavior. Cursor records are immutable resume
keys, not repository snapshots.

`RecoveryCheckpointVerifier` composes the independent scan and detail cursors.
Its `State` can be serialized by an operator after each progress callback and
passed to `resume`. Detail delivery is at-least-once: a process failure after a
consumer side effect but before persisting the reported state may cause the last
page to be delivered again. Consumers requiring exactly-once effects must use
an idempotency key of `(relativePath, status, expected fingerprint, current
fingerprint)` or commit their side effect and verifier state atomically.

## Exception and lifecycle contract

Asynchronous validation and storage failures complete the returned
`CompletionStage` exceptionally. Session handles are local lifecycle gates;
closing one handle does not close a separately reopened handle. Stale JSONL
handles fail with `SessionError.Code.STORAGE` and must be reopened. Events,
markers, external notifiers, diagnostics, and progress callbacks are never
durability authority.

## Compatibility policy

- `0.x`: experimental session APIs may change with release notes.
- Upstream-compatible wire records and fixtures remain stable within the stated
  upstream baseline.
- Removal or renaming of a public non-experimental API requires a deprecation
  cycle once a `1.0` baseline exists.
- Binary compatibility automation starts from the first non-snapshot release;
  there is no legitimate previous artifact baseline for `0.1.0-SNAPSHOT`.
