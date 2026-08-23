# Operations testing

## Provider live tests

Live Provider tests are disabled by default and never run during ordinary
`test`, `package`, or `verify` lifecycles. Enable them explicitly:

```bash
mvn -Pprovider-live-tests -pl vertx -am verify
```

Each test is skipped unless its credential environment is present. These tests
send a real prompt and may incur Provider charges. Defaults select a small
catalog model; override them when the account does not expose that model.

| Provider | Required environment | Optional model override |
| --- | --- | --- |
| OpenAI | `OPENAI_API_KEY` | `PI_LIVE_OPENAI_MODEL` |
| Anthropic | `ANTHROPIC_API_KEY` | `PI_LIVE_ANTHROPIC_MODEL` |
| Google AI Studio | `GOOGLE_API_KEY` | `PI_LIVE_GOOGLE_MODEL` |
| Google Vertex | `GOOGLE_VERTEX_ACCESS_TOKEN`, `GOOGLE_CLOUD_PROJECT`, `GOOGLE_CLOUD_LOCATION` | `PI_LIVE_VERTEX_MODEL` |
| Mistral | `MISTRAL_API_KEY` | `PI_LIVE_MISTRAL_MODEL` |
| Bedrock | `AWS_BEARER_TOKEN_BEDROCK`, or environment access/secret keys | `PI_LIVE_BEDROCK_MODEL`, `AWS_REGION` |
| Local OpenAI-compatible | `PI_LIVE_OPENAI_COMPAT_BASE_URL`, `PI_LIVE_OPENAI_COMPAT_MODEL` | `PI_LIVE_OPENAI_COMPAT_API_KEY` |

The suite requires a terminal `Done` event with non-empty text and rejects
terminal error events. A skipped run validates profile wiring but is not proof
of service compatibility. CI deliberately supplies no cloud credentials.

## Recovery trend benchmark

Run the bounded recovery scenario on a quiet Linux host:

```bash
PI_BENCH_ITERATIONS=10 tools/benchmark-recovery.sh
```

The script warms the test once and records Surefire elapsed seconds for a
128-generation end-to-end scenario with eight changed generations, scan
batches of 17, and detail pages of three. It writes
`target/recovery-benchmark.txt` by default. The measurement includes repository
creation, checkpoint capture, mutation, and verification, but excludes Maven
startup. It is a trend sample, not a CI pass/fail threshold or a microbenchmark.
Use JFR or a dedicated JMH module before making claims about individual hashing,
locking, or codec operations.

The local reference run on GraalVM JDK 25.0.4 under Linux/WSL2 completed the
scenario in 0.344 seconds. Host, filesystem, antivirus, and storage cache make
cross-machine comparisons invalid.

## Failure and concurrency matrix

| Boundary | Coverage |
| --- | --- |
| Torn JSONL tail | Open repairs a recognized torn tail; malformed committed data reports corruption. |
| Atomic mutation publication | Transaction staging and full-file atomic replacement tests. |
| Cross-JVM writer ownership | Independent process holds the stable generation lock while another commit blocks. |
| Stale generation | Repeated delete/recreate uses fresh paths and fences every stale child process. |
| Unknown provider effect | Recovery creates a later-numbered assistant attempt. |
| SAFE/NEVER tool effect | SAFE takeover and explicit NEVER resolution, including forced-kill recovery. |
| Remote abort | Independent JVM Provider, single tool, and parallel tool cancellation and settlement. |
| Forged advisory | Marker and external notifications require exact durable lane/run records. |
| WatchService unavailable | Injected factory failure records fallback and completes cancellation through polling. |
| Static rejected marker | Change-driven polling avoids repeated writer leases and log reads. |
| Checkpoint mutation race | Independent JVM append/delete/create while immutable manifests are captured and verified. |
| Bounded recovery | 128 generations cross scan and detail page boundaries without count duplication. |
| Verifier interruption | Persisted dual-cursor state resumes without recounting; completed state is terminal. |
| Manifest corruption/path traversal | Structured decoder validates version, root ownership, unique paths, size, hash, and normalized containment. |
| Provider protocol compatibility | The pinned TypeScript 0.84.2 Anthropic, Google, Mistral, and Bedrock implementations run against deterministic mock transports to generate semantic requests, final wire payloads, frames, events, and terminal messages. Java codecs compare against the resulting versioned fixture. |
| Java extension reload | A test compiles a real extension JAR with the JDK compiler, writes its ServiceLoader descriptor without shell tools, then verifies load/start/reload/start/close and exactly-once reverse shutdown. |
| Headless extension orchestration | Resource, input, transition, compaction, model-change, provider middleware, and append-only durable state are tested for ordered chaining, short-circuiting, failure isolation, and session authority. |
| Provider middleware transport | A real local Vert.x server verifies mutated headers and JSON payload on the wire and confirms response middleware runs before stream consumption. |
| Credentialed Provider services | Opt-in Failsafe tests cover OpenAI, Anthropic, Google AI Studio/Vertex, Mistral, Bedrock, and a local OpenAI-compatible endpoint; absent credentials produce explicit skips. |
| Skills portability and bounds | Discovery normalizes ordering across path separators, accepts UTF-8 BOM with LF/CRLF, limits depth/filesystem-entry count/skill size, and gates default project roots on host trust. |

Disk-full and permission-denied behavior is expressed as `SessionError.Code.STORAGE`
by the same persistence-before-publication paths, but deterministic tests for
those operating-system failures require a faulting filesystem provider or a
privileged mount. CI must not emulate them by changing ordinary directory
permissions because that is unreliable on Windows and when tests run with
elevated privileges.

## Platform matrix

GitHub Actions runs `mvn clean verify` with GraalVM JDK 25 on Linux, macOS, and
Windows. Linux additionally checks dependency and `jdeps` boundaries, attached
artifacts, and fixture hashes. Native WatchService behavior is therefore tested
on each hosted filesystem; the injected failure test deterministically covers
the fallback branch on every platform.
