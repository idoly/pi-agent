# Operations testing

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
