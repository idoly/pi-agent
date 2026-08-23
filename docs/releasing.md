# Releasing

## Prerequisites

- GraalVM or another JDK 25 distribution
- Maven 3.9 or newer
- A Sonatype Central Portal account authorized for `io.github.idoly`
- A GPG signing key available to `maven-gpg-plugin`
- Maven server credentials named `central`

## Verify

```bash
mvn --batch-mode clean verify
jdeps --multi-release 25 --ignore-missing-deps core/target/pi-agent-core-0.1.0-SNAPSHOT.jar
```

The normal build attaches binary, source, and Javadoc JARs. The fixed
`project.build.outputTimestamp` makes archive timestamps reproducible. CI runs
the full suite on Linux, macOS, and Windows and checks core dependency
boundaries on Linux.

Before the first release, replace `0.1.0-SNAPSHOT` with the release version and
set the SCM tag. Verify that the GitHub repository URL and developer metadata in
the root POM are correct for the publishing account.

## Sign and stage

Configure credentials outside the repository:

```xml
<server>
  <id>central</id>
  <username>${env.CENTRAL_USERNAME}</username>
  <password>${env.CENTRAL_PASSWORD}</password>
</server>
```

Then run:

```bash
mvn --batch-mode -Prelease clean deploy
```

The `release` profile signs all attached artifacts and uploads a Central bundle
with automatic publication disabled. Inspect and publish the deployment in the
Central Portal. The project does not commit signing keys or credentials.

## Release checks

- All upstream fixture hashes match the reviewed baseline.
- `AgentHarness` release notes state that upstream `0.84.2` exposes a scaffold.
- Java durable run/tool/queue and administration APIs are identified as
  experimental session-level extensions.
- No `com.openai`, OkHttp, Kotlin runtime, Vert.x, Mutiny, or Netty types leak
  into the core boundary where prohibited.
- Binary compatibility comparison starts after publishing the first baseline.
