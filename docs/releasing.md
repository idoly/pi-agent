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
mvn --batch-mode -Prelease -Dgpg.skip=true clean verify
tools/verify-reproducible-build.sh
jdeps --multi-release 25 --ignore-missing-deps core/target/pi-agent-core-0.1.0-SNAPSHOT.jar
```

A credential-free deployment-layout smoke test can use a temporary local Maven
repository:

```bash
mvn -DskipTests deploy \
  -DaltDeploymentRepository=release-dry-run::file:/tmp/pi-agent-release
```

It must publish the root POM plus binary, source, Javadoc, and POM artifacts for
each of the three modules. It does not validate signatures or Central Portal
authorization.

The normal build attaches binary, source, and Javadoc JARs. It also compiles
`examples/HeadlessExtensionHost.java` in CI and compares the reviewed public API
text baseline. The fixed
`project.build.outputTimestamp` makes archive timestamps reproducible. CI runs
the full suite on Linux, macOS, and Windows and checks core dependency
boundaries on Linux.

Before the first release, replace `0.1.0-SNAPSHOT` with the release version and
set the SCM tag. Verify that the GitHub repository URL and developer metadata in
the root POM are correct for the publishing account.

Credentialed service checks are a separate, potentially billable gate:

```bash
mvn --batch-mode -Pprovider-live-tests -pl vertx -am verify
```

A provider without configured credentials is skipped, so inspect the Failsafe
summary and require zero skips for the services claimed by a release run.

After `0.1.0` is available from the configured Maven repositories, later
releases compare all three public artifacts with japicmp:

```bash
mvn --batch-mode -Papi-compat \
  -Dpi.api.previousVersion=0.1.0 clean verify
```

The profile excludes `io.github.idoly.pi.vertx.internal`, skips the root POM,
and fails on public binary or source incompatibility. During first-release
preparation its wiring can be tested against a locally installed snapshot:

```bash
mvn -DskipTests install
mvn -Papi-compat -Dpi.api.previousVersion=0.1.0-SNAPSHOT clean verify
```

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
- The checked-in `0.1.0` API text baseline matches generated JAR signatures.
- The `api-compat` profile passes against the selected previously published artifact version.
