# StreamPark DataBase Guide

## Initialization Guide

Currently, StreamPark supports MySQL and PostgreSQL. The schema directory is the current database schema, it includes mysql and pgsql
schema. The data directory is the current complete data, it includes mysql and pgsql data.

If you use MySQL as the StreamPark database, please execute `mysql-schema.sql` first, then execute `mysql-data.sql` to initialize data.

If you use PostgreSQL as the StreamPark database, please execute `pgsql-schema.sql` first, then execute `pgsql-data.sql` to initialize data.

## Upgrade Guide

When upgrading StreamPark from an old version to a new version, you need to execute some SQL to upgrade the schema and data of the database.

We sorted out the upgrade sql in the upgrade directory, you can choose `mysql` or `pgsql` according to your database. And we have sorted out
the corresponding upgrade SQL for each version of StreamPark. The `version.sql` means: upgrade from the previous version to the current
version, and it includes schema changes and data changes.

For example:

- `1.2.3.sql` needs to be executed when StreamPark is upgraded from `1.2.2` to `1.2.3`.
- `1.2.3.sql` and `2.0.0.sql`  needs to be executed when StreamPark is upgraded from `1.2.2` to `2.0.0`. 

## Managed Flink preview for 2.1.7

For an existing 2.1.7 database, apply `upgrade/mysql/2.1.7.sql` or
`upgrade/pgsql/2.1.7.sql` before upgrading all Console instances. For a fresh
installation, use the matching schema and data scripts above. If you already ran
an earlier 2.1.7 preview, rerun the upgrade script to create the shared TLS
log cursor table. Restart any open log search after upgrading.

For an isolated local Console, set these Spring properties:

| Property | Value |
| --- | --- |
| `streampark.managed-flink.enabled` | `true` to expose the managed API and scheduler |
| `streampark.managed-flink.mock-enabled` | `true` for the local `mock` and `alibaba-fixture` providers |
| `streampark.managed-flink.artifact-directory` | An absolute staging directory writable by the Console. Use a filesystem with hard-link support so concurrent uploads can publish a digest atomically. Multiple Console instances must share the directory. |
| `streampark.managed-flink.credential-key` | A Base64-encoded 32-byte key supplied through deployment secret configuration when using cloud credentials |

Open **Create managed Flink job** from the existing Flink job creation screen.
In managed settings, register a connection and environment. For the mock
provider, use an isolated account reference and placeholder target IDs. Create
and publish a SQL draft, wait for the operation to confirm the release, then
start it explicitly. A JAR job accepts an uploaded JAR or a JAR from a
successful project build in the same team. Save the draft after attaching it.

```bash
./mvnw -Pscala-2.12 -pl streampark-console/streampark-console-service -am \
  -Dtest=ManagedFlinkLifecycleTest,ManagedFlinkArtifactAndReleaseTest,ManagedFlinkControllerTest,ManagedFlinkNativeRoutingTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Mock provider observations live in memory. The operation and release records
persist across Console restarts. A lost mock observation leaves dispatched
operations awaiting verification. The Volcengine adapter probes the bound
account, project, directory and resource pool. Its deployment profile controls
SQL and JAR submission, snapshots, metrics and log access. Check the environment
capabilities in the Console before using each operation.

To enable lifecycle actions with credentials entered by a team administrator, set
`streampark.managed-flink.volcengine.team-credential-lifecycle-enabled` to
`true` and enable the corresponding lifecycle capabilities. The configured
team, account, project, directory, pool and engine must match the environment.
The adapter verifies the cloud account before each write. Configure additional
pools in the same project with
`streampark.managed-flink.volcengine.additional-pools` using comma-separated
`pool-id=pool-name` pairs. Keep the original pool bound while a published
release still uses it.

Use one writer for each managed cloud job. Coordinate cloud-console edits with
StreamPark operations. The public deploy call addresses a draft by ID. Native
restart and savepoint creation address a job by ID. An external change between
the preflight read and the write can change which revision or instance receives
the operation.

Enable `streampark.managed-flink.volcengine.native-stop-with-savepoint-enabled`
after validating native stop with savepoint on the bound test project and engine.
The Console confirms both the target instance's stop and its new available
savepoint. The action and the separate snapshot-then-stop flow both require
`app:cancel` and `savepoint:trigger`. Refresh the instance details to retrieve
its current Flink UI link when Volcengine supplies an approved HTTPS URL.

To import an existing TOS JAR, give the Console profile read access to the
configured artifact bucket and prefix. Import captures the JAR under its
digest. Publication stages that immutable content for the Flink runtime.

If a write loses its response, retain the StreamPark operation ID and use
**Refresh evidence** on that operation. If it confirms an intermediate step,
an operator authorized for the original command can select **Continue**.
For a lost draft creation response, an authorized operator can provide the
cloud draft ID and a cloud audit reference for readback. An ambiguous write
stays in `VERIFY_REQUIRED`. Capture the cloud job and instance history when
investigating it.

Grant `app:log` to team members authorized to read text emitted by the job.
The application controls that text and should keep credentials out of its logs.
