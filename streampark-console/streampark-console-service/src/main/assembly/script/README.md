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
installation, use the matching schema and data scripts above.

For an isolated local Console, set these Spring properties:

| Property | Value |
| --- | --- |
| `streampark.managed-flink.enabled` | `true` to expose the managed API and scheduler |
| `streampark.managed-flink.mock-enabled` | `true` for the local `mock` and `alibaba-fixture` providers |
| `streampark.managed-flink.artifact-directory` | An absolute staging directory writable by the Console. Use shared storage for a multi-instance Console. |
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
