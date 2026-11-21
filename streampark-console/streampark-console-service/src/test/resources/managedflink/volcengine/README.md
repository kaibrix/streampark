<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Volcengine managed Flink acceptance jobs

Run each streaming case against `FLINK_VERSION_1_16`, `FLINK_VERSION_1_17`, and
`FLINK_VERSION_1_20` in an authorized test directory and pool.

Configure the Console with a deployment-owned profile scoped to the same
team, cloud account, project, directory, and pool.

```yaml
streampark:
  managed-flink:
    enabled: true
    artifact-directory: /path/to/local/artifacts
    volcengine:
      profile: default
      team-id: 100000
      account: YOUR_CLOUD_ACCOUNT_ID
      project-id: YOUR_FLINK_PROJECT_ID
      directory-id: YOUR_FLINK_DIRECTORY_ID
      pool-id: YOUR_RESOURCE_POOL_ID
      pool-name: YOUR_RESOURCE_POOL_NAME
      engine: FLINK_VERSION_1_16,FLINK_VERSION_1_17,FLINK_VERSION_1_20
      sql-lifecycle-enabled: true
      snapshot-lifecycle-enabled: true
      artifact-lifecycle-enabled: true
      artifact-bucket: YOUR_TEST_BUCKET
      artifact-prefix: YOUR_TEST_PREFIX
```

Create a Volcengine connection for the configured account with empty AK and
SK fields. Use the exact same IDs and artifact target in the managed
environment. The artifact prefix should grant the Console upload access and
the Flink runtime read access.

Build the fixtures with a Java compiler that supports `--release 8`. Use the
Flink table-common JAR for the UDF. Use the Flink core, runtime, and streaming
Java JARs for the standalone job.

```sh
mkdir -p /tmp/managed-flink-fixture/udf /tmp/managed-flink-fixture/job
javac --release 8 -cp "$FLINK_TABLE_COMMON" -d /tmp/managed-flink-fixture/udf LabelFunction.java
javac --release 8 -cp "$FLINK_STREAMING_CLASSPATH" -d /tmp/managed-flink-fixture/job StatefulSmoke.java
jar cf /tmp/managed-flink-fixture/label-udf.jar -C /tmp/managed-flink-fixture/udf .
jar cfe /tmp/managed-flink-fixture/stateful-smoke.jar \
  org.apache.streampark.acceptance.StatefulSmoke -C /tmp/managed-flink-fixture/job .
```

| Case | Draft | Expected runtime evidence |
| --- | --- | --- |
| SQL | Use `plain-sql.sql` | New instance prints `sql-v1`. |
| SQL with UDF | Use `stateful-udf.sql` and attach `label-udf.jar` as a SQL dependency | TaskManager prints `udf-jar-verified:<count>:tm=tm-smoke`. |
| JAR | Upload `stateful-smoke.jar` and set the main class from its manifest | TaskManager prints `jar-v1 [alpha beta, 中文, --limit=7] jm=jm-smoke tm=tm-smoke offset=<count>`. |

Set these runtime options before publishing the SQL with UDF and JAR cases.

```text
execution.checkpointing.interval=4 s
containerized.master.env.STREAM_PARK_ACCEPTANCE_JM=jm-smoke
containerized.taskmanager.env.STREAM_PARK_ACCEPTANCE_TM=tm-smoke
```

For the JAR, enter `alpha beta`, `中文`, and `--limit=7` in the Console
as three separate argument lines. Set `args.base64.enabled=true` and
`containerized.master.env.STREAM_PARK_ACCEPTANCE_ARGS_BASE64=true`. The
fixture main class decodes one Base64 token into a UTF-8 argument vector
whose elements are separated by NUL. Applications using this mode need to
decode the vector before parsing their arguments.

For each stateful case, publish and verify the scoped draft, job and
executable revision before starting. In the new instance, verify the engine,
configured checkpoint interval, and expected output. After the count
reaches 15, create a named savepoint. Verify the savepoint is `AVAILABLE`,
stop that instance, and start with that exact savepoint ID. Verify the resumed
counter exceeds 15 and the same job configuration and output markers remain.
Stop the restored test instance. A restore with an unavailable savepoint must
leave the job stopped.

`volc-flink draft get`, `job get`, `job instance get`,
`job instance logs`, and `savepoint list` provide independent evidence
for these checks. Use the same test profile, project, and region for every
command. Run one test instance at a time when the pool has limited capacity.
