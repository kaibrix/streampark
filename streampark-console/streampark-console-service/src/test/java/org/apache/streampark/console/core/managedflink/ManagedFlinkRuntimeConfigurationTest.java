/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.console.core.managedflink;

import org.apache.streampark.console.core.managedflink.ManagedFlinkLifecycleTest.Fixture;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Command;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CommandType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.FrozenCommand;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobInput;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Restore;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.RestoreMode;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Spec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ManagedFlinkRuntimeConfigurationTest {

  private static final long TEAM = 100000L;
  private static final long APP = 900401L;
  private static final String CHECKPOINT = "execution.checkpointing.interval";
  private static final String SQL_PARAMS = "pipeline.global-job-parameters";
  private static final String TM_ENV = "containerized.taskmanager.env.STREAM_PARK_ACCEPTANCE_TM";
  private static final String JM_ENV = "containerized.master.env.STREAM_PARK_ACCEPTANCE_JM";
  private static final List<String> JAR_ARGS = Arrays.asList("--label", "words with spaces", "中文");

  @Test
  void sqlRuntimeConfigSurvivesSnapshotRestore() {
    Fixture fixture = new Fixture(new MockManagedFlinkProvider());
    Spec spec = sql();
    spec.setConfiguration(sqlRuntimeConfig());
    verifyFrozenRestore(fixture, spec, RestoreMode.FROM_SNAPSHOT);
  }

  @Test
  void sqlLatestRestorePinsTheNewestCompatibleSnapshot() {
    Fixture fixture = new Fixture(new MockManagedFlinkProvider());
    Spec spec = sql();
    spec.setConfiguration(sqlRuntimeConfig());
    verifyFrozenRestore(fixture, spec, RestoreMode.FROM_LATEST);
  }

  @Test
  void jarRuntimeConfigAndArgsSurviveRestore(@TempDir Path directory) throws IOException {
    Fixture fixture = new Fixture(new MockManagedFlinkProvider());
    fixture.sql.update(
        "update t_flink_managed_job set draft_spec=? where app_id=?", "{\"type\":\"JAR\"}", APP);
    ManagedFlinkArtifacts artifacts =
        new ManagedFlinkArtifacts(
            fixture.store, fixture.catalog, fixture.jobs, directory.toString(), 65_536);
    String artifact = artifacts.upload(TEAM, APP, jar());
    Spec spec = new Spec();
    spec.setType(JobType.JAR);
    spec.setArtifactId(artifact);
    spec.setArgs(JAR_ARGS);
    spec.setConfiguration(runtimeConfig());

    FrozenCommand restored = verifyFrozenRestore(fixture, spec, RestoreMode.FROM_SNAPSHOT);
    assertThat(restored.getSpec().getArgs()).containsExactlyElementsOf(JAR_ARGS);
    assertThat(restored.getArtifactRef()).endsWith(".jar");
  }

  @Test
  void invalidResourcesAndUnknownKeysAreRejected() {
    Fixture fixture = new Fixture(new MockManagedFlinkProvider());
    assertBadConfig(fixture, "parallelism.default", "0");
    assertBadConfig(fixture, "taskmanager.numberOfTaskSlots", "0");
    assertBadConfig(fixture, "jobmanager.memory.process.size", "-1m");
    assertBadConfig(fixture, "execution.unapproved.option", "fixture");
    assertBadConfig(fixture, "restart.attempt.enable", "maybe");
    assertBadConfig(fixture, "restart.attempt.max.count", "-1");
    assertBadConfig(fixture, "restart.attempt.interval.min", "2147483648");
  }

  @Test
  void invalidEnvironmentAndControlCharsAreRejected() {
    Fixture fixture = new Fixture(new MockManagedFlinkProvider());
    assertBadConfig(fixture, "containerized.taskmanager.env.1INVALID", "fixture");
    assertBadConfig(fixture, TM_ENV, "first\nsecond");
    assertBadConfig(fixture, JM_ENV, "bad\u0000value");

    Spec jar = new Spec();
    jar.setType(JobType.JAR);
    jar.setArtifactId("does-not-exist");
    jar.setArgs(Collections.singletonList("bad\rargument"));
    assertBadSpec(fixture, jar);
  }

  private FrozenCommand verifyFrozenRestore(Fixture fixture, Spec spec, RestoreMode mode) {
    Map<String, String> configured = new LinkedHashMap<>(spec.getConfiguration());
    fixture.jobs.validateDraft(TEAM, APP, spec);
    edit(fixture, spec, 1L);
    Command publish = fixture.command();
    publish.setDraftRevision(2L);
    String publication = fixture.publish("release-config", publish);
    fixture.complete(publication);
    FrozenCommand releaseCommand = frozen(fixture, publication);
    String release = fixture.jobField("active_release_id");
    assertThat(releaseCommand.getSpec().getConfiguration()).isEqualTo(configured);

    spec.getConfiguration().put(TM_ENV, "draft-only-value");
    if (spec.getType() == JobType.JAR) {
      spec.setArgs(Collections.singletonList("draft-only-argument"));
    } else {
      spec.getConfiguration().put(SQL_PARAMS, "acceptance.run-label:draft-only");
    }
    edit(fixture, spec, 2L);

    String started = submitStart(fixture, "fresh-start", release, RestoreMode.FROM_NEW, null);
    fixture.complete(started);
    assertThat(frozen(fixture, started).getSpec()).isEqualTo(releaseCommand.getSpec());
    String firstRun = fixture.jobField("current_run_id");
    Command snapshot = new Command();
    snapshot.setExpectedRunId(firstRun);
    fixture.complete(fixture.enqueue(CommandType.SNAPSHOT, "snapshot", snapshot));
    if (mode == RestoreMode.FROM_LATEST) {
      fixture.sql.update("update t_flink_managed_snapshot set created_at=1 where app_id=?", APP);
      fixture.complete(fixture.enqueue(CommandType.SNAPSHOT, "newer-snapshot", snapshot));
    }
    String snapshotId =
        fixture.sql.queryForObject(
            "select id from t_flink_managed_snapshot where app_id=? and release_id=? "
                + "order by created_at desc limit 1",
            String.class,
            APP,
            release);
    String restoreRef =
        fixture.sql.queryForObject(
            "select restore_ref from t_flink_managed_snapshot where app_id=? and id=?",
            String.class,
            APP,
            snapshotId);
    assertThat(restoreRef).isNotBlank();

    Command stop = new Command();
    stop.setExpectedRunId(firstRun);
    fixture.complete(fixture.enqueue(CommandType.STOP, "stop", stop));
    String recovered =
        submitStart(
            fixture,
            "snapshot-start",
            release,
            mode,
            mode == RestoreMode.FROM_SNAPSHOT ? snapshotId : null);
    fixture.complete(recovered);
    FrozenCommand restart = frozen(fixture, recovered);
    assertThat(restart.getSpec()).isEqualTo(releaseCommand.getSpec());
    assertThat(restart.getSpecHash()).isEqualTo(releaseCommand.getSpecHash());
    assertThat(restart.getRestoreRef()).isEqualTo(restoreRef);
    assertThat(fixture.jobField("current_run_id")).isNotEqualTo(firstRun);
    Observation result =
        fixture.store.decode(
            fixture.sql.queryForObject(
                "select result from t_flink_managed_operation_step where operation_id=?",
                String.class,
                recovered),
            Observation.class);
    assertThat(result.getRestoredFrom()).isEqualTo(restoreRef);
    return restart;
  }

  private String submitStart(
      Fixture fixture, String key, String release, RestoreMode mode, String snapshot) {
    Restore restore = new Restore();
    restore.setMode(mode);
    restore.setSnapshotId(snapshot);
    Command start = new Command();
    start.setExpectedReleaseId(release);
    start.setRestore(restore);
    return fixture.enqueue(CommandType.START, key, start);
  }

  private FrozenCommand frozen(Fixture fixture, String operation) {
    return fixture.store.decode(
        fixture.sql.queryForObject(
            "select command from t_flink_managed_operation where id=?", String.class, operation),
        FrozenCommand.class);
  }

  private void edit(Fixture fixture, Spec spec, long expectedRevision) {
    JobInput input = new JobInput();
    input.setExpectedDraftRevision(expectedRevision);
    input.setSpec(spec);
    fixture.tx.executeWithoutResult(status -> fixture.jobs.edit(TEAM, APP, input));
  }

  private Map<String, String> runtimeConfig() {
    Map<String, String> config = new LinkedHashMap<>();
    config.put("parallelism.default", "1");
    config.put(CHECKPOINT, "4 s");
    config.put("execution.checkpointing.timeout", "600s");
    config.put("restart.attempt.enable", "true");
    config.put("restart.attempt.max.count", "3");
    config.put("restart.attempt.interval.min", "1");
    config.put(TM_ENV, "fixture");
    config.put(JM_ENV, "fixture-jm");
    return config;
  }

  private Map<String, String> sqlRuntimeConfig() {
    Map<String, String> config = runtimeConfig();
    config.put(SQL_PARAMS, "acceptance.run-label:sql-smoke,acceptance.tenant:租户 beta");
    return config;
  }

  private Spec sql() {
    Spec spec = new Spec();
    spec.setType(JobType.SQL);
    spec.setSql("SELECT 1");
    return spec;
  }

  private void assertBadConfig(Fixture fixture, String key, String value) {
    Spec spec = sql();
    spec.getConfiguration().put(key, value);
    assertBadSpec(fixture, spec);
  }

  private void assertBadSpec(Fixture fixture, Spec spec) {
    assertThatThrownBy(() -> fixture.jobs.validateDraft(TEAM, APP, spec))
        .isInstanceOfSatisfying(
            ResponseStatusException.class,
            error -> assertThat(error.getStatus().value()).isEqualTo(400));
  }

  private MockMultipartFile jar() throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
      zip.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"));
      zip.write(
          "Manifest-Version: 1.0\r\nMain-Class: fixture.Main\r\n\r\n"
              .getBytes(StandardCharsets.UTF_8));
      zip.closeEntry();
    }
    return new MockMultipartFile(
        "file", "runtime-fixture.jar", "application/java-archive", bytes.toByteArray());
  }
}
