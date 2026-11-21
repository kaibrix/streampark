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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobInput;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Restore;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.RestoreMode;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Spec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockMultipartFile;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class ManagedFlinkArtifactAndReleaseTest {

  private static final long APP = 900401L;
  private static final long TEAM = 100000L;

  @Test
  void publishStartSnapshotStopAndRestoreAreSeparate() {
    Fixture test = new Fixture(new MockManagedFlinkProvider());
    complete(test, test.publish("publish-one", test.command()));
    String release = activeRelease(test);
    assertThat(release).isNotBlank();
    assertThat(countRuns(test)).isZero();

    String firstStart = submit(test, CommandType.START, start(release, RestoreMode.FROM_NEW, null));
    complete(test, firstStart);
    String firstRun = currentRun(test);
    assertThat(firstRun).isNotBlank();
    assertThat(countRuns(test)).isEqualTo(1);
    assertThat(runState(test, firstRun)).isEqualTo("RUNNING");

    Command snapshot = new Command();
    snapshot.setExpectedRunId(firstRun);
    complete(test, submit(test, CommandType.SNAPSHOT, snapshot));
    String snapshotId =
        test.sql.queryForObject(
            "select id from t_flink_managed_snapshot where app_id=? and state='AVAILABLE'",
            String.class,
            APP);
    String restoreRef =
        test.sql.queryForObject(
            "select restore_ref from t_flink_managed_snapshot where id=?",
            String.class,
            snapshotId);
    assertThat(snapshotId).isNotBlank();
    assertThat(restoreRef).isNotBlank();

    Command stop = new Command();
    stop.setExpectedRunId(firstRun);
    stop.setMode("AFTER_SNAPSHOT");
    String stopOperation = submit(test, CommandType.STOP, stop);
    complete(test, stopOperation);
    assertThat(
            test.sql.queryForList(
                "select action from t_flink_managed_operation_step where operation_id=? order by step_index",
                String.class,
                stopOperation))
        .containsExactly("SNAPSHOT", "STOP");
    assertThat(runState(test, firstRun)).isEqualTo("STOPPED");

    String restoredOperation =
        submit(test, CommandType.START, start(release, RestoreMode.FROM_SNAPSHOT, snapshotId));
    complete(test, restoredOperation);
    String restored = currentRun(test);
    assertThat(restored).isNotEqualTo(firstRun);
    assertThat(countRuns(test)).isEqualTo(2);
    assertThat(runState(test, restored)).isEqualTo("RUNNING");
    assertThat(
            test.sql.queryForObject(
                "select result from t_flink_managed_operation_step where operation_id=?",
                String.class,
                restoredOperation))
        .contains(restoreRef);
  }

  @Test
  void rollbackPublishesFrozenHistoryWithoutStarting() {
    Fixture test = new Fixture(new MockManagedFlinkProvider());
    complete(test, test.publish("publish-one", test.command()));
    String first = activeRelease(test);

    JobInput edit = new JobInput();
    edit.setExpectedDraftRevision(1L);
    Spec spec = new Spec();
    spec.setType(JobType.SQL);
    spec.setSql("SELECT 2");
    edit.setSpec(spec);
    test.tx.executeWithoutResult(status -> test.jobs.edit(TEAM, APP, edit));
    Command second = test.command();
    second.setDraftRevision(2L);
    second.setExpectedReleaseId(first);
    complete(test, test.publish("publish-two", second));
    String secondRelease = activeRelease(test);
    assertThat(secondRelease).isNotEqualTo(first);
    assertThat(spec(test, secondRelease)).contains("SELECT 2");

    Command rollback = test.command();
    rollback.setSourceReleaseId(first);
    rollback.setExpectedReleaseId(secondRelease);
    complete(test, submit(test, CommandType.ROLLBACK, rollback));
    String third = activeRelease(test);
    assertThat(third).isNotEqualTo(first).isNotEqualTo(secondRelease);
    assertThat(spec(test, third)).isEqualTo(spec(test, first));
    assertThat(
            test.sql.queryForObject(
                "select release_no from t_flink_managed_release where id=?", Long.class, third))
        .isEqualTo(3);
    assertThat(countRuns(test)).isZero();
  }

  @Test
  void alibabaSqlUsesLockAndReadback() {
    Fixture test = new Fixture(new MockManagedFlinkProvider.AlibabaFixture());
    String operation = test.publish("alibaba-sql", test.command());
    complete(test, operation);
    assertThat(
            test.sql.queryForList(
                "select action from t_flink_managed_operation_step "
                    + "where operation_id=? order by step_index",
                String.class,
                operation))
        .containsExactly("CREATE_DRAFT", "LOCK_DRAFT", "UPDATE_DRAFT", "PUBLISH");
    assertThat(countRuns(test)).isZero();
  }

  @Test
  void jarArtifactAndArgumentsAreFrozen(@TempDir Path artifactDirectory) throws Exception {
    Fixture test = new Fixture(new MockManagedFlinkProvider.AlibabaFixture());
    test.sql.update(
        "update t_flink_managed_job set draft_spec=? where app_id=?", "{\"type\":\"JAR\"}", APP);
    ManagedFlinkArtifacts artifacts =
        new ManagedFlinkArtifacts(
            test.store, test.catalog, test.jobs, artifactDirectory.toString(), 100_000);
    byte[] jar;
    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ZipOutputStream zip = new ZipOutputStream(bytes)) {
      zip.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"));
      zip.write(
          "Manifest-Version: 1.0\r\nMain-Class: example.Main\r\n\r\n"
              .getBytes(StandardCharsets.UTF_8));
      zip.closeEntry();
      zip.finish();
      jar = bytes.toByteArray();
    }
    MockMultipartFile input =
        new MockMultipartFile("file", "review.jar", "application/java-archive", jar);
    String artifact = artifacts.upload(TEAM, APP, input);
    assertThat(artifacts.upload(TEAM, APP, input)).isEqualTo(artifact);

    JobInput edit = new JobInput();
    edit.setExpectedDraftRevision(1L);
    Spec spec = new Spec();
    spec.setType(JobType.JAR);
    spec.setArtifactId(artifact);
    spec.setArgs(Arrays.asList("contains spaces", "'quoted'", "汉字"));
    edit.setSpec(spec);
    test.tx.executeWithoutResult(status -> test.jobs.edit(TEAM, APP, edit));
    Command publish = test.command();
    publish.setDraftRevision(2L);
    String operation = test.publish("jar-release", publish);
    complete(test, operation);
    assertThat(
            test.sql.queryForList(
                "select action from t_flink_managed_operation_step "
                    + "where operation_id=? order by step_index",
                String.class,
                operation))
        .containsExactly("STAGE_ARTIFACT", "PUBLISH");
    String release = activeRelease(test);
    assertThat(spec(test, release)).contains("example.Main", "contains spaces", "'quoted'", "汉字");
    String artifactRef =
        test.sql.queryForObject(
            "select artifact_ref from t_flink_managed_release where id=?", String.class, release);
    assertThat(artifactRef).endsWith(".jar");

    String start = submit(test, CommandType.START, start(release, RestoreMode.FROM_NEW, null));
    complete(test, start);
    assertThat(
            test.sql.queryForObject(
                "select command from t_flink_managed_operation where id=?", String.class, start))
        .contains(artifactRef, "contains spaces");
  }

  private String submit(Fixture test, CommandType type, Command request) {
    return test.tx.execute(
        status -> test.jobs.enqueue(TEAM, APP, type, UUID.randomUUID().toString(), request));
  }

  private void complete(Fixture test, String operation) {
    for (int pass = 0; pass < 16 && !"SUCCEEDED".equals(test.status(operation)); pass++) {
      test.makeDue(operation);
      test.first.advance(operation);
    }
    assertThat(test.status(operation)).isEqualTo("SUCCEEDED");
  }

  private Command start(String release, RestoreMode mode, String snapshot) {
    Command command = new Command();
    command.setExpectedReleaseId(release);
    Restore restore = new Restore();
    restore.setMode(mode);
    restore.setSnapshotId(snapshot);
    command.setRestore(restore);
    return command;
  }

  private String activeRelease(Fixture test) {
    return test.sql.queryForObject(
        "select active_release_id from t_flink_managed_job where app_id=?", String.class, APP);
  }

  private String currentRun(Fixture test) {
    return test.sql.queryForObject(
        "select current_run_id from t_flink_managed_job where app_id=?", String.class, APP);
  }

  private int countRuns(Fixture test) {
    return test.sql.queryForObject(
        "select count(*) from t_flink_managed_run where app_id=?", Integer.class, APP);
  }

  private String runState(Fixture test, String run) {
    return test.sql.queryForObject(
        "select run_state from t_flink_managed_run where id=?", String.class, run);
  }

  private String spec(Fixture test, String release) {
    return test.sql.queryForObject(
        "select spec from t_flink_managed_release where id=?", String.class, release);
  }
}
