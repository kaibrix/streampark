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

import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.managedflink.ManagedFlinkLifecycleTest.Fixture;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Action;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Command;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CommandType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Context;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobInput;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Restore;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.RestoreMode;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Spec;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Target;
import org.apache.streampark.console.core.service.ApplicationService;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.util.StreamUtils;
import org.springframework.web.server.ResponseStatusException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ManagedFlinkSqlUdfTest {

  private static final long TEAM = 100000L;
  private static final long APP = 900402L;
  private static final String SQL =
      "CREATE TEMPORARY FUNCTION normalize AS '"
          + ReviewUpper.class.getName()
          + "' USING JAR 'udf.jar'; SELECT normalize('streampark');";

  @Test
  void sqlUdfUsesEngine116(@TempDir Path directory) throws IOException {
    verifyLifecycle("1.16", directory);
  }

  @Test
  void sqlUdfUsesEngine117(@TempDir Path directory) throws IOException {
    verifyLifecycle("1.17", directory);
  }

  @Test
  void sqlUdfUsesEngine120(@TempDir Path directory) throws IOException {
    verifyLifecycle("1.20", directory);
  }

  @Test
  void unverifiedSqlUdfCannotPublish(@TempDir Path directory) throws IOException {
    TrackingProvider provider = new TrackingProvider(false);
    Fixture fixture = new Fixture(provider);
    ManagedFlinkJobs jobs = createApp(fixture, "1.20");
    bindArtifact(fixture, jobs, directory);
    assertThatThrownBy(() -> publish(fixture, jobs))
        .isInstanceOfSatisfying(
            ResponseStatusException.class,
            error -> {
              assertThat(error.getStatus().value()).isEqualTo(422);
              assertThat(error.getReason()).contains("PUBLISH_SQL_UDF", "UNVERIFIED");
            });
    assertThat(provider.steps).isEmpty();
    assertThat(
            fixture.sql.queryForObject(
                "select count(*) from t_flink_managed_release", Integer.class))
        .isZero();
  }

  @Test
  void udfArtifactBelongsToItsSqlApp() {
    Fixture fixture = new Fixture(new TrackingProvider(true));
    ManagedFlinkJobs jobs = createApp(fixture, "1.16");
    fixture.sql.update(
        "insert into t_flink_managed_artifact (id,app_id,digest,size_bytes,object_ref,created_at) "
            + "values ('other-app-artifact',900401,'fixture',1,'fixture',1)");
    Spec spec = sqlSpec();
    spec.setUdfArtifactId("other-app-artifact");
    assertThatThrownBy(() -> jobs.validateDraft(TEAM, APP, spec))
        .isInstanceOfSatisfying(
            ResponseStatusException.class,
            error -> assertThat(error.getStatus().value()).isEqualTo(404));
  }

  @Test
  void additionalJarsAreStagedInOrderAndReusedWhenStartingTheRelease(@TempDir Path dir)
      throws IOException {
    TrackingProvider provider = new TrackingProvider(true);
    Fixture fixture = new Fixture(provider);
    ManagedFlinkJobs jobs = createApp(fixture, "FLINK_VERSION_1_17");
    ManagedFlinkArtifacts artifacts =
        new ManagedFlinkArtifacts(fixture.store, fixture.catalog, jobs, dir.toString(), 65536);
    List<String> ids = new ArrayList<>();
    for (Class<?> type : new Class<?>[] {ReviewUpper.class, ReviewLower.class}) {
      ids.add(
          artifacts.upload(
              TEAM,
              APP,
              new MockMultipartFile(
                  "file",
                  type.getSimpleName() + ".jar",
                  "application/java-archive",
                  udfJar(type))));
    }
    JobInput draft = new JobInput();
    draft.setExpectedDraftRevision(1L);
    Spec spec = sqlSpec();
    spec.setDependencyIds(ids);
    draft.setSpec(spec);
    fixture.tx.executeWithoutResult(t -> jobs.edit(TEAM, APP, draft));
    fixture.complete(publish(fixture, jobs));
    OperationStep publication = provider.steps.get(provider.steps.size() - 1);
    List<String> refs = VolcengineExecutable.dependencies(publication);
    assertThat(refs).hasSize(2).doesNotHaveDuplicates().doesNotContainNull();

    JsonObject remote = new JsonObject();
    remote.addProperty("JobType", VolcengineExecutable.providerType(spec));
    remote.addProperty("EngineVersion", publication.getCommand().getEngineRef());
    VolcengineExecutable.writeDraft(remote, publication);
    assertThat(VolcengineExecutable.matches(remote, publication, true)).isTrue();
    remote.remove("Dependency");
    assertThat(VolcengineExecutable.matches(remote, publication, true)).isFalse();

    Command start = new Command();
    start.setExpectedReleaseId(
        fixture.sql.queryForObject(
            "select active_release_id from t_flink_managed_job where app_id=?", String.class, APP));
    Restore restore = new Restore();
    restore.setMode(RestoreMode.FROM_NEW);
    start.setRestore(restore);
    fixture.complete(
        fixture.tx.execute(t -> jobs.enqueue(TEAM, APP, CommandType.START, "start-deps", start)));
    OperationStep started = provider.steps.get(provider.steps.size() - 1);
    assertThat(VolcengineExecutable.dependencies(started)).containsExactlyElementsOf(refs);
  }

  @Test
  void failedFirstInstanceKeepsStagedDependenciesAndDirectReceipt(@TempDir Path dir)
      throws IOException {
    TrackingProvider provider =
        new TrackingProvider(true) {
          @Override
          public Map<String, String> capabilities(Context context) {
            Map<String, String> result = super.capabilities(context);
            result.put("SUBMIT_INSTANCE", "AVAILABLE");
            return result;
          }

          @Override
          public Observation dispatch(Context context, OperationStep step) {
            if (step.getAction() == Action.SUBMIT) {
              steps.add(step);
              return Observation.outcome(ManagedFlinkModels.Outcome.WAITING, null);
            }
            return super.dispatch(context, step);
          }

          @Override
          public Observation observe(Context context, OperationStep step, Observation ack) {
            if (step.getAction() != Action.SUBMIT) {
              return super.observe(context, step, ack);
            }
            Observation result = Observation.outcome(ManagedFlinkModels.Outcome.CONFIRMED, null);
            result.setSpecHash(step.getCommand().getSpecHash());
            result.setVersion("1");
            result.setJobId("42");
            result.setInstanceId("43");
            result.setRunState("FAILED");
            recordState(
                new ManagedFlinkModels.CloudState(
                    ManagedFlinkModels.ReadStatus.FOUND,
                    "42",
                    "1",
                    "43",
                    null,
                    "FAILED",
                    System.currentTimeMillis()));
            return result;
          }
        };
    Fixture fixture = new Fixture(provider);
    ManagedFlinkJobs jobs = createApp(fixture, "1.17");
    ManagedFlinkArtifacts artifacts =
        new ManagedFlinkArtifacts(fixture.store, fixture.catalog, jobs, dir.toString(), 65536);
    String dep =
        artifacts.upload(
            TEAM,
            APP,
            new MockMultipartFile("file", "udf.jar", "application/java-archive", udfJar()));
    JobInput draft = new JobInput();
    draft.setExpectedDraftRevision(1L);
    Spec spec = sqlSpec();
    spec.setDependencyIds(java.util.Collections.singletonList(dep));
    draft.setSpec(spec);
    fixture.tx.executeWithoutResult(t -> jobs.edit(TEAM, APP, draft));
    Command submit = new Command();
    submit.setExpectedEnvVersion(1);
    submit.setDraftRevision(2L);
    String operation =
        fixture.tx.execute(t -> jobs.enqueue(TEAM, APP, CommandType.SUBMIT, "submit", submit));
    for (int i = 0; i < 6 && !"FAILED".equals(fixture.status(operation)); i++) {
      fixture.makeDue(operation);
      fixture.first.advance(operation);
    }
    assertThat(fixture.status(operation)).isEqualTo("FAILED");
    Map<String, Object> job = jobs.job(TEAM, APP, false);
    String release = ManagedFlinkStore.text(job, "active_release_id");
    assertThat(release).isNotNull();
    Map<String, Object> env = fixture.catalog.environment(TEAM, "env", false);
    Target target = fixture.catalog.target(env);
    target.setTlsTopic("new-log-topic");
    env.put("target", fixture.store.encode(target));
    assertThat(
            fixture
                .catalog
                .jobContext(fixture.catalog.connection(TEAM, "conn"), env, null, job)
                .getTarget()
                .getDirectReleaseId())
        .isEqualTo(release);
    assertThat(
            fixture
                .catalog
                .jobContext(fixture.catalog.connection(TEAM, "conn"), env, target, job)
                .getTarget()
                .getDirectReleaseId())
        .isEqualTo(release);
    assertThat(target.getDirectReleaseId()).isNull();
    Target otherPool = target;
    otherPool.setResourcePoolId("another-pool");
    env.put("target", fixture.store.encode(otherPool));
    assertThat(
            fixture
                .catalog
                .jobContext(fixture.catalog.connection(TEAM, "conn"), env, otherPool, job)
                .getTarget()
                .getDirectReleaseId())
        .isNull();
    assertThat(
            fixture
                .catalog
                .jobContext(fixture.catalog.connection(TEAM, "conn"), env, null, job)
                .getTarget()
                .getDirectReleaseId())
        .isEqualTo(release);
    Command start = new Command();
    start.setExpectedReleaseId(release);
    Restore restore = new Restore();
    restore.setMode(RestoreMode.FROM_NEW);
    restore.setConfirmedNew(true);
    start.setRestore(restore);
    fixture.complete(
        fixture.tx.execute(t -> jobs.enqueue(TEAM, APP, CommandType.START, "retry", start)));
    OperationStep retry = provider.steps.get(provider.steps.size() - 1);
    assertThat(VolcengineExecutable.dependencies(retry))
        .containsExactlyElementsOf(VolcengineExecutable.dependencies(provider.steps.get(1)));
  }

  private void verifyLifecycle(String engine, Path directory) throws IOException {
    TrackingProvider provider = new TrackingProvider(true);
    Fixture fixture = new Fixture(provider);
    ManagedFlinkJobs jobs = createApp(fixture, engine);
    String artifactId = bindArtifact(fixture, jobs, directory);

    String publish = publish(fixture, jobs);
    fixture.complete(publish);
    assertThat(provider.steps)
        .extracting(OperationStep::getAction)
        .containsExactly(
            Action.STAGE_ARTIFACT, Action.CREATE_DRAFT, Action.UPDATE_DRAFT, Action.PUBLISH);
    OperationStep stage = provider.steps.get(0);
    assertThat(stage.getCommand().getArtifactDigest()).hasSize(64);
    assertThat(stage.getCommand().getArtifactSize()).isPositive();
    OperationStep deployment = provider.steps.get(3);
    assertThat(deployment.getCommand().getEngineRef()).isEqualTo(engine);
    assertThat(deployment.getCommand().getTarget().getEngineRef()).isEqualTo(engine);
    assertThat(deployment.getCommand().getSpec().getUdfArtifactId()).isEqualTo(artifactId);
    assertThat(deployment.getCommand().getSpec().getSql()).isEqualTo(SQL);
    assertThat(deployment.getStagedArtifact()).endsWith(".jar");

    String release =
        fixture.sql.queryForObject(
            "select active_release_id from t_flink_managed_job where app_id=?", String.class, APP);
    assertThat(release).isNotBlank();
    assertThat(
            fixture.sql.queryForObject(
                "select artifact_ref from t_flink_managed_release where id=?",
                String.class,
                release))
        .isEqualTo(deployment.getStagedArtifact());
    assertThat(
            fixture.sql.queryForObject("select count(*) from t_flink_managed_run", Integer.class))
        .isZero();

    Command start = new Command();
    start.setExpectedReleaseId(release);
    Restore restore = new Restore();
    restore.setMode(RestoreMode.FROM_NEW);
    start.setRestore(restore);
    fixture.complete(
        fixture.tx.execute(status -> jobs.enqueue(TEAM, APP, CommandType.START, "start", start)));
    OperationStep startStep = provider.steps.get(4);
    assertThat(startStep.getAction()).isEqualTo(Action.START);
    assertThat(startStep.getCommand().getEngineRef()).isEqualTo(engine);
    assertThat(startStep.getCommand().getSpec().getUdfArtifactId()).isEqualTo(artifactId);
    assertThat(startStep.getCommand().getArtifactRef()).isEqualTo(deployment.getStagedArtifact());
  }

  private ManagedFlinkJobs createApp(Fixture fixture, String engine) {
    Target target = new Target();
    target.setEngineRef(engine);
    fixture.sql.update(
        "update t_flink_managed_env set target=? where id='env'", fixture.store.encode(target));
    ApplicationService applications = mock(ApplicationService.class);
    when(applications.save(any(Application.class)))
        .thenAnswer(
            invocation -> {
              Application app = invocation.getArgument(0);
              app.setId(APP);
              fixture.sql.update(
                  "insert into t_flink_app (id,team_id,user_id,execution_mode,job_name,state,tracking)"
                      + " values (?,?,?,?,?,?,?)",
                  app.getId(),
                  app.getTeamId(),
                  app.getUserId(),
                  app.getExecutionMode(),
                  app.getJobName(),
                  app.getState(),
                  app.getTracking());
              return true;
            });
    ManagedFlinkJobs jobs =
        new ManagedFlinkJobs(
            fixture.store, fixture.catalog, applications, fixture.tx.getTransactionManager());
    JobInput input = new JobInput();
    input.setName("sql-udf");
    input.setEnvironmentId("env");
    input.setSpec(sqlSpec());
    Long created = fixture.tx.execute(status -> jobs.create(TEAM, input));
    assertThat(created).isEqualTo(APP);
    return jobs;
  }

  private String bindArtifact(Fixture fixture, ManagedFlinkJobs jobs, Path directory)
      throws IOException {
    ManagedFlinkArtifacts artifacts =
        new ManagedFlinkArtifacts(
            fixture.store, fixture.catalog, jobs, directory.toString(), 65_536);
    String artifactId =
        artifacts.upload(
            TEAM,
            APP,
            new MockMultipartFile("file", "udf.jar", "application/java-archive", udfJar()));
    JobInput draft = new JobInput();
    draft.setExpectedDraftRevision(1L);
    Spec spec = sqlSpec();
    spec.setUdfArtifactId(artifactId);
    draft.setSpec(spec);
    fixture.tx.executeWithoutResult(status -> jobs.edit(TEAM, APP, draft));
    jobs.validateDraft(TEAM, APP, spec);
    return artifactId;
  }

  private String publish(Fixture fixture, ManagedFlinkJobs jobs) {
    Command command = new Command();
    command.setDraftRevision(2L);
    command.setExpectedEnvVersion(1);
    return fixture.tx.execute(
        status -> jobs.enqueue(TEAM, APP, CommandType.PUBLISH, "publish", command));
  }

  private Spec sqlSpec() {
    Spec spec = new Spec();
    spec.setType(JobType.SQL);
    spec.setSql(SQL);
    return spec;
  }

  private byte[] udfJar() throws IOException {
    return udfJar(ReviewUpper.class);
  }

  private byte[] udfJar(Class<?> type) throws IOException {
    String name = type.getName().replace('.', '/') + ".class";
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ZipOutputStream archive = new ZipOutputStream(bytes);
        InputStream code = ReviewUpper.class.getClassLoader().getResourceAsStream(name)) {
      archive.putNextEntry(new ZipEntry(name));
      StreamUtils.copy(code, archive);
      archive.closeEntry();
    }
    return bytes.toByteArray();
  }

  public static class ReviewLower {
    public String eval(String value) {
      return value.toLowerCase(java.util.Locale.ROOT);
    }
  }

  public static class ReviewUpper {

    public String eval(String value) {
      return value == null ? null : value.toUpperCase(java.util.Locale.ROOT);
    }
  }

  private static class TrackingProvider extends MockManagedFlinkProvider {
    final List<OperationStep> steps = new ArrayList<>();
    final boolean supported;

    TrackingProvider(boolean supported) {
      this.supported = supported;
    }

    @Override
    public Map<String, String> capabilities(Context context) {
      Map<String, String> capabilities = super.capabilities(context);
      if (!supported) {
        capabilities.remove("PUBLISH_SQL_UDF");
      }
      return capabilities;
    }

    @Override
    public Observation dispatch(Context context, OperationStep step) {
      steps.add(step);
      return super.dispatch(context, step);
    }
  }
}
