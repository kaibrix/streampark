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

import org.apache.streampark.console.core.enums.UserType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Action;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CloudState;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Command;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CommandType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Context;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobInput;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Outcome;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.ReadStatus;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Restore;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.RestoreMode;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Spec;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.ServiceHelper;
import org.apache.streampark.console.system.entity.Member;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.service.MemberService;
import org.apache.streampark.console.system.service.UserService;

import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ManagedFlinkLifecycleTest {

  @Test
  void takeoverUsesEvidenceAndFencesLateAck() throws Exception {
    RecordingProvider provider = new RecordingProvider(true, true);
    Fixture test = new Fixture(provider);
    String operation = test.publish("one", test.command());
    test.reachPublish(operation);
    ExecutorService dispatch = Executors.newSingleThreadExecutor();
    try {
      Future<?> firstWorker = dispatch.submit(() -> test.first.advance(operation));
      assertThat(provider.applied.await(10, TimeUnit.SECONDS)).isTrue();
      test.makeDue(operation);
      test.sql.update("update t_flink_managed_operation set lease_until=0 where id=?", operation);
      test.second.advance(operation);
      assertThat(test.status(operation)).isEqualTo("SUCCEEDED");
      provider.allowAck.countDown();
      firstWorker.get(10, TimeUnit.SECONDS);
      assertThat(test.status(operation)).isEqualTo("SUCCEEDED");
      assertThat(provider.publishes).hasValue(1);
      assertThat(
              test.sql.queryForObject(
                  "select pending_operation_id from t_flink_managed_job where app_id=900401",
                  String.class))
          .isNull();
      assertThat(
              test.sql.queryForObject(
                  "select status from t_flink_managed_release where app_id=900401", String.class))
          .isEqualTo("PUBLISHED");
    } finally {
      provider.allowAck.countDown();
      dispatch.shutdownNow();
    }
  }

  @Test
  void liveWorkerRenewsItsLeaseWhileWaitingForProvider() throws Exception {
    RecordingProvider provider = new RecordingProvider(true, true);
    Fixture test = new Fixture(provider);
    String operation = test.publish("heartbeat", test.command());
    test.reachPublish(operation);
    ExecutorService dispatch = Executors.newSingleThreadExecutor();
    try {
      Future<?> firstWorker = dispatch.submit(() -> test.first.advance(operation, 3_000));
      assertThat(provider.applied.await(10, TimeUnit.SECONDS)).isTrue();
      String leaseSql = "select lease_until from t_flink_managed_operation where id=?";
      long initialLease = test.sql.queryForObject(leaseSql, Long.class, operation);
      long waitUntil = initialLease + 20;
      while (System.currentTimeMillis() < waitUntil) {
        Thread.sleep(50);
      }
      assertThat(test.sql.queryForObject(leaseSql, Long.class, operation)).isGreaterThan(waitUntil);
      test.second.advance(operation);
      assertThat(test.status(operation)).isEqualTo("EXECUTING");

      provider.allowAck.countDown();
      firstWorker.get(10, TimeUnit.SECONDS);
      assertThat(test.status(operation)).isEqualTo("WAITING_PROVIDER");
      test.makeDue(operation);
      test.second.advance(operation);
      assertThat(test.status(operation)).isEqualTo("SUCCEEDED");
      assertThat(provider.publishes).hasValue(1);
      assertThat(test.sql.queryForObject(leaseSql, Long.class, operation)).isNull();
    } finally {
      provider.allowAck.countDown();
      dispatch.shutdownNow();
      test.first.stopRenewals();
      test.second.stopRenewals();
    }
  }

  @Test
  void aReadThatOutlivesItsLeaseAndDeadlineCannotDispatch() throws Exception {
    CountDownLatch reading = new CountDownLatch(1);
    CountDownLatch completeRead = new CountDownLatch(1);
    AtomicInteger starts = new AtomicInteger();
    MockManagedFlinkProvider provider =
        new MockManagedFlinkProvider() {
          @Override
          public Observation dispatch(Context context, OperationStep step, BooleanSupplier permit) {
            if (step.getAction() == Action.START) {
              reading.countDown();
              try {
                assertThat(completeRead.await(10, TimeUnit.SECONDS)).isTrue();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
              }
              if (!permit.getAsBoolean()) {
                return Observation.outcome(Outcome.REJECTED, "DISPATCH_NOT_AUTHORIZED");
              }
              starts.incrementAndGet();
              return dispatch(context, step);
            }
            return super.dispatch(context, step, permit);
          }
        };
    Fixture test = new Fixture(provider);
    test.complete(test.publish("published", test.command()));
    Command start = new Command();
    start.setExpectedReleaseId(test.jobField("active_release_id"));
    Restore restore = new Restore();
    restore.setMode(RestoreMode.FROM_NEW);
    restore.setConfirmedNew(true);
    start.setRestore(restore);
    String operation = test.enqueue(CommandType.START, "expired-read", start);

    ExecutorService dispatch = Executors.newSingleThreadExecutor();
    try {
      Future<?> oldWorker = dispatch.submit(() -> test.first.advance(operation));
      assertThat(reading.await(10, TimeUnit.SECONDS)).isTrue();
      test.sql.update(
          "update t_flink_managed_operation set deadline_at=0,lease_until=0, next_run_at=0 where id=?",
          operation);
      test.second.advance(operation);
      assertThat(test.status(operation)).isEqualTo("FAILED");
      completeRead.countDown();
      oldWorker.get(10, TimeUnit.SECONDS);
      assertThat(starts).hasValue(0);
      assertThat(test.jobField("pending_operation_id")).isNull();
      assertThat(
              test.sql.queryForObject(
                  "select count(*) from t_flink_managed_operation_step where operation_id=?",
                  Integer.class,
                  operation))
          .isZero();
    } finally {
      completeRead.countDown();
      dispatch.shutdownNow();
      test.first.stopRenewals();
      test.second.stopRenewals();
    }
  }

  @Test
  void unknownPublishKeepsFenceAndSameKey() {
    RecordingProvider provider = new RecordingProvider(false, false);
    Fixture test = new Fixture(provider);
    Command original = test.command();
    String operation = test.publish("one", original);
    test.reachPublish(operation);
    test.first.advance(operation);
    test.makeDue(operation);
    test.second.advance(operation);
    assertThat(test.status(operation)).isEqualTo("WAITING_PROVIDER");
    test.sql.update(
        "update t_flink_managed_operation set deadline_at=0, next_run_at=0 where id=?", operation);
    test.second.advance(operation);
    assertThat(test.status(operation)).isEqualTo("VERIFY_REQUIRED");
    assertThat(provider.publishes).hasValue(1);
    assertThat(
            test.sql.queryForObject(
                "select pending_operation_id from t_flink_managed_job where app_id=900401",
                String.class))
        .isEqualTo(operation);
    assertThat(test.publish("one", original)).isEqualTo(operation);
    assertThatExceptionOfType(ResponseStatusException.class)
        .isThrownBy(() -> test.publish("different", original))
        .matches(error -> error.getStatus().value() == 409);
    Command changed = test.command();
    changed.setDescription("different parameters");
    assertThatExceptionOfType(ResponseStatusException.class)
        .isThrownBy(() -> test.publish("one", changed))
        .matches(error -> error.getStatus().value() == 409);
  }

  @Test
  void teamPermissionsAndResourceOwnership() {
    Fixture test = new Fixture(new RecordingProvider(false, false));
    User user = new User();
    user.setUserId(100001L);
    user.setUserType(UserType.USER);
    when(test.login.getLoginUser()).thenReturn(user);
    when(test.members.findByUserId(100000L, 100001L)).thenReturn(new Member());
    when(test.members.findByUserId(200000L, 100001L)).thenReturn(new Member());
    when(test.users.getPermissions(100001L, 100000L))
        .thenReturn(Collections.singleton("app:detail"));
    when(test.users.getPermissions(100001L, 200000L))
        .thenReturn(Collections.singleton("app:detail"));

    assertThat(test.jobs.detail(100000L, 900401L)).containsEntry("appId", "900401");
    assertThatExceptionOfType(ResponseStatusException.class)
        .isThrownBy(() -> test.jobs.detail(200000L, 900401L))
        .matches(error -> error.getStatus().value() == 404);
    assertThatExceptionOfType(ResponseStatusException.class)
        .isThrownBy(() -> test.publish("blocked", test.command()))
        .matches(error -> error.getStatus().value() == 403);
    assertThat(
            test.sql.queryForObject(
                "select count(*) from t_flink_managed_operation", Integer.class))
        .isZero();
  }

  @Test
  void publishStartSnapshotStopAndRestore() {
    Fixture test = new Fixture(new MockManagedFlinkProvider());
    String publish = test.publish("publish", test.command());
    test.complete(publish);
    assertThat(test.sql.queryForObject("select count(*) from t_flink_managed_run", Integer.class))
        .isZero();
    String release = test.jobField("active_release_id");
    Restore restore = new Restore();
    restore.setMode(RestoreMode.FROM_NEW);
    Command start = new Command();
    start.setExpectedReleaseId(release);
    start.setRestore(restore);
    test.complete(test.enqueue(CommandType.START, "start", start));

    Command stop = new Command();
    stop.setExpectedRunId(test.jobField("current_run_id"));
    stop.setMode("AFTER_SNAPSHOT");
    test.complete(test.enqueue(CommandType.STOP, "stop", stop));
    String snapshot =
        test.sql.queryForObject(
            "select id from t_flink_managed_snapshot where state='AVAILABLE' and restorable=true",
            String.class);
    assertThat(snapshot).isNotBlank();

    restore.setMode(RestoreMode.FROM_SNAPSHOT);
    restore.setSnapshotId(snapshot);
    test.complete(test.enqueue(CommandType.START, "restore", start));
    assertThat(test.jobField("current_run_id")).isNotEqualTo(stop.getExpectedRunId());
    assertThat(
            test.sql.queryForObject(
                "select count(*) from t_flink_managed_run where run_state='RUNNING'",
                Integer.class))
        .isEqualTo(1);
  }

  @Test
  void confirmedStartupFailureKeepsRunEvidenceAndShowsFailedStep() {
    MockManagedFlinkProvider provider =
        new MockManagedFlinkProvider() {
          @Override
          public Observation observe(Context context, OperationStep step, Observation ack) {
            Observation result = super.observe(context, step, ack);
            if (step.getAction() == Action.START) {
              result.setRunState("FAILED");
            }
            return result;
          }
        };
    Fixture test = new Fixture(provider);
    test.complete(test.publish("publish", test.command()));
    String release = test.jobField("active_release_id");
    Restore restore = new Restore();
    restore.setMode(RestoreMode.FROM_NEW);
    Command start = new Command();
    start.setExpectedReleaseId(release);
    start.setRestore(restore);
    String operation = test.enqueue(CommandType.START, "failed-start", start);
    test.first.advance(operation);
    test.makeDue(operation);
    test.first.advance(operation);

    assertThat(test.status(operation)).isEqualTo("FAILED");
    assertThat(test.jobField("active_release_id")).isEqualTo(release);
    assertThat(test.jobField("pending_operation_id")).isNull();
    assertThat(
            test.sql.queryForMap(
                "select state,error_code,request_id from t_flink_managed_operation_step "
                    + "where operation_id=? and action='START'",
                operation))
        .containsEntry("state", "FAILED")
        .containsEntry("error_code", "RUNTIME_FAILED")
        .containsEntry("request_id", "mock-" + operation);
    assertThat(
            test.sql.queryForMap(
                "select run_state,provider_instance_id,release_id from t_flink_managed_run "
                    + "where id=?",
                test.jobField("current_run_id")))
        .containsEntry("run_state", "FAILED")
        .containsEntry("provider_instance_id", "instance-" + operation)
        .containsEntry("release_id", release);
  }

  @Test
  void rejectedPublishKeepsPreviousReleaseActive() {
    AtomicInteger publications = new AtomicInteger();
    MockManagedFlinkProvider provider =
        new MockManagedFlinkProvider() {
          @Override
          public Observation dispatch(Context context, OperationStep step) {
            if (step.getAction() == Action.PUBLISH && publications.incrementAndGet() > 1) {
              return Observation.outcome(Outcome.REJECTED, "DRAFT_REJECTED");
            }
            return super.dispatch(context, step);
          }
        };
    Fixture test = new Fixture(provider);
    test.complete(test.publish("publish", test.command()));
    String release = test.jobField("active_release_id");
    Command update = test.command();
    update.setExpectedReleaseId(release);
    String operation = test.publish("rejected-publish", update);
    for (int transition = 0; transition < 4; transition++) {
      test.makeDue(operation);
      test.first.advance(operation);
    }
    assertThat(test.status(operation)).isEqualTo("FAILED");
    assertThat(test.jobField("active_release_id")).isEqualTo(release);
    assertThat(publications).hasValue(2);
    assertThat(
            test.sql.queryForMap(
                "select state,error_code from t_flink_managed_operation_step "
                    + "where operation_id=? and action='PUBLISH'",
                operation))
        .containsEntry("state", "FAILED")
        .containsEntry("error_code", "DRAFT_REJECTED");
    assertThat(test.sql.queryForObject("select count(*) from t_flink_managed_run", Integer.class))
        .isZero();
  }

  @Test
  void failedRestoreKeepsOriginalRunStopped() {
    AtomicInteger restoreRequests = new AtomicInteger();
    MockManagedFlinkProvider provider =
        new MockManagedFlinkProvider() {
          @Override
          public Observation dispatch(Context context, OperationStep step) {
            if (step.getAction() == Action.START && step.getCommand().getRestoreRef() != null) {
              restoreRequests.incrementAndGet();
              return Observation.outcome(Outcome.REJECTED, "STATE_INCOMPATIBLE");
            }
            return super.dispatch(context, step);
          }
        };
    Fixture test = new Fixture(provider);
    test.complete(test.publish("publish", test.command()));
    String release = test.jobField("active_release_id");
    Restore restore = new Restore();
    restore.setMode(RestoreMode.FROM_NEW);
    Command start = new Command();
    start.setExpectedReleaseId(release);
    start.setRestore(restore);
    test.complete(test.enqueue(CommandType.START, "start", start));
    String oldRun = test.jobField("current_run_id");

    Command stop = new Command();
    stop.setExpectedRunId(oldRun);
    stop.setMode("AFTER_SNAPSHOT");
    test.complete(test.enqueue(CommandType.STOP, "stop", stop));

    String snapshot =
        test.sql.queryForObject("select id from t_flink_managed_snapshot", String.class);
    restore.setMode(RestoreMode.FROM_SNAPSHOT);
    restore.setSnapshotId(snapshot);
    String restoreOperation = test.enqueue(CommandType.START, "restore", start);
    test.first.advance(restoreOperation);
    assertThat(test.status(restoreOperation)).isEqualTo("FAILED");
    assertThat(restoreRequests).hasValue(1);
    assertThat(test.jobField("current_run_id")).isEqualTo(oldRun);
    assertThat(test.sql.queryForObject("select run_state from t_flink_managed_run", String.class))
        .isEqualTo("STOPPED");
    assertThat(
            test.sql.queryForObject(
                "select count(*) from t_flink_managed_operation_step where operation_id=?",
                Integer.class,
                restoreOperation))
        .isEqualTo(1);
  }

  @Test
  void externalRunAndDriftFreezeWrites() {
    MockManagedFlinkProvider provider = new MockManagedFlinkProvider();
    Fixture test = new Fixture(provider);
    test.complete(test.publish("publish", test.command()));
    String job = test.jobField("provider_job_id");
    String version =
        test.sql.queryForObject(
            "select provider_version from t_flink_managed_release", String.class);
    provider.recordState(
        new CloudState(
            ReadStatus.FOUND,
            job,
            version,
            "external",
            "runtime",
            "RUNNING",
            System.currentTimeMillis()));
    test.reconciler.refresh();
    assertThat(test.sql.queryForObject("select origin from t_flink_managed_run", String.class))
        .isEqualTo("EXTERNAL");
    assertThat(test.sql.queryForObject("select release_id from t_flink_managed_run", String.class))
        .isNull();

    provider.recordState(
        new CloudState(
            ReadStatus.FOUND,
            job,
            "different-version",
            "external",
            "runtime",
            "RUNNING",
            System.currentTimeMillis()));
    test.sql.update("update t_flink_managed_job set next_observe_at=0");
    test.reconciler.refresh();
    assertThat(test.jobField("sync_status")).isEqualTo("DRIFTED");
    assertThatExceptionOfType(ResponseStatusException.class)
        .isThrownBy(() -> test.publish("blocked", test.command()))
        .matches(error -> error.getStatus().value() == 409);
  }

  @Test
  void cloudVerificationBlocksNewCommandsWhileDraftAndDuplicateReceiptStayAvailable() {
    Fixture test = new Fixture(new MockManagedFlinkProvider());
    test.sql.update("update t_flink_managed_job set sync_status='STALE'");
    assertThat(test.jobs.detail(100000L, 900401L)).containsEntry("cloudBound", false);
    String published = test.publish("first", test.command());
    test.complete(published);
    Map<String, Object> confirmed = test.jobs.detail(100000L, 900401L);
    assertThat(confirmed).containsEntry("cloudBound", true).containsEntry("syncStatus", "FRESH");
    assertThat(((Number) confirmed.get("observedAt")).longValue()).isPositive();

    for (String status : List.of("STALE", "DRIFTED", "MISSING", "AUTH_ERROR")) {
      test.sql.update("update t_flink_managed_job set sync_status=?", status);
      assertThat(test.jobs.detail(100000L, 900401L))
          .containsEntry("cloudBound", true)
          .containsEntry("syncStatus", status)
          .containsEntry("observedAt", confirmed.get("observedAt"));
      for (CommandType type : CommandType.values()) {
        assertThatExceptionOfType(ResponseStatusException.class)
            .isThrownBy(() -> test.enqueue(type, "blocked-" + type, test.command()))
            .matches(error -> error.getStatus().value() == 409);
      }
    }

    Spec draft = new Spec();
    draft.setType(JobType.SQL);
    draft.setSql("SELECT 2");
    JobInput edit = new JobInput();
    edit.setExpectedDraftRevision(1L);
    edit.setSpec(draft);
    test.tx.executeWithoutResult(t -> test.jobs.edit(100000L, 900401L, edit));
    assertThat(((Spec) test.jobs.detail(100000L, 900401L).get("spec")).getSql())
        .isEqualTo("SELECT 2");
    assertThat(test.publish("first", test.command())).isEqualTo(published);
    assertThat(
            test.sql.queryForObject(
                "select count(*) from t_flink_managed_operation", Integer.class))
        .isEqualTo(1);
  }

  @Test
  void olderActiveInstanceDoesNotSilentlyReplaceNewerRecordedHistory() {
    MockManagedFlinkProvider provider = new MockManagedFlinkProvider();
    Fixture test = new Fixture(provider);
    test.complete(test.publish("publish", test.command()));
    String job = test.jobField("provider_job_id");
    String version =
        test.sql.queryForObject(
            "select provider_version from t_flink_managed_release", String.class);
    CloudState latest =
        new CloudState(ReadStatus.FOUND, job, version, "newest", "runtime-newest", "STOPPED", 2000);
    provider.recordState(latest);
    test.reconciler.refresh();
    String current = test.jobField("current_run_id");
    assertThat(test.jobField("sync_status")).isEqualTo("FRESH");

    test.reconciler.apply(
        test.jobs.job(100000L, 900401L, false),
        new CloudState(
            ReadStatus.FOUND, "other-job", version, "older", "runtime-older", "RUNNING", 1000),
        System.currentTimeMillis());
    assertThat(test.jobField("sync_status")).isEqualTo("FRESH");

    provider.recordState(
        new CloudState(ReadStatus.FOUND, job, version, "older", "runtime-older", "STOPPED", 1000));
    test.sql.update("update t_flink_managed_job set next_observe_at=0");
    test.reconciler.refresh();
    assertThat(test.jobField("sync_status")).isEqualTo("FRESH");

    provider.recordState(
        new CloudState(ReadStatus.FOUND, job, version, "older", "runtime-older", "RUNNING", 1000));
    test.sql.update("update t_flink_managed_job set next_observe_at=0");
    test.reconciler.refresh();
    assertThat(test.jobField("sync_status")).isEqualTo("STALE");
    assertThat(test.jobField("current_run_id")).isEqualTo(current);
    assertThat(
            test.sql.queryForObject(
                "select provider_instance_id from t_flink_managed_run", String.class))
        .isEqualTo("newest");

    provider.recordState(latest);
    test.sql.update("update t_flink_managed_job set next_observe_at=0");
    test.reconciler.refresh();
    assertThat(test.jobField("sync_status")).isEqualTo("FRESH");
    assertThat(test.jobField("current_run_id")).isEqualTo(current);
  }

  @Test
  void keepsConfirmedStateWhileBackingOffAndResetsAfterASuccessfulRead() {
    MockManagedFlinkProvider provider = new MockManagedFlinkProvider();
    Fixture test = new Fixture(provider);
    test.complete(test.publish("publish", test.command()));
    String job = test.jobField("provider_job_id");
    String version =
        test.sql.queryForObject(
            "select provider_version from t_flink_managed_release", String.class);
    CloudState healthy =
        new CloudState(ReadStatus.FOUND, job, version, "run", "runtime", "RUNNING", 2000);
    provider.recordState(healthy);
    test.reconciler.refresh();
    String latestRead = test.jobField("observed_at");
    String activeRelease = test.jobField("active_release_id");
    String currentRun = test.jobField("current_run_id");
    assertThat(test.jobField("sync_status")).isEqualTo("FRESH");
    assertThat(currentRun).isNotBlank();

    long now = System.currentTimeMillis();
    CloudState failed = new CloudState(ReadStatus.THROTTLED, job, null, null, null, null, 0);
    test.reconciler.apply(test.jobs.job(100000L, 900401L, false), failed, now);
    assertThat(test.jobField("sync_status")).isEqualTo("THROTTLED");
    assertThat(Long.parseLong(test.jobField("next_observe_at")) - now).isBetween(60_000L, 72_000L);
    assertThat(test.jobField("read_failures")).isEqualTo("1");

    // The healthy provider still answers, but the next scheduled poll is not due.
    test.reconciler.refresh();
    assertThat(test.jobField("sync_status")).isEqualTo("THROTTLED");

    test.reconciler.apply(test.jobs.job(100000L, 900401L, false), failed, now);
    assertThat(Long.parseLong(test.jobField("next_observe_at")) - now)
        .isBetween(120_000L, 144_000L);
    assertThat(test.jobField("read_failures")).isEqualTo("2");

    long serverRetry = now + 20 * 60_000;
    CloudState unavailable =
        new CloudState(ReadStatus.AUTH_ERROR, job, null, null, null, null, 0, serverRetry);
    test.reconciler.apply(test.jobs.job(100000L, 900401L, false), unavailable, now);
    assertThat(test.jobField("sync_status")).isEqualTo("AUTH_ERROR");
    assertThat(test.jobField("next_observe_at")).isEqualTo(Long.toString(serverRetry));
    assertThat(test.jobField("observed_at")).isEqualTo(latestRead);
    assertThat(test.jobField("active_release_id")).isEqualTo(activeRelease);
    assertThat(test.jobField("current_run_id")).isEqualTo(currentRun);
    assertThat(test.jobField("pending_operation_id")).isNull();

    test.sql.update("update t_flink_managed_job set next_observe_at=0");
    test.reconciler.refresh();
    assertThat(test.jobField("sync_status")).isEqualTo("FRESH");
    assertThat(test.jobField("read_failures")).isEqualTo("0");
    assertThat(Long.parseLong(test.jobField("next_observe_at")))
        .isEqualTo(Long.parseLong(test.jobField("observed_at")) + 30_000);
    assertThat(test.jobField("current_run_id")).isEqualTo(currentRun);
  }

  static final class Fixture {
    final JdbcTemplate sql;
    final TransactionTemplate tx;
    final ManagedFlinkJobs jobs;
    final ManagedFlinkStore store;
    final ManagedFlinkCatalog catalog;
    final ManagedFlinkWorker first;
    final ManagedFlinkWorker second;
    final ManagedFlinkReconciler reconciler;
    final ServiceHelper login = mock(ServiceHelper.class);
    final MemberService members = mock(MemberService.class);
    final UserService users = mock(UserService.class);

    Fixture(ManagedFlinkProvider provider) {
      DriverManagerDataSource dataSource =
          new DriverManagerDataSource(
              "jdbc:h2:mem:"
                  + UUID.randomUUID()
                  + ";MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=true",
              "sa",
              "");
      new ResourceDatabasePopulator(new ClassPathResource("db/schema-h2.sql")).execute(dataSource);
      sql = new JdbcTemplate(dataSource);
      DataSourceTransactionManager manager = new DataSourceTransactionManager(dataSource);
      tx = new TransactionTemplate(manager);
      ProxyFactory repository = new ProxyFactory(new ManagedFlinkStore(sql));
      repository.setProxyTargetClass(true);
      store = (ManagedFlinkStore) repository.getProxy();
      User admin = new User();
      admin.setUserId(100000L);
      admin.setUserType(UserType.ADMIN);
      when(login.getLoginUser()).thenReturn(admin);
      catalog =
          new ManagedFlinkCatalog(
              store,
              login,
              members,
              users,
              new ManagedFlinkCredentials("", store),
              Collections.singletonList(provider));
      jobs = new ManagedFlinkJobs(store, catalog, mock(ApplicationService.class), manager);
      first = new ManagedFlinkWorker(store, catalog, jobs, manager);
      second = new ManagedFlinkWorker(store, catalog, jobs, manager);
      reconciler = new ManagedFlinkReconciler(store, catalog, manager);
      sql.update(
          "insert into t_flink_app (id,team_id,execution_mode,job_name,state,tracking)"
              + " values (900401,100000,7,'review',0,0)");
      sql.update(
          "insert into t_flink_cloud_connection (id,team_id,name,provider,account_ref,"
              + "created_by,created_at) values ('conn',100000,'review',?,'test-account',100000,1)",
          provider.id());
      sql.update(
          "insert into t_flink_managed_env (id,team_id,connection_id,name,region,target)"
              + " values ('env',100000,'conn','review','test','{}')");
      sql.update(
          "insert into t_flink_managed_job (app_id,team_id,environment_id,engine_ref,"
              + "draft_revision,draft_spec,scope_key) values (900401,100000,'env','engine',1,?,'scope')",
          "{\"type\":\"SQL\",\"sql\":\"SELECT 1\"}");
    }

    String publish(String key, Command command) {
      return tx.execute(
          status -> jobs.enqueue(100000L, 900401L, CommandType.PUBLISH, key, command));
    }

    String enqueue(CommandType type, String key, Command command) {
      return tx.execute(status -> jobs.enqueue(100000L, 900401L, type, key, command));
    }

    void complete(String operation) {
      for (int transition = 0;
          transition < 10 && !"SUCCEEDED".equals(status(operation));
          transition++) {
        makeDue(operation);
        first.advance(operation);
      }
      assertThat(status(operation)).isEqualTo("SUCCEEDED");
    }

    String jobField(String column) {
      return ManagedFlinkStore.text(
          sql.queryForMap("select * from t_flink_managed_job where app_id=900401"), column);
    }

    Command command() {
      Command command = new Command();
      command.setDraftRevision(1L);
      command.setExpectedEnvVersion(1);
      return command;
    }

    void reachPublish(String operation) {
      for (int transition = 0; transition < 4; transition++) {
        makeDue(operation);
        first.advance(operation);
      }
      makeDue(operation);
      assertThat(
              sql.queryForObject(
                  "select step_index from t_flink_managed_operation where id=?",
                  Integer.class,
                  operation))
          .isEqualTo(2);
    }

    void makeDue(String operation) {
      sql.update("update t_flink_managed_operation set next_run_at=0 where id=?", operation);
    }

    String status(String operation) {
      return sql.queryForObject(
          "select status from t_flink_managed_operation where id=?", String.class, operation);
    }
  }

  private static final class RecordingProvider implements ManagedFlinkProvider {
    final MockManagedFlinkProvider delegate = new MockManagedFlinkProvider();
    final CountDownLatch applied = new CountDownLatch(1);
    final CountDownLatch allowAck = new CountDownLatch(1);
    final AtomicInteger publishes = new AtomicInteger();
    final boolean holdAck;
    final boolean publishVisible;

    RecordingProvider(boolean holdAck, boolean publishVisible) {
      this.holdAck = holdAck;
      this.publishVisible = publishVisible;
    }

    @Override
    public String id() {
      return delegate.id();
    }

    @Override
    public Map<String, String> capabilities(Context context) {
      return delegate.capabilities(context);
    }

    @Override
    public Observation dispatch(Context context, OperationStep step) {
      Observation result = delegate.dispatch(context, step);
      if (step.getAction() != Action.PUBLISH) {
        return result;
      }
      publishes.incrementAndGet();
      applied.countDown();
      if (holdAck) {
        try {
          if (!allowAck.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException(
                "Timed out waiting to return the original acknowledgement");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Original acknowledgement interrupted", e);
        }
      }
      throw new IllegalStateException("Simulated provider response loss after effect");
    }

    @Override
    public Observation observe(Context context, OperationStep step, Observation ack) {
      if (step.getAction() == Action.PUBLISH && !publishVisible) {
        return Observation.outcome(Outcome.UNKNOWN, "PROVIDER_EVIDENCE_AMBIGUOUS");
      }
      return delegate.observe(context, step, ack);
    }
  }
}
