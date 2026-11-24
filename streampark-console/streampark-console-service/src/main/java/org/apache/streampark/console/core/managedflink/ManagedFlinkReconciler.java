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

import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CloudState;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.ReadStatus;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.id;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.number;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.text;

@Slf4j
@Component
@ConditionalOnProperty(name = "streampark.managed-flink.enabled", havingValue = "true")
class ManagedFlinkReconciler {

  private static final List<String> TERMINAL = Arrays.asList("STOPPED", "FAILED", "FINISHED");
  private static final long OBSERVE_INTERVAL_MS = 30_000;
  private static final long MAX_BACKOFF_MS = 15 * 60_000;
  private static final int MAX_READ_FAILURES = 5;
  private final ManagedFlinkStore store;
  private final ManagedFlinkCatalog catalog;
  private final TransactionTemplate tx;

  ManagedFlinkReconciler(
      ManagedFlinkStore store, ManagedFlinkCatalog catalog, PlatformTransactionManager manager) {
    this.store = store;
    this.catalog = catalog;
    tx = new TransactionTemplate(manager);
  }

  @Scheduled(fixedDelayString = "${streampark.managed-flink.reconcile-delay-ms:5000}")
  void refresh() {
    long now = System.currentTimeMillis();
    for (Map<String, Object> job :
        store
            .jdbc()
            .queryForList(
                "select * from t_flink_managed_job where provider_job_id is not null and archived=false "
                    + "and pending_operation_id is null and next_observe_at<=? order by next_observe_at,app_id limit 16",
                now)) {
      long app = number(job, "app_id");
      long team = number(job, "team_id");
      long next = System.currentTimeMillis() + OBSERVE_INTERVAL_MS;
      if (store
              .jdbc()
              .update(
                  "update t_flink_managed_job set next_observe_at=? "
                      + "where app_id=? and next_observe_at=? and pending_operation_id is null",
                  next,
                  app,
                  number(job, "next_observe_at"))
          == 0) {
        continue;
      }
      CloudState observed;
      try {
        Map<String, Object> env = catalog.environment(team, text(job, "environment_id"), false);
        Map<String, Object> conn = catalog.connection(team, text(env, "connection_id"));
        observed =
            catalog
                .provider(text(conn, "provider"))
                .inspect(catalog.jobContext(conn, env, null, job), text(job, "provider_job_id"));
      } catch (RuntimeException e) {
        log.info("Managed Flink read failed for app {} ({})", app, e.getClass().getSimpleName());
        observed = new CloudState(ReadStatus.STALE, null, null, null, null, null, 0);
      }
      if (observed == null) {
        continue;
      }
      CloudState state = observed;
      tx.executeWithoutResult(
          t -> {
            Map<String, Object> current =
                store.find(
                    "select * from t_flink_managed_job where app_id=? "
                        + "and next_observe_at=? and pending_operation_id is null and archived=false for update",
                    app,
                    next);
            if (current != null
                && Objects.equals(text(current, "observed_at"), text(job, "observed_at"))) {
              apply(current, state, System.currentTimeMillis());
            }
          });
    }
  }

  void apply(Map<String, Object> job, CloudState state, long now) {
    long app = number(job, "app_id");
    if (state.getStatus() != ReadStatus.FOUND) {
      recordReadFailure(job, state.getStatus(), state.getRetryNotBefore(), now);
      return;
    }
    if (!Objects.equals(state.getJobId(), text(job, "provider_job_id"))) {
      return;
    }
    boolean hasInstance = state.getInstanceId() != null && !state.getInstanceId().isBlank();
    if (!hasInstance && job.get("current_run_id") != null) {
      Map<String, Object> current =
          store.find(
              "select run_state from t_flink_managed_run where app_id=? and id=?",
              app,
              job.get("current_run_id"));
      if (current == null || !TERMINAL.contains(text(current, "run_state"))) {
        recordReadFailure(job, ReadStatus.STALE, 0, now);
        return;
      }
    }
    if (hasInstance && olderInstance(job, state)) {
      if (!TERMINAL.contains(state.getRunState())) {
        recordReadFailure(job, ReadStatus.STALE, 0, now);
      }
      return;
    }
    Map<String, Object> release =
        store.find(
            "select id,provider_version from t_flink_managed_release where app_id=? and id=?",
            app,
            job.get("active_release_id"));
    boolean match =
        release != null
            && state.getVersion() != null
            && Objects.equals(state.getVersion(), text(release, "provider_version"));
    store
        .jdbc()
        .update(
            "update t_flink_managed_job set sync_status=?,observed_at=?,missing_reads=0,"
                + "read_failures=0,next_observe_at=? where app_id=?",
            match ? "FRESH" : "DRIFTED",
            now,
            now + OBSERVE_INTERVAL_MS,
            app);
    if (hasInstance) {
      if (!state.getTerminalInstances().isEmpty()) {
        for (Map<String, Object> unfinished :
            store
                .jdbc()
                .queryForList(
                    "select provider_instance_id from t_flink_managed_run where app_id=? "
                        + "and ended_at is null and provider_instance_id<>?",
                    app,
                    state.getInstanceId())) {
          String id = text(unfinished, "provider_instance_id");
          String terminal = state.getTerminalInstances().get(id);
          if (TERMINAL.contains(terminal)) {
            store
                .jdbc()
                .update(
                    "update t_flink_managed_run set run_state=?,raw_state=?,"
                        + "ended_at=?,observed_at=? where app_id=? and provider_instance_id=? and ended_at is null",
                    terminal,
                    terminal,
                    now,
                    now,
                    app,
                    id);
          }
        }
      }
      projectRun(app, state, now);
    }
  }

  private void recordReadFailure(
      Map<String, Object> job, ReadStatus result, long retryNotBefore, long now) {
    long missing = result == ReadStatus.MISSING ? Math.min(2, number(job, "missing_reads") + 1) : 0;
    String status = "STALE";
    if (missing >= 2) {
      status = "MISSING";
    } else if (result == ReadStatus.AUTH_ERROR || result == ReadStatus.THROTTLED) {
      status = result.name();
    }
    String current = text(job, "sync_status");
    // Preserve a confirmed drift or missing state through transport failures.
    if ("MISSING".equals(current) || "DRIFTED".equals(current)) {
      status = current;
    }
    int failures = (int) Math.min(MAX_READ_FAILURES, number(job, "read_failures") + 1);
    long delay = Math.min(MAX_BACKOFF_MS, OBSERVE_INTERVAL_MS << failures);
    long jitter = ThreadLocalRandom.current().nextLong(delay / 5 + 1);
    long next = Math.max(retryNotBefore, now + Math.min(MAX_BACKOFF_MS, delay + jitter));
    store
        .jdbc()
        .update(
            "update t_flink_managed_job set sync_status=?,missing_reads=?,read_failures=?,"
                + "next_observe_at=? where app_id=?",
            status,
            missing,
            failures,
            next,
            number(job, "app_id"));
  }

  private boolean olderInstance(Map<String, Object> job, CloudState state) {
    if (job.get("current_run_id") == null) {
      return false;
    }
    Map<String, Object> active =
        store.find(
            "select provider_instance_id,started_at "
                + "from t_flink_managed_run where app_id=? and id=?",
            job.get("app_id"),
            job.get("current_run_id"));
    if (active == null || state.getInstanceId().equals(text(active, "provider_instance_id"))) {
      return false;
    }
    long currentStartedAt = number(active, "started_at");
    if (state.getStartedAt() > 0 && state.getStartedAt() < currentStartedAt) {
      return true;
    }
    Map<String, Object> observed =
        store.find(
            "select started_at from t_flink_managed_run "
                + "where app_id=? and provider_instance_id=?",
            job.get("app_id"),
            state.getInstanceId());
    return observed != null && number(observed, "started_at") <= currentStartedAt;
  }

  private void projectRun(long app, CloudState state, long now) {
    String instance = state.getInstanceId();
    String runState =
        Arrays.asList(
                    "STARTING",
                    "RUNNING",
                    "RESTARTING",
                    "STOPPING",
                    "STOPPED",
                    "FAILED",
                    "FINISHED")
                .contains(state.getRunState())
            ? state.getRunState()
            : "UNKNOWN";
    boolean ended = TERMINAL.contains(runState);
    Long endedAt = ended ? now : null;
    Map<String, Object> row =
        store.find(
            "select id,started_at,ended_at,runtime_application_id from t_flink_managed_run "
                + "where app_id=? and provider_instance_id=?",
            app,
            instance);
    long cloudStart = state.getStartedAt();
    long started = cloudStart > 0 && cloudStart <= now ? cloudStart : now;
    String run = row == null ? id() : text(row, "id");
    String runtimeId = state.getRuntimeId();
    if (runtimeId != null && runtimeId.isBlank()) {
      runtimeId = null;
    }
    if (row != null && runtimeId != null) {
      String recorded = text(row, "runtime_application_id");
      if (recorded == null || recorded.isBlank()) {
        store
            .jdbc()
            .update(
                "update t_flink_managed_run set runtime_application_id=? where id=?",
                runtimeId,
                run);
      }
    }
    if (row == null) {
      // Job head revision is insufficient evidence for the version of an external instance.
      store
          .jdbc()
          .update(
              "insert into t_flink_managed_run (id,app_id,provider_instance_id,"
                  + "runtime_application_id,run_state,raw_state,origin,started_at,ended_at,observed_at) "
                  + "values (?,?,?,?,?,?,'EXTERNAL',?,?,?)",
              run,
              app,
              instance,
              runtimeId,
              runState,
              state.getRunState(),
              started,
              endedAt,
              now);
    } else if (row.get("ended_at") == null) {
      started = number(row, "started_at");
      store
          .jdbc()
          .update(
              "update t_flink_managed_run set run_state=?,raw_state=?,ended_at=?,"
                  + "observed_at=? where id=?",
              runState,
              state.getRunState(),
              endedAt,
              now,
              run);
    } else {
      return;
    }
    store.jdbc().update("update t_flink_managed_job set current_run_id=? where app_id=?", run, app);
    FlinkAppState projection;
    switch (runState) {
      case "RUNNING":
        projection = FlinkAppState.RUNNING;
        break;
      case "STOPPED":
        projection = FlinkAppState.CANCELED;
        break;
      case "FAILED":
        projection = FlinkAppState.FAILED;
        break;
      case "FINISHED":
        projection = FlinkAppState.FINISHED;
        break;
      default:
        return;
    }
    store
        .jdbc()
        .update(
            "update t_flink_app set state=?,start_time=?,end_time=? where id=?",
            projection.getValue(),
            new Date(started),
            ended ? new Date(now) : null,
            app);
  }
}
