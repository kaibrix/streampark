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

import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.id;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.number;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.text;

@Slf4j
@Component
@ConditionalOnProperty(name = "streampark.managed-flink.enabled", havingValue = "true")
class ManagedFlinkReconciler {

  private static final List<String> TERMINAL = Arrays.asList("STOPPED", "FAILED", "FINISHED");
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
      long next = now + 30_000;
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
      Map<String, Object> env = catalog.environment(team, text(job, "environment_id"), false);
      Map<String, Object> conn = catalog.connection(team, text(env, "connection_id"));
      CloudState observed;
      try {
        observed =
            catalog
                .provider(text(conn, "provider"))
                .inspect(catalog.context(conn, env, null), text(job, "provider_job_id"));
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
              apply(current, state, now);
            }
          });
    }
  }

  void apply(Map<String, Object> job, CloudState state, long now) {
    long app = number(job, "app_id");
    if (state.getStatus() != ReadStatus.FOUND) {
      long missing = state.getStatus() == ReadStatus.MISSING ? number(job, "missing_reads") + 1 : 0;
      String status =
          missing >= 2
              ? "MISSING"
              : state.getStatus() == ReadStatus.AUTH_ERROR ? "AUTH_ERROR" : "STALE";
      // Preserve a previously confirmed drift or missing state through transport failures.
      if (Arrays.asList("MISSING", "DRIFTED").contains(text(job, "sync_status"))) {
        status = text(job, "sync_status");
      }
      store
          .jdbc()
          .update(
              "update t_flink_managed_job set sync_status=?,missing_reads=? where app_id=?",
              status,
              missing,
              app);
      return;
    }
    if (!Objects.equals(state.getJobId(), text(job, "provider_job_id"))) {
      return;
    }
    if (olderInstance(job, state)) {
      if (!TERMINAL.contains(state.getRunState())
          && !Arrays.asList("MISSING", "DRIFTED").contains(text(job, "sync_status"))) {
        // Do not replace newer history or report it as fresh when an older run appears active.
        store
            .jdbc()
            .update("update t_flink_managed_job set sync_status='STALE' where app_id=?", app);
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
            "update t_flink_managed_job set sync_status=?,observed_at=?,missing_reads=0 "
                + "where app_id=?",
            match ? "FRESH" : "DRIFTED",
            now,
            app);
    if (state.getInstanceId() != null && !state.getInstanceId().trim().isEmpty()) {
      projectRun(app, state, now);
    }
  }

  private boolean olderInstance(Map<String, Object> job, CloudState state) {
    if (state.getInstanceId() == null || job.get("current_run_id") == null) {
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
            "select id,ended_at from t_flink_managed_run "
                + "where app_id=? and provider_instance_id=?",
            app,
            instance);
    long started = state.getStartedAt() > 0 ? state.getStartedAt() : now;
    String run = row == null ? id() : text(row, "id");
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
              state.getRuntimeId(),
              runState,
              state.getRunState(),
              started,
              endedAt,
              now);
    } else if (row.get("ended_at") == null) {
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
