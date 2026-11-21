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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Action;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CloudState;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Context;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.FrozenCommand;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Outcome;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.ReadStatus;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
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
class ManagedFlinkWorker {

  private static final long LEASE_MS = 60_000;
  private final ManagedFlinkStore store;
  private final ManagedFlinkCatalog catalog;
  private final ManagedFlinkJobs jobs;
  private final TransactionTemplate tx;
  private final String owner = id();

  ManagedFlinkWorker(
      ManagedFlinkStore store,
      ManagedFlinkCatalog catalog,
      ManagedFlinkJobs jobs,
      PlatformTransactionManager manager) {
    this.store = store;
    this.catalog = catalog;
    this.jobs = jobs;
    this.tx = new TransactionTemplate(manager);
  }

  @Scheduled(fixedDelayString = "${streampark.managed-flink.worker-delay-ms:1000}")
  void tick() {
    long now = System.currentTimeMillis();
    List<Map<String, Object>> due =
        store.sql.queryForList(
            "select id from t_flink_managed_operation where status in ('PENDING','EXECUTING','WAITING_PROVIDER') "
                + "and next_run_at<=? and (lease_until is null or lease_until<=?) "
                + "order by next_run_at, id limit 16",
            now,
            now);
    for (Map<String, Object> row : due) {
      String operation = text(row, "id");
      try {
        advance(operation);
      } catch (RuntimeException e) {
        log.warn(
            "Managed Flink operation {} was interrupted ({})",
            operation,
            e.getClass().getSimpleName());
      }
    }
  }

  void advance(String operation) {
    Map<String, Object> op = claim(operation);
    if (op == null) {
      return;
    }
    long app = number(op, "app_id");
    long team = number(op, "team_id");
    long epoch = number(op, "lease_epoch");
    int index = (int) number(op, "step_index");
    FrozenCommand command = store.decode(text(op, "command"), FrozenCommand.class);
    Map<String, Object> job = jobs.job(team, app, false);
    Action action = command.getActions().get(index);
    OperationStep step =
        new OperationStep(
            operation,
            app,
            action,
            command,
            text(job, "provider_draft_id"),
            text(job, "provider_job_id"),
            staged(operation));
    Map<String, Object> attempt = attempt(operation, index);
    if (System.currentTimeMillis() >= number(op, "deadline_at")) {
      tx.executeWithoutResult(
          t -> {
            if (owns(operation, epoch)) {
              finish(op, attempt == null ? "FAILED" : "VERIFY_REQUIRED");
            }
          });
      return;
    }
    Map<String, Object> env = catalog.environment(team, text(job, "environment_id"), false);
    Map<String, Object> connection = catalog.connection(team, text(env, "connection_id"));
    ManagedFlinkProvider provider = catalog.provider(text(connection, "provider"));
    Context context;
    try {
      context = catalog.context(connection, env, command.getTarget());
    } catch (RuntimeException e) {
      tx.executeWithoutResult(
          t -> {
            if (owns(operation, epoch)) {
              finish(op, attempt == null ? "FAILED" : "VERIFY_REQUIRED");
            }
          });
      return;
    }
    if (attempt == null) {
      String conflict = preflight(provider, context, step, job);
      if (conflict != null) {
        tx.executeWithoutResult(
            t -> {
              if (owns(operation, epoch)) {
                if (!"ARTIFACT_UNREADABLE".equals(conflict)) {
                  store.sql.update(
                      "update t_flink_managed_job set sync_status=? where app_id=?", conflict, app);
                }
                store.sql.update(
                    "insert into t_flink_managed_operation_step "
                        + "(operation_id,step_index,action,state,error_code,started_at,finished_at) "
                        + "values (?,?,?,'FAILED',?,?,?)",
                    operation,
                    index,
                    action.name(),
                    conflict,
                    System.currentTimeMillis(),
                    System.currentTimeMillis());
                finish(op, "FAILED");
              }
            });
        return;
      }
      if (!Boolean.TRUE.equals(tx.execute(t -> markDispatch(step, epoch, index)))) {
        return;
      }
      Observation response;
      try {
        response = provider.dispatch(context, step);
      } catch (RuntimeException e) {
        response = Observation.outcome(Outcome.UNKNOWN, "RESULT_UNCERTAIN");
      }
      Observation ack = response;
      tx.executeWithoutResult(
          t -> {
            if (owns(operation, epoch)) {
              updateAttempt(step, index, ack, "WAITING_PROVIDER");
              if (ack.getOutcome() == Outcome.REJECTED) {
                updateAttempt(step, index, ack, "FAILED");
                finish(op, "FAILED");
              } else {
                defer(operation);
              }
            }
          });
      return;
    }
    Observation response;
    try {
      String persisted = text(attempt, "result");
      response =
          provider.observe(
              context, step, persisted == null ? null : store.decode(persisted, Observation.class));
    } catch (RuntimeException e) {
      response = Observation.outcome(Outcome.UNKNOWN, "OBSERVATION_FAILED");
    }
    Observation evidence = response;
    tx.executeWithoutResult(
        t -> {
          if (owns(operation, epoch)) {
            apply(op, job, step, index, evidence);
          }
        });
  }

  private String preflight(
      ManagedFlinkProvider provider, Context context, OperationStep step, Map<String, Object> job) {
    if (step.getAction() == Action.STAGE_ARTIFACT) {
      return artifactMatches(step.getCommand()) ? null : "ARTIFACT_UNREADABLE";
    }
    if (step.getProviderJobId() == null) {
      return null;
    }
    CloudState state;
    try {
      state = provider.inspect(context, step.getProviderJobId());
    } catch (RuntimeException e) {
      return "STALE";
    }
    if (state == null) {
      return null;
    }
    if (state.getStatus() != ReadStatus.FOUND) {
      return state.getStatus() == ReadStatus.AUTH_ERROR ? "AUTH_ERROR" : "STALE";
    }
    Map<String, Object> release = jobs.release(step.getAppId(), text(job, "active_release_id"));
    if (!Objects.equals(step.getProviderJobId(), state.getJobId())
        || !Objects.equals(text(release, "provider_version"), state.getVersion())) {
      return "DRIFTED";
    }
    boolean stopped =
        state.getInstanceId() == null
            || java.util.Arrays.asList("STOPPED", "FAILED", "FINISHED")
                .contains(state.getRunState());
    if (step.getAction() == Action.STOP || step.getAction() == Action.SNAPSHOT) {
      return !stopped
              && Objects.equals(step.getCommand().getProviderInstance(), state.getInstanceId())
          ? null
          : "DRIFTED";
    }
    return stopped ? null : "DRIFTED";
  }

  private boolean artifactMatches(FrozenCommand command) {
    if (command.getArtifactRef() == null || command.getArtifactSize() == null) {
      return false;
    }
    Path path = Paths.get(command.getArtifactRef());
    try (InputStream input = Files.newInputStream(path)) {
      MessageDigest digest = ManagedFlinkStore.sha256();
      byte[] buffer = new byte[8192];
      long total = 0;
      int count;
      while ((count = input.read(buffer)) != -1) {
        total += count;
        if (total > command.getArtifactSize()) {
          return false;
        }
        digest.update(buffer, 0, count);
      }
      return total == command.getArtifactSize()
          && Objects.equals(command.getArtifactDigest(), ManagedFlinkStore.hex(digest.digest()));
    } catch (IOException e) {
      return false;
    }
  }

  private Map<String, Object> claim(String operation) {
    long now = System.currentTimeMillis();
    return tx.execute(
        t -> {
          int changed =
              store.sql.update(
                  "update t_flink_managed_operation set status='EXECUTING',"
                      + "lease_owner=?,lease_until=?,lease_epoch=lease_epoch+1,updated_at=? where id=? "
                      + "and status in ('PENDING','EXECUTING','WAITING_PROVIDER') and next_run_at<=? "
                      + "and (lease_until is null or lease_until<=?)",
                  owner,
                  now + LEASE_MS,
                  now,
                  operation,
                  now,
                  now);
          return changed == 0
              ? null
              : store.require("select * from t_flink_managed_operation where id=?", operation);
        });
  }

  private boolean owns(String operation, long epoch) {
    return store.find(
            "select id from t_flink_managed_operation where id=? and lease_owner=? "
                + "and lease_epoch=? and lease_until>? and status='EXECUTING' for update",
            operation,
            owner,
            epoch,
            System.currentTimeMillis())
        != null;
  }

  private Map<String, Object> attempt(String operation, int index) {
    return store.find(
        "select * from t_flink_managed_operation_step where operation_id=? and step_index=?",
        operation,
        index);
  }

  private String staged(String operation) {
    Map<String, Object> stage =
        store.find(
            "select result from t_flink_managed_operation_step "
                + "where operation_id=? and action='STAGE_ARTIFACT' and state='CONFIRMED'",
            operation);
    return stage == null
        ? null
        : store.decode(text(stage, "result"), Observation.class).getArtifactRef();
  }

  private boolean markDispatch(OperationStep step, long epoch, int index) {
    if (!owns(step.getOperationId(), epoch)) {
      return false;
    }
    store.sql.update(
        "insert into t_flink_managed_operation_step "
            + "(operation_id,step_index,action,state,started_at) values (?,?,?,'DISPATCHING',?)",
        step.getOperationId(),
        index,
        step.getAction().name(),
        System.currentTimeMillis());
    return true;
  }

  private void apply(
      Map<String, Object> op,
      Map<String, Object> job,
      OperationStep step,
      int index,
      Observation evidence) {
    if (evidence.getOutcome() == Outcome.REJECTED) {
      updateAttempt(step, index, evidence, "FAILED");
      finish(op, "FAILED");
      return;
    }
    if (evidence.getOutcome() != Outcome.CONFIRMED || !project(op, job, step, evidence)) {
      // Preserve the acknowledged identity if a temporary read failure returned no evidence.
      store.sql.update(
          "update t_flink_managed_operation_step set error_code=? "
              + "where operation_id=? and step_index=?",
          safe(evidence.getCode(), 64),
          step.getOperationId(),
          index);
      defer(step.getOperationId());
      return;
    }
    updateAttempt(step, index, evidence, "CONFIRMED");
    if (step.getAction() == Action.START && !"RUNNING".equals(evidence.getRunState())) {
      finish(op, "FAILED");
    } else if (index + 1 == step.getCommand().getActions().size()) {
      finish(op, "SUCCEEDED");
    } else {
      store.sql.update(
          "update t_flink_managed_operation set step_index=step_index+1 where id=?",
          step.getOperationId());
      defer(step.getOperationId());
    }
  }

  private boolean project(
      Map<String, Object> op, Map<String, Object> job, OperationStep step, Observation result) {
    long app = step.getAppId();
    long now = System.currentTimeMillis();
    String release = text(op, "release_id");
    switch (step.getAction()) {
      case STAGE_ARTIFACT:
        return hasId(result.getArtifactRef(), 768);
      case CREATE_DRAFT:
        if (!hasId(result.getDraftId(), 191)) {
          return false;
        }
        store.sql.update(
            "update t_flink_managed_job set provider_draft_id=? where app_id=?",
            result.getDraftId(),
            app);
        return true;
      case LOCK_DRAFT:
        return step.getDraftId() != null && step.getDraftId().equals(result.getDraftId());
      case UPDATE_DRAFT:
        return Objects.equals(step.getDraftId(), result.getDraftId())
            && step.getDraftId() != null
            && Objects.equals(step.getCommand().getSpecHash(), result.getSpecHash());
      case PUBLISH:
        if (!hasId(result.getJobId(), 191)
            || !hasId(result.getVersion(), 191)
            || !Objects.equals(step.getCommand().getSpecHash(), result.getSpecHash())
            || (step.getProviderJobId() != null
                && !step.getProviderJobId().equals(result.getJobId()))) {
          return false;
        }
        store.sql.update(
            "update t_flink_managed_release set provider_version=?,artifact_ref=?,status='PUBLISHED' "
                + "where id=?",
            result.getVersion(),
            step.getStagedArtifact(),
            release);
        store.sql.update(
            "update t_flink_managed_job set provider_job_id=?,active_release_id=?,"
                + "observed_at=?,sync_status='FRESH' where app_id=?",
            result.getJobId(),
            release,
            now,
            app);
        store.sql.update("update t_flink_app set release=0 where id=?", app);
        return true;
      case START:
        return start(job, step, result, release, now);
      case STOP:
        if (!sameInstance(step, result) || !"STOPPED".equals(result.getRunState())) {
          return false;
        }
        store.sql.update(
            "update t_flink_managed_run set run_state='STOPPED',raw_state='STOPPED',"
                + "ended_at=?,observed_at=? where app_id=? and provider_instance_id=?",
            now,
            now,
            app,
            result.getInstanceId());
        store.sql.update(
            "update t_flink_app set state=?,end_time=? where id=?",
            FlinkAppState.CANCELED.getValue(),
            new Date(now),
            app);
        return true;
      case SNAPSHOT:
        if (!sameInstance(step, result)
            || !hasId(result.getRecordId(), 191)
            || !hasId(result.getRestoreRef(), 191)) {
          return false;
        }
        Map<String, Object> current = jobs.currentRun(job);
        if (current == null
            || !Objects.equals(result.getInstanceId(), text(current, "provider_instance_id"))) {
          return false;
        }
        store.sql.update(
            "insert into t_flink_managed_snapshot (id,app_id,run_id,release_id,"
                + "operation_id,provider_record_id,restore_ref,state,restorable,created_at,observed_at) "
                + "values (?,?,?,?,?,?,?,'AVAILABLE',true,?,?)",
            id(),
            app,
            current.get("id"),
            current.get("release_id"),
            step.getOperationId(),
            result.getRecordId(),
            result.getRestoreRef(),
            now,
            now);
        return true;
      default:
        return false;
    }
  }

  private boolean start(
      Map<String, Object> job, OperationStep step, Observation result, String release, long now) {
    String runState = result.getRunState();
    boolean running = "RUNNING".equals(runState);
    if ((!running && !"FAILED".equals(runState))
        || !hasId(result.getInstanceId(), 191)
        || !hasId(step.getProviderJobId(), 191)
        || !step.getProviderJobId().equals(result.getJobId())
        || !Objects.equals(step.getCommand().getProviderVersion(), result.getVersion())
        || (running
            && !Objects.equals(step.getCommand().getRestoreRef(), result.getRestoredFrom()))) {
      return false;
    }
    long app = number(job, "app_id");
    String run = id();
    store.sql.update(
        "insert into t_flink_managed_run (id,app_id,release_id,provider_instance_id,"
            + "runtime_application_id,run_state,raw_state,origin,started_at,ended_at,observed_at) "
            + "values (?,?,?,?,?,?,?,'STREAMPARK',?,?,?)",
        run,
        app,
        release,
        result.getInstanceId(),
        result.getRuntimeId(),
        runState,
        runState,
        now,
        running ? null : now,
        now);
    store.sql.update(
        "update t_flink_managed_job set current_run_id=?,observed_at=?,sync_status='FRESH' "
            + "where app_id=?",
        run,
        now,
        app);
    store.sql.update(
        "update t_flink_app set state=?,start_time=?,end_time=? where id=?",
        running ? FlinkAppState.RUNNING.getValue() : FlinkAppState.FAILED.getValue(),
        new Date(now),
        running ? null : new Date(now),
        app);
    return true;
  }

  private boolean sameInstance(OperationStep step, Observation result) {
    return hasId(step.getProviderJobId(), 191)
        && step.getProviderJobId().equals(result.getJobId())
        && hasId(step.getCommand().getProviderInstance(), 191)
        && step.getCommand().getProviderInstance().equals(result.getInstanceId());
  }

  private void updateAttempt(OperationStep step, int index, Observation evidence, String state) {
    store.sql.update(
        "update t_flink_managed_operation_step set state=?,"
            + "request_id=coalesce(?,request_id),result=?,error_code=?,finished_at=? "
            + "where operation_id=? and step_index=?",
        state,
        safe(evidence.getRequestId(), 128),
        store.encode(evidence),
        safe(evidence.getCode(), 64),
        "WAITING_PROVIDER".equals(state) ? null : System.currentTimeMillis(),
        step.getOperationId(),
        index);
  }

  private void finish(Map<String, Object> op, String status) {
    String operation = text(op, "id");
    store.sql.update(
        "update t_flink_managed_operation set status=?,lease_owner=null,lease_until=null,"
            + "updated_at=? where id=?",
        status,
        System.currentTimeMillis(),
        operation);
    if (!"VERIFY_REQUIRED".equals(status)) {
      store.sql.update(
          "update t_flink_managed_job set pending_operation_id=null "
              + "where app_id=? and pending_operation_id=?",
          op.get("app_id"),
          operation);
    }
    if (!"SUCCEEDED".equals(status)
        && ("PUBLISH".equals(text(op, "command_type"))
            || "ROLLBACK".equals(text(op, "command_type")))) {
      store.sql.update(
          "update t_flink_managed_release set status=? where id=? and status<>'PUBLISHED'",
          status,
          op.get("release_id"));
    }
  }

  private void defer(String operation) {
    long now = System.currentTimeMillis();
    store.sql.update(
        "update t_flink_managed_operation set status='WAITING_PROVIDER',"
            + "lease_owner=null,lease_until=null,next_run_at=?,updated_at=? where id=?",
        now + 2000,
        now,
        operation);
  }

  private boolean hasId(String value, int max) {
    return value != null && !value.trim().isEmpty() && value.length() <= max;
  }

  private String safe(String value, int max) {
    return hasId(value, max) && value.matches("[a-zA-Z0-9._-]+") ? value : null;
  }
}
