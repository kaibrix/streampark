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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.DependencyArtifact;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.FrozenCommand;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Outcome;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.ResolutionAudit;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.ResolutionDecision;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.PreDestroy;

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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
  private final ScheduledThreadPoolExecutor renewals;
  private final ThreadPoolExecutor dispatcher =
      new ThreadPoolExecutor(
          0,
          1,
          1,
          TimeUnit.MINUTES,
          new SynchronousQueue<>(),
          task -> new Thread(task, "managed-flink-dispatch"),
          new ThreadPoolExecutor.DiscardPolicy());

  ManagedFlinkWorker(
      ManagedFlinkStore store,
      ManagedFlinkCatalog catalog,
      ManagedFlinkJobs jobs,
      PlatformTransactionManager manager) {
    this.store = store;
    this.catalog = catalog;
    this.jobs = jobs;
    this.tx = new TransactionTemplate(manager);
    this.renewals =
        new ScheduledThreadPoolExecutor(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "managed-flink-lease");
              thread.setDaemon(true);
              return thread;
            });
    renewals.setRemoveOnCancelPolicy(true);
  }

  @PreDestroy
  void stopRenewals() {
    dispatcher.shutdownNow();
    renewals.shutdownNow();
  }

  @Scheduled(fixedDelayString = "${streampark.managed-flink.worker-delay-ms:1000}")
  void tick() {
    dispatcher.execute(this::processDue);
  }

  private void processDue() {
    long now = System.currentTimeMillis();
    List<Map<String, Object>> due =
        store
            .jdbc()
            .queryForList(
                "select id from t_flink_managed_operation where status in ('PENDING','EXECUTING','WAITING_PROVIDER') "
                    + "and next_run_at<=? and (lease_until is null or lease_until<=?) "
                    + "order by next_run_at, id limit 16",
                now,
                now);
    for (Map<String, Object> row : due) {
      if (dispatcher.isShutdown()) {
        return;
      }
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
    advance(operation, LEASE_MS);
  }

  void advance(String operation, long leaseMs) {
    Map<String, Object> op = claim(operation, leaseMs);
    if (op == null) {
      return;
    }
    long epoch = number(op, "lease_epoch");
    long interval = Math.max(1, leaseMs / 3);
    ScheduledFuture<?> renewal =
        renewals.scheduleWithFixedDelay(
            () -> renewLease(operation, epoch, leaseMs), interval, interval, TimeUnit.MILLISECONDS);
    try {
      advanceClaimed(operation, op, epoch);
    } finally {
      renewal.cancel(false);
    }
  }

  private void advanceClaimed(String operation, Map<String, Object> op, long epoch) {
    long app = number(op, "app_id");
    long team = number(op, "team_id");
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
    if (action == Action.STAGE_DEPENDENCY) {
      // Dependency stages are prepended in classpath order when the release is frozen.
      step.setDependency(command.getDependencies().get(index));
    }
    Map<String, Object> attempt = attempt(operation, index);
    if (command.isReconcileOnly() && attempt == null) {
      tx.executeWithoutResult(
          t -> {
            if (owns(operation, epoch)) {
              finish(op, "VERIFY_REQUIRED");
            }
          });
      return;
    }
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
    Context preflightContext;
    try {
      // Stop and snapshot commands queued by earlier versions may carry an edited environment.
      String type = text(op, "command_type");
      boolean runningRelease = "STOP".equals(type) || "SNAPSHOT".equals(type);
      context =
          catalog.jobContext(
              connection,
              env,
              runningRelease && op.get("release_id") != null
                  ? jobs.releaseTarget(app, text(op, "release_id"))
                  : command.getTarget(),
              job);
      // Before republishing to an edited target, verify the existing cloud head in its own pool.
      preflightContext =
          attempt == null
                  && step.getProviderJobId() != null
                  && (action == Action.UPDATE_DRAFT || action == Action.PUBLISH)
              ? catalog.jobContext(connection, env, null, job)
              : context;
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
      if (action == Action.UPDATE_DRAFT) {
        bindDraftEvidence(step);
      }
      String conflict = preflight(provider, preflightContext, step, job);
      if (conflict != null) {
        tx.executeWithoutResult(
            t -> {
              if (owns(operation, epoch)) {
                if (!"ARTIFACT_UNREADABLE".equals(conflict)) {
                  store
                      .jdbc()
                      .update(
                          "update t_flink_managed_job set sync_status=? where app_id=?",
                          conflict,
                          app);
                }
                failBeforeDispatch(
                    op, step, index, Observation.outcome(Outcome.REJECTED, conflict));
              }
            });
        return;
      }
      Observation response;
      try {
        response =
            provider.dispatch(
                context,
                step,
                () -> Boolean.TRUE.equals(tx.execute(t -> markDispatch(step, op, epoch, index))));
      } catch (RuntimeException e) {
        response = Observation.outcome(Outcome.UNKNOWN, "RESULT_UNCERTAIN");
      }
      Observation ack = response;
      tx.executeWithoutResult(
          t -> {
            if (owns(operation, epoch)) {
              if (attempt(operation, index) == null) {
                failBeforeDispatch(op, step, index, ack);
                return;
              }
              updateAttempt(step, index, ack, "WAITING_PROVIDER");
              if (ack.getOutcome() == Outcome.REJECTED) {
                updateAttempt(step, index, ack, "FAILED");
                finish(op, "FAILED");
              } else {
                defer(op, ack.getRetryNotBefore());
              }
            } else if (ack.getDraftId() != null
                || ack.getInstanceId() != null
                || ack.getJobId() != null
                || ack.getRequestId() != null) {
              // Keep the exact attempt's receipt for the current owner to verify.
              store
                  .jdbc()
                  .update(
                      "update t_flink_managed_operation_step set result=?,request_id=coalesce(request_id,?) "
                          + "where operation_id=? and step_index=? and action=? and state='DISPATCHING' and result is null",
                      store.encode(ack),
                      safe(ack.getRequestId(), 128),
                      operation,
                      index,
                      action.name());
            }
          });
      return;
    }
    Observation response;
    try {
      step.setDispatchedAt(number(attempt, "started_at"));
      response = provider.observe(context, step, readbackReceipt(step, index, attempt));
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

  private Observation readbackReceipt(OperationStep step, int index, Map<String, Object> attempt) {
    String persisted = text(attempt, "result");
    Observation receipt = persisted == null ? null : store.decode(persisted, Observation.class);
    if (step.getAction() != Action.CREATE_DRAFT
        || (receipt != null && receipt.getDraftId() != null)) {
      return receipt;
    }
    List<ResolutionAudit> history = step.getCommand().getReviewHistory();
    for (int i = history.size() - 1; i >= 0; i--) {
      ResolutionAudit decision = history.get(i);
      if (decision.getStepIndex() == index
          && decision.getDecision() == ResolutionDecision.CONFIRM_DRAFT) {
        if (receipt == null) {
          receipt = Observation.outcome(Outcome.WAITING, null);
        }
        receipt.setDraftId(decision.getDraftId());
        break;
      }
    }
    return receipt;
  }

  private String preflight(
      ManagedFlinkProvider provider, Context context, OperationStep step, Map<String, Object> job) {
    if (step.getAction() == Action.STAGE_DEPENDENCY) {
      DependencyArtifact dep = step.getDependency();
      return artifactMatches(dep.getLocalRef(), dep.getSize(), dep.getDigest())
          ? null
          : "ARTIFACT_UNREADABLE";
    }
    if (step.getAction() == Action.STAGE_ARTIFACT) {
      FrozenCommand cmd = step.getCommand();
      return artifactMatches(cmd.getArtifactRef(), cmd.getArtifactSize(), cmd.getArtifactDigest())
          ? null
          : "ARTIFACT_UNREADABLE";
    }
    if (step.getProviderJobId() == null) {
      if (step.getAction() == Action.SUBMIT) {
        step.getCommand().setInstanceBeforeDispatch("");
      }
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
    switch (state.getStatus()) {
      case AUTH_ERROR:
      case THROTTLED:
        return state.getStatus().name();
      case FOUND:
        break;
      default:
        return "STALE";
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
    if (step.getAction() == Action.STOP
        || step.getAction() == Action.STOP_WITH_SAVEPOINT
        || step.getAction() == Action.RESTART
        || step.getAction() == Action.SNAPSHOT) {
      return !stopped
              && Objects.equals(step.getCommand().getProviderInstance(), state.getInstanceId())
          ? null
          : "DRIFTED";
    }
    if (step.getAction() == Action.START
        && step.getCommand().getProviderInstance() != null
        && !Objects.equals(step.getCommand().getProviderInstance(), state.getInstanceId())) {
      return "DRIFTED";
    }
    if (stopped && (step.getAction() == Action.START || step.getAction() == Action.SUBMIT)) {
      step.getCommand().setInstanceBeforeDispatch(Objects.toString(state.getInstanceId(), ""));
    }
    return stopped ? null : "DRIFTED";
  }

  private boolean artifactMatches(String ref, Long size, String expectedDigest) {
    if (ref == null || size == null) {
      return false;
    }
    Path path = Paths.get(ref);
    try (InputStream input = Files.newInputStream(path)) {
      MessageDigest digest = ManagedFlinkStore.sha256();
      byte[] buffer = new byte[8192];
      long total = 0;
      int count;
      while ((count = input.read(buffer)) != -1) {
        total += count;
        if (total > size) {
          return false;
        }
        digest.update(buffer, 0, count);
      }
      return total == size
          && Objects.equals(expectedDigest, ManagedFlinkStore.hex(digest.digest()));
    } catch (IOException e) {
      return false;
    }
  }

  private Map<String, Object> claim(String operation, long leaseMs) {
    long now = System.currentTimeMillis();
    long until = now + leaseMs;
    return tx.execute(
        t -> {
          int changed =
              store
                  .jdbc()
                  .update(
                      "update t_flink_managed_operation set status='EXECUTING',"
                          + "lease_owner=?,lease_until=case when deadline_at>? and deadline_at<? "
                          + "then deadline_at else ? end,lease_epoch=lease_epoch+1,updated_at=? where id=? "
                          + "and status in ('PENDING','EXECUTING','WAITING_PROVIDER') and next_run_at<=? "
                          + "and (lease_until is null or lease_until<=?)",
                      owner,
                      now,
                      until,
                      until,
                      now,
                      operation,
                      now,
                      now);
          return changed == 0
              ? null
              : store.require("select * from t_flink_managed_operation where id=?", operation);
        });
  }

  private void renewLease(String operation, long epoch, long leaseMs) {
    try {
      tx.executeWithoutResult(
          t -> {
            Map<String, Object> lease = lockedLease(operation, epoch);
            if (lease == null) {
              return;
            }
            long now = System.currentTimeMillis();
            long previous = number(lease, "lease_until");
            long until = Math.min(number(lease, "deadline_at"), now + leaseMs);
            if (previous > now && until > previous) {
              store
                  .jdbc()
                  .update(
                      "update t_flink_managed_operation set lease_until=? where id=?",
                      until,
                      operation);
            }
          });
    } catch (RuntimeException e) {
      log.debug(
          "Could not renew managed Flink lease {} ({})", operation, e.getClass().getSimpleName());
    }
  }

  private boolean owns(String operation, long epoch) {
    Map<String, Object> lease = lockedLease(operation, epoch);
    return lease != null && number(lease, "lease_until") > System.currentTimeMillis();
  }

  private Map<String, Object> lockedLease(String operation, long epoch) {
    return store.find(
        "select lease_until,deadline_at from t_flink_managed_operation where id=? "
            + "and lease_owner=? and lease_epoch=? and status='EXECUTING' for update",
        operation,
        owner,
        epoch);
  }

  private Map<String, Object> attempt(String operation, int index) {
    return store.find(
        "select * from t_flink_managed_operation_step where operation_id=? and step_index=?",
        operation,
        index);
  }

  private void bindDraftEvidence(OperationStep step) {
    Map<String, Object> confirmed =
        store.find(
            "select s.result,s.action,s.operation_id,o.command from t_flink_managed_operation_step s "
                + "join t_flink_managed_operation o on o.id=s.operation_id "
                + "where o.app_id=? and s.state='CONFIRMED' and s.action in ('CREATE_DRAFT','UPDATE_DRAFT') "
                + "order by s.finished_at desc,o.created_at desc,s.step_index desc limit 1",
            step.getAppId());
    if (confirmed == null || text(confirmed, "result") == null) {
      return;
    }
    Observation proof = store.decode(text(confirmed, "result"), Observation.class);
    if (!Objects.equals(step.getDraftId(), proof.getDraftId())) {
      return;
    }
    step.setDraftFingerprint(proof.getDraftFingerprint());
    if (proof.getDraftFingerprint() == null && "UPDATE_DRAFT".equals(text(confirmed, "action"))) {
      String operation = text(confirmed, "operation_id");
      step.setPreviousDraftUpdate(
          new OperationStep(
              operation,
              step.getAppId(),
              Action.UPDATE_DRAFT,
              store.decode(text(confirmed, "command"), FrozenCommand.class),
              step.getDraftId(),
              step.getProviderJobId(),
              staged(operation)));
    }
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

  private boolean markDispatch(OperationStep step, Map<String, Object> op, long epoch, int index) {
    if (!owns(step.getOperationId(), epoch) || attempt(step.getOperationId(), index) != null) {
      return false;
    }
    if (System.currentTimeMillis() >= number(op, "deadline_at")) {
      failBeforeDispatch(
          op, step, index, Observation.outcome(Outcome.REJECTED, "OPERATION_DEADLINE_EXPIRED"));
      return false;
    }
    store
        .jdbc()
        .update(
            "insert into t_flink_managed_operation_step "
                + "(operation_id,step_index,action,state,started_at) values (?,?,?,'DISPATCHING',?)",
            step.getOperationId(),
            index,
            step.getAction().name(),
            System.currentTimeMillis());
    if (step.getCommand().getInstanceBeforeDispatch() != null
        || step.getCommand().getSavepointsBeforeStop() != null) {
      store
          .jdbc()
          .update(
              "update t_flink_managed_operation set command=? where id=?",
              store.encode(step.getCommand()),
              step.getOperationId());
    }
    return true;
  }

  private void failBeforeDispatch(
      Map<String, Object> op, OperationStep step, int index, Observation response) {
    String code =
        response.getOutcome() == Outcome.REJECTED ? response.getCode() : "STEP_NOT_DISPATCHED";
    long now = System.currentTimeMillis();
    store
        .jdbc()
        .update(
            "insert into t_flink_managed_operation_step "
                + "(operation_id,step_index,action,state,request_id,error_code,started_at,finished_at) "
                + "values (?,?,?,'FAILED',?,?,?,?)",
            step.getOperationId(),
            index,
            step.getAction().name(),
            safe(response.getRequestId(), 128),
            safe(code, 64),
            now,
            now);
    finish(op, "FAILED");
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
      store
          .jdbc()
          .update(
              "update t_flink_managed_operation_step set error_code=? "
                  + "where operation_id=? and step_index=?",
              safe(evidence.getCode(), 64),
              step.getOperationId(),
              index);
      defer(op, evidence.getRetryNotBefore());
      return;
    }
    if ((step.getAction() == Action.START
            || step.getAction() == Action.RESTART
            || step.getAction() == Action.SUBMIT)
        && ("FAILED".equals(evidence.getRunState()) || "STOPPED".equals(evidence.getRunState()))) {
      if (evidence.getCode() == null) {
        evidence.setCode(
            "STOPPED".equals(evidence.getRunState()) ? "RUNTIME_STOPPED" : "RUNTIME_FAILED");
      }
      updateAttempt(step, index, evidence, "FAILED");
      finish(op, "FAILED");
      return;
    }
    updateAttempt(step, index, evidence, "CONFIRMED");
    if (index + 1 == step.getCommand().getActions().size()) {
      finish(op, "SUCCEEDED");
    } else if (step.getCommand().isReconcileOnly()) {
      finish(op, "VERIFY_REQUIRED");
    } else {
      store
          .jdbc()
          .update(
              "update t_flink_managed_operation set step_index=step_index+1 where id=?",
              step.getOperationId());
      defer(op, 0);
    }
  }

  private boolean project(
      Map<String, Object> op, Map<String, Object> job, OperationStep step, Observation result) {
    long app = step.getAppId();
    long now = System.currentTimeMillis();
    String release = text(op, "release_id");
    switch (step.getAction()) {
      case STAGE_DEPENDENCY:
        if (!hasId(result.getArtifactRef(), 768)) {
          return false;
        }
        step.getDependency().setRemoteRef(result.getArtifactRef());
        store
            .jdbc()
            .update(
                "update t_flink_managed_operation set command=? where id=?",
                store.encode(step.getCommand()),
                step.getOperationId());
        return true;
      case STAGE_ARTIFACT:
        return hasId(result.getArtifactRef(), 768);
      case CREATE_DRAFT:
        if (!hasId(result.getDraftId(), 191)) {
          return false;
        }
        store
            .jdbc()
            .update(
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
      case SUBMIT:
      case PUBLISH:
        if (!hasId(result.getJobId(), 191)
            || !hasId(result.getVersion(), 191)
            || !Objects.equals(step.getCommand().getSpecHash(), result.getSpecHash())
            || (step.getProviderJobId() != null
                && !step.getProviderJobId().equals(result.getJobId()))) {
          return false;
        }
        if (step.getAction() == Action.SUBMIT && !start(job, step, result, release, now)) {
          return false;
        }
        store
            .jdbc()
            .update(
                "update t_flink_managed_release set provider_version=?,artifact_ref=?,status='PUBLISHED' "
                    + "where id=?",
                result.getVersion(),
                step.getStagedArtifact(),
                release);
        store
            .jdbc()
            .update(
                "update t_flink_managed_job set provider_job_id=?,active_release_id=?,"
                    + "observed_at=?,sync_status='FRESH' where app_id=?",
                result.getJobId(),
                release,
                now,
                app);
        store.markApplicationReleased(app);
        return true;
      case RESTART:
      case START:
        return hasId(step.getProviderJobId(), 191)
            && step.getProviderJobId().equals(result.getJobId())
            && Objects.equals(step.getCommand().getProviderVersion(), result.getVersion())
            && start(job, step, result, release, now);
      case STOP:
        if (!sameInstance(step, result)
            || !java.util.Arrays.asList("STOPPED", "FAILED", "FINISHED")
                .contains(result.getRunState())) {
          return false;
        }
        recordStopped(app, result, now);
        return true;
      case STOP_WITH_SAVEPOINT:
        if (!"STOPPED".equals(result.getRunState()) || !recordSnapshot(job, step, result, now)) {
          return false;
        }
        recordStopped(app, result, now);
        return true;
      case SNAPSHOT:
        return recordSnapshot(job, step, result, now);
      default:
        return false;
    }
  }

  private void recordStopped(long app, Observation result, long now) {
    store
        .jdbc()
        .update(
            "update t_flink_managed_run set run_state=?,raw_state=?,"
                + "ended_at=?,observed_at=? where app_id=? and provider_instance_id=?",
            result.getRunState(),
            result.getRunState(),
            now,
            now,
            app,
            result.getInstanceId());
    store
        .jdbc()
        .update(
            "update t_flink_app set state=?,end_time=? where id=? and exists "
                + "(select 1 from t_flink_managed_job j join t_flink_managed_run r "
                + "on r.app_id=j.app_id and r.id=j.current_run_id "
                + "where j.app_id=? and r.provider_instance_id=?)",
            ("STOPPED".equals(result.getRunState())
                    ? FlinkAppState.CANCELED
                    : FlinkAppState.of(result.getRunState()))
                .getValue(),
            new Date(now),
            app,
            app,
            result.getInstanceId());
  }

  private boolean recordSnapshot(
      Map<String, Object> job, OperationStep step, Observation result, long now) {
    if (!sameInstance(step, result)
        || !hasId(result.getRecordId(), 191)
        || !hasId(result.getRestoreRef(), 191)) {
      return false;
    }
    Map<String, Object> run =
        store.find(
            "select id,release_id from t_flink_managed_run where app_id=? and provider_instance_id=?",
            step.getAppId(),
            result.getInstanceId());
    if (run == null
        || (step.getAction() == Action.SNAPSHOT
            && !Objects.equals(run.get("id"), job.get("current_run_id")))) {
      return false;
    }
    store
        .jdbc()
        .update(
            "insert into t_flink_managed_snapshot (id,app_id,run_id,release_id,"
                + "operation_id,provider_record_id,restore_ref,state,restorable,created_at,observed_at) "
                + "values (?,?,?,?,?,?,?,'AVAILABLE',true,?,?)",
            id(),
            step.getAppId(),
            run.get("id"),
            run.get("release_id"),
            step.getOperationId(),
            result.getRecordId(),
            result.getRestoreRef(),
            now,
            now);
    return true;
  }

  private boolean start(
      Map<String, Object> job, OperationStep step, Observation result, String release, long now) {
    String runState = result.getRunState();
    boolean running = "RUNNING".equals(runState);
    boolean failed = "FAILED".equals(runState);
    if ((!running && !failed && !"STOPPED".equals(runState) && !"FINISHED".equals(runState))
        || !hasId(result.getInstanceId(), 191)
        || (!failed
            && !Objects.equals(step.getCommand().getRestoreRef(), result.getRestoredFrom()))) {
      return false;
    }
    long app = number(job, "app_id");
    boolean restarted = step.getAction() == Action.RESTART;
    if (restarted) {
      if (Objects.equals(step.getCommand().getProviderInstance(), result.getInstanceId())) {
        return false;
      }
      store
          .jdbc()
          .update(
              "update t_flink_managed_run set run_state='STOPPED',raw_state='STOPPED',"
                  + "ended_at=?,observed_at=? where app_id=? and provider_instance_id=?",
              now,
              now,
              app,
              step.getCommand().getProviderInstance());
    }
    long cloudStart = result.getInstanceStartedAt();
    long started = cloudStart > 0 && cloudStart <= now ? cloudStart : now;
    String run = id();
    store
        .jdbc()
        .update(
            "insert into t_flink_managed_run (id,app_id,release_id,provider_instance_id,"
                + "runtime_application_id,run_state,raw_state,origin,started_at,ended_at,observed_at) "
                + "values (?,?,?,?,?,?,?,?,?,?,?)",
            run,
            app,
            release,
            result.getInstanceId(),
            result.getRuntimeId(),
            runState,
            runState,
            restarted ? "CLOUD_OBSERVED" : "STREAMPARK",
            started,
            running ? null : now,
            now);
    store
        .jdbc()
        .update(
            "update t_flink_managed_job set current_run_id=?,observed_at=?,sync_status='FRESH' "
                + "where app_id=?",
            run,
            now,
            app);
    store
        .jdbc()
        .update(
            "update t_flink_app set state=?,start_time=?,end_time=? where id=?",
            ("STOPPED".equals(runState) ? FlinkAppState.CANCELED : FlinkAppState.of(runState))
                .getValue(),
            new Date(started),
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
    store
        .jdbc()
        .update(
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
    store
        .jdbc()
        .update(
            "update t_flink_managed_operation set status=?,lease_owner=null,lease_until=null,"
                + "updated_at=? where id=?",
            status,
            System.currentTimeMillis(),
            operation);
    if (!"VERIFY_REQUIRED".equals(status)) {
      store
          .jdbc()
          .update(
              "update t_flink_managed_job set pending_operation_id=null "
                  + "where app_id=? and pending_operation_id=?",
              op.get("app_id"),
              operation);
    }
    if (!"SUCCEEDED".equals(status)
        && ("SUBMIT".equals(text(op, "command_type"))
            || "PUBLISH".equals(text(op, "command_type"))
            || "ROLLBACK".equals(text(op, "command_type")))) {
      store
          .jdbc()
          .update(
              "update t_flink_managed_release set status=? where id=? and status<>'PUBLISHED'",
              status,
              op.get("release_id"));
    }
  }

  private void defer(Map<String, Object> op, long retryNotBefore) {
    long now = System.currentTimeMillis();
    // A due operation at its deadline is fenced without another cloud read.
    long next = Math.min(number(op, "deadline_at"), Math.max(now + 2_000, retryNotBefore));
    store
        .jdbc()
        .update(
            "update t_flink_managed_operation set status='WAITING_PROVIDER',"
                + "lease_owner=null,lease_until=null,next_run_at=?,updated_at=? where id=?",
            next,
            now,
            op.get("id"));
  }

  private boolean hasId(String value, int max) {
    return value != null && !value.trim().isEmpty() && value.length() <= max;
  }

  private String safe(String value, int max) {
    return hasId(value, max) && value.matches("[a-zA-Z0-9._-]+") ? value : null;
  }
}
