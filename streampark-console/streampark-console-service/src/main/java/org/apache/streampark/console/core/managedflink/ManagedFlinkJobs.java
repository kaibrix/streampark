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

import org.apache.streampark.common.enums.ApplicationType;
import org.apache.streampark.common.enums.DevelopmentMode;
import org.apache.streampark.common.enums.ExecutionMode;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Action;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Command;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CommandType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Context;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.FrozenCommand;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobInput;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Restore;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.RestoreMode;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Spec;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Target;
import org.apache.streampark.console.core.service.ApplicationService;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.bool;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.check;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.hash;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.id;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.number;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.required;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.text;

@Service
public class ManagedFlinkJobs {

  private final ManagedFlinkStore store;
  private final ManagedFlinkCatalog catalog;
  private final ApplicationService applications;

  ManagedFlinkJobs(
      ManagedFlinkStore store, ManagedFlinkCatalog catalog, ApplicationService applications) {
    this.store = store;
    this.catalog = catalog;
    this.applications = applications;
  }

  @Transactional
  public long create(long team, JobInput input) {
    long actor = catalog.authorize(team, "app:create");
    Map<String, Object> env = catalog.environment(team, input.getEnvironmentId(), true);
    check(bool(env, "enabled"), HttpStatus.UNPROCESSABLE_ENTITY, "Environment disabled");
    Map<String, Object> conn = catalog.connection(team, text(env, "connection_id"));
    validate(input.getSpec(), false);
    String name = required(input.getName(), 128);
    check(
        name.matches("[a-zA-Z0-9][a-zA-Z0-9_.-]*"),
        HttpStatus.BAD_REQUEST,
        "Job name must contain only letters, digits, dots, underscores, or hyphens");
    check(
        input.getSpec().getArtifactId() == null,
        HttpStatus.BAD_REQUEST,
        "Create the job before attaching an artifact");
    Application app = new Application();
    app.setJobName(name);
    app.setTeamId(team);
    app.setUserId(actor);
    app.setExecutionMode(ExecutionMode.MANAGED_FLINK.getMode());
    app.setJobType(
        input.getSpec().getType() == JobType.SQL
            ? DevelopmentMode.FLINK_SQL.getValue()
            : DevelopmentMode.CUSTOM_CODE.getValue());
    app.setAppType(ApplicationType.APACHE_FLINK.getType());
    app.setState(FlinkAppState.ADDED.getValue());
    app.setTracking(0);
    app.setOptionState(0);
    app.setBuild(false);
    app.setCreateTime(new Date());
    app.setModifyTime(app.getCreateTime());
    check(applications.save(app), HttpStatus.INTERNAL_SERVER_ERROR, "Unable to create job");
    store
        .jdbc()
        .update(
            "insert into t_flink_managed_job "
                + "(app_id,team_id,environment_id,engine_ref,draft_revision,draft_spec,scope_key) "
                + "values (?,?,?,?,1,?,?)",
            app.getId(),
            team,
            env.get("id"),
            catalog.target(env).getEngineRef(),
            store.encode(input.getSpec()),
            catalog.scope(conn, env));
    return app.getId();
  }

  @Transactional
  public void edit(long team, long app, JobInput input) {
    catalog.authorize(team, "app:update");
    Map<String, Object> job = job(team, app, true);
    check(
        Objects.equals(input.getExpectedDraftRevision(), number(job, "draft_revision")),
        HttpStatus.CONFLICT,
        "Draft revision changed");
    check(
        input.getEnvironmentId() == null
            || input.getEnvironmentId().equals(text(job, "environment_id")),
        HttpStatus.CONFLICT,
        "Environment is fixed for this job");
    Spec spec = input.getSpec();
    validate(spec, false);
    Spec old = store.decode(text(job, "draft_spec"), Spec.class);
    check(old.getType() == spec.getType(), HttpStatus.CONFLICT, "Job type is fixed");
    if (spec.getArtifactId() != null) {
      artifact(app, spec.getArtifactId());
    }
    store
        .jdbc()
        .update(
            "update t_flink_managed_job set draft_spec=?,draft_revision=draft_revision+1 "
                + "where app_id=?",
            store.encode(spec),
            app);
    store.jdbc().update("update t_flink_app set modify_time=? where id=?", new Date(), app);
  }

  public Map<String, Object> detail(long team, long app) {
    catalog.authorize(team, "app:detail");
    Map<String, Object> job = job(team, app, false);
    Map<String, Object> result = new java.util.LinkedHashMap<>();
    result.put("appId", Long.toString(app));
    result.put("environmentId", text(job, "environment_id"));
    result.put("engineRef", text(job, "engine_ref"));
    result.put("draftRevision", number(job, "draft_revision"));
    result.put("spec", store.decode(text(job, "draft_spec"), Spec.class));
    result.put("activeReleaseId", text(job, "active_release_id"));
    result.put("currentRunId", text(job, "current_run_id"));
    result.put("pendingOperationId", text(job, "pending_operation_id"));
    result.put("syncStatus", text(job, "sync_status"));
    return result;
  }

  public void validateDraft(long team, long app, Spec candidate) {
    catalog.authorize(team, "app:update");
    Map<String, Object> job = job(team, app, false);
    Spec spec = candidate == null ? store.decode(text(job, "draft_spec"), Spec.class) : candidate;
    validate(spec, true);
    if (spec.getArtifactId() != null) {
      artifact(app, spec.getArtifactId());
    }
  }

  @Transactional(isolation = Isolation.READ_COMMITTED)
  public String enqueue(long team, long app, CommandType type, String key, Command cmd) {
    long actor = catalog.authorize(team, permission(type));
    key = required(key, 128);
    Map<String, Object> job = job(team, app, true);
    String requestHash = hash(type.name() + ":" + store.encode(cmd));
    Map<String, Object> existing =
        store.find(
            "select id, request_hash from t_flink_managed_operation where team_id=? and app_id=? and idempotency_key=?",
            team,
            app,
            key);
    if (existing != null) {
      check(
          requestHash.equals(text(existing, "request_hash")),
          HttpStatus.CONFLICT,
          "Idempotency key already has different parameters");
      return text(existing, "id");
    }
    check(text(job, "pending_operation_id") == null, HttpStatus.CONFLICT, "A command is pending");
    check(
        (text(job, "provider_job_id") == null || "FRESH".equals(text(job, "sync_status"))),
        HttpStatus.CONFLICT,
        "Cloud mapping requires verification");
    Map<String, Object> env = catalog.environment(team, text(job, "environment_id"), true);
    Map<String, Object> conn = catalog.connection(team, text(env, "connection_id"));
    ManagedFlinkProvider provider = catalog.provider(text(conn, "provider"));
    Target target = catalog.target(env);
    Context context = new Context(text(env, "region"), text(conn, "account_ref"), target, null);
    Map<String, String> capabilities = provider.capabilities(context);
    FrozenCommand frozen = new FrozenCommand();
    frozen.setRequest(cmd);
    frozen.setTarget(target);
    frozen.setEngineRef(text(job, "engine_ref"));
    frozen.setJobName(
        text(store.require("select job_name from t_flink_app where id=?", app), "job_name"));

    String release = text(job, "active_release_id");
    Map<String, Object> current = currentRun(job);
    if (type == CommandType.PUBLISH || type == CommandType.ROLLBACK) {
      release = freezeRelease(app, type, cmd, job, env, provider, capabilities, frozen, current);
    } else if (type == CommandType.START) {
      prepareStart(app, job, env, capabilities, cmd, frozen, release, current);
    } else {
      prepareRunAction(team, type, cmd, capabilities, frozen, current);
    }
    String operation = id();
    long now = System.currentTimeMillis();
    store
        .jdbc()
        .update(
            "insert into t_flink_managed_operation (id,app_id,team_id,idempotency_key,"
                + "request_hash,command_type,command,release_id,status,deadline_at,next_run_at,created_by,"
                + "created_at,updated_at) values (?,?,?,?,?,?,?,?,'PENDING',?,?,?,?,?)",
            operation,
            app,
            team,
            key,
            requestHash,
            type.name(),
            store.encode(frozen),
            release,
            now + 5 * 60_000,
            now,
            actor,
            now,
            now);
    store
        .jdbc()
        .update(
            "update t_flink_managed_job set pending_operation_id=? where app_id=?", operation, app);
    return operation;
  }

  private String freezeRelease(
      long app,
      CommandType type,
      Command cmd,
      Map<String, Object> job,
      Map<String, Object> env,
      ManagedFlinkProvider provider,
      Map<String, String> capabilities,
      FrozenCommand frozen,
      Map<String, Object> current) {
    check(bool(env, "enabled"), HttpStatus.UNPROCESSABLE_ENTITY, "Environment disabled");
    check(!isActive(current), HttpStatus.CONFLICT, "Stop the active instance before publishing");
    check(
        Objects.equals(cmd.getExpectedReleaseId(), text(job, "active_release_id")),
        HttpStatus.CONFLICT,
        "Published version changed");
    check(
        Objects.equals(cmd.getExpectedEnvVersion(), (int) number(env, "config_version")),
        HttpStatus.CONFLICT,
        "Environment configuration changed");
    long revision = number(job, "draft_revision");
    Spec spec;
    if (type == CommandType.ROLLBACK) {
      Map<String, Object> old = release(app, cmd.getSourceReleaseId());
      check(
          "PUBLISHED".equals(text(old, "status")),
          HttpStatus.CONFLICT,
          "Only a verified release can be rolled back");
      spec = store.decode(text(old, "spec"), Spec.class);
      revision = number(old, "draft_revision");
    } else {
      check(
          Objects.equals(cmd.getDraftRevision(), revision),
          HttpStatus.CONFLICT,
          "Draft revision changed");
      spec = store.decode(text(job, "draft_spec"), Spec.class);
    }
    validate(spec, true);
    capability(capabilities, "PUBLISH_" + spec.getType().name());
    frozen.setSpec(spec);
    String encoded = store.encode(spec);
    if (spec.getType() == JobType.JAR) {
      Map<String, Object> artifact = artifact(app, spec.getArtifactId());
      frozen.setArtifactRef(text(artifact, "object_ref"));
      frozen.setArtifactDigest(text(artifact, "digest"));
      frozen.setArtifactSize(number(artifact, "size_bytes"));
      if (spec.getMainClass() == null || spec.getMainClass().trim().isEmpty()) {
        spec.setMainClass(text(artifact, "main_class"));
      }
      required(spec.getMainClass(), 255);
      encoded = store.encode(spec);
    }
    frozen.setSpecHash(hash(encoded));
    frozen.setActions(
        provider.publishSteps(spec.getType(), text(job, "provider_draft_id") != null));
    String release = id();
    Long next =
        store
            .jdbc()
            .queryForObject(
                "select coalesce(max(release_no),0)+1 from t_flink_managed_release where app_id=?",
                Long.class,
                app);
    store
        .jdbc()
        .update(
            "insert into t_flink_managed_release "
                + "(id,app_id,release_no,draft_revision,config_version,spec,spec_hash,target,status,created_at) "
                + "values (?,?,?,?,?,?,?,?,'PENDING',?)",
            release,
            app,
            next,
            revision,
            number(env, "config_version"),
            encoded,
            frozen.getSpecHash(),
            store.encode(frozen.getTarget()),
            System.currentTimeMillis());
    return release;
  }

  private void prepareStart(
      long app,
      Map<String, Object> job,
      Map<String, Object> env,
      Map<String, String> capabilities,
      Command cmd,
      FrozenCommand frozen,
      String release,
      Map<String, Object> current) {
    check(bool(env, "enabled"), HttpStatus.UNPROCESSABLE_ENTITY, "Environment disabled");
    check(
        release != null
            && Objects.equals(release, cmd.getExpectedReleaseId())
            && !isActive(current),
        HttpStatus.CONFLICT,
        "Expected published release with no active instance");
    Map<String, Object> version = release(app, release);
    check(
        "PUBLISHED".equals(text(version, "status")), HttpStatus.CONFLICT, "Release is unverified");
    Restore restore = cmd.getRestore();
    check(
        restore != null && restore.getMode() != null,
        HttpStatus.BAD_REQUEST,
        "Choose a restore mode");
    capability(capabilities, "START");
    if (restore.getMode() == RestoreMode.FROM_NEW) {
      check(
          current == null || restore.isConfirmedNew(),
          HttpStatus.UNPROCESSABLE_ENTITY,
          "Confirm a fresh start for a previously started job");
    } else if (restore.getMode() == RestoreMode.FROM_SNAPSHOT) {
      capability(capabilities, "RESTORE_FROM_SNAPSHOT");
      Map<String, Object> snapshot =
          store.require(
              "select * from t_flink_managed_snapshot where app_id=? and id=?",
              app,
              restore.getSnapshotId());
      check(
          bool(snapshot, "restorable")
              && "AVAILABLE".equals(text(snapshot, "state"))
              && Objects.equals(release, text(snapshot, "release_id")),
          HttpStatus.UNPROCESSABLE_ENTITY,
          "Snapshot is unavailable or incompatible");
      frozen.setRestoreRef(required(text(snapshot, "restore_ref"), 191));
    } else {
      capability(capabilities, "RESTORE_FROM_LATEST");
      Map<String, Object> latest =
          store.find(
              "select restore_ref from t_flink_managed_snapshot "
                  + "where app_id=? and release_id=? and restorable=true and state='AVAILABLE' "
                  + "order by created_at desc, id desc limit 1",
              app,
              release);
      check(latest != null, HttpStatus.UNPROCESSABLE_ENTITY, "No compatible latest snapshot");
      frozen.setRestoreRef(required(text(latest, "restore_ref"), 191));
    }
    frozen.setTarget(store.decode(text(version, "target"), Target.class));
    frozen.setSpec(store.decode(text(version, "spec"), Spec.class));
    frozen.setArtifactRef(text(version, "artifact_ref"));
    frozen.setSpecHash(text(version, "spec_hash"));
    frozen.setProviderVersion(text(version, "provider_version"));
    frozen.setActions(Collections.singletonList(Action.START));
  }

  private void prepareRunAction(
      long team,
      CommandType type,
      Command cmd,
      Map<String, String> capabilities,
      FrozenCommand frozen,
      Map<String, Object> current) {
    check(
        isActive(current) && Objects.equals(cmd.getExpectedRunId(), text(current, "id")),
        HttpStatus.CONFLICT,
        "Expected a current running instance");
    frozen.setProviderInstance(text(current, "provider_instance_id"));
    if (type == CommandType.SNAPSHOT) {
      capability(capabilities, "SNAPSHOT");
      frozen.setActions(Collections.singletonList(Action.SNAPSHOT));
    } else if ("AFTER_SNAPSHOT".equals(cmd.getMode())) {
      catalog.authorize(team, "savepoint:trigger");
      capability(capabilities, "SNAPSHOT_THEN_STOP");
      capability(capabilities, "SNAPSHOT");
      capability(capabilities, "STOP");
      frozen.setActions(Arrays.asList(Action.SNAPSHOT, Action.STOP));
    } else {
      check(
          cmd.getMode() == null || "IMMEDIATE".equals(cmd.getMode()),
          HttpStatus.BAD_REQUEST,
          "Unknown stop mode");
      capability(capabilities, "STOP");
      frozen.setActions(Collections.singletonList(Action.STOP));
    }
  }

  @Transactional
  public void archive(long team, long app) {
    catalog.authorize(team, "app:delete");
    Map<String, Object> job = job(team, app, true);
    check(
        text(job, "pending_operation_id") == null
            && !isActive(currentRun(job))
            && !"DRIFTED".equals(text(job, "sync_status")),
        HttpStatus.CONFLICT,
        "Job must be stopped and reconciled");
    store.jdbc().update("update t_flink_managed_job set archived=true where app_id=?", app);
  }

  Map<String, Object> job(long team, long app, boolean lock) {
    return store.require(
        "select * from t_flink_managed_job where app_id=? and team_id=? "
            + "and archived=false"
            + (lock ? " for update" : ""),
        app,
        team);
  }

  Map<String, Object> release(long app, String release) {
    return store.require(
        "select * from t_flink_managed_release where app_id=? and id=?", app, release);
  }

  Map<String, Object> artifact(long app, String artifact) {
    return store.require(
        "select * from t_flink_managed_artifact where app_id=? and id=?", app, artifact);
  }

  Map<String, Object> currentRun(Map<String, Object> job) {
    return store.find(
        "select * from t_flink_managed_run where app_id=? and id=?",
        job.get("app_id"),
        job.get("current_run_id"));
  }

  private void capability(Map<String, String> capabilities, String feature) {
    check(
        "AVAILABLE".equals(capabilities.get(feature)),
        HttpStatus.UNPROCESSABLE_ENTITY,
        feature + " is " + capabilities.getOrDefault(feature, "UNVERIFIED"));
  }

  private boolean isActive(Map<String, Object> run) {
    return run != null
        && !Arrays.asList("STOPPED", "FAILED", "FINISHED").contains(text(run, "run_state"));
  }

  private String permission(CommandType type) {
    switch (type) {
      case START:
        return "app:start";
      case STOP:
        return "app:cancel";
      case SNAPSHOT:
        return "savepoint:trigger";
      default:
        return "app:release";
    }
  }

  private void validate(Spec spec, boolean complete) {
    check(
        spec != null && spec.getType() != null,
        HttpStatus.BAD_REQUEST,
        "Expected a SQL or JAR definition");
    if (spec.getType() == JobType.SQL) {
      if (complete) {
        required(spec.getSql(), 1_000_000);
      }
      check(spec.getArtifactId() == null, HttpStatus.BAD_REQUEST, "SQL cannot have a main JAR");
    } else {
      check(spec.getSql() == null, HttpStatus.BAD_REQUEST, "JAR cannot have SQL text");
      if (complete) {
        required(spec.getArtifactId(), 36);
      }
    }
    check(
        spec.getArgs() != null
            && spec.getArgs().size() <= 128
            && spec.getArgs().stream().allMatch(arg -> arg != null && arg.length() <= 4096),
        HttpStatus.BAD_REQUEST,
        "Invalid program arguments");
    List<String> allowed =
        Arrays.asList(
            "parallelism.default",
            "execution.checkpointing.interval",
            "execution.checkpointing.timeout",
            "taskmanager.numberOfTaskSlots",
            "jobmanager.memory.process.size",
            "taskmanager.memory.process.size");
    check(
        spec.getConfiguration() != null && allowed.containsAll(spec.getConfiguration().keySet()),
        HttpStatus.BAD_REQUEST,
        "Unsupported Flink configuration");
  }
}
