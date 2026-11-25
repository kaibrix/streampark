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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CloudState;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Command;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CommandType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.DependencyArtifact;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.FrozenCommand;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobInput;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.LogQuery;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.ReadStatus;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Restore;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.RestoreMode;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Spec;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Target;
import org.apache.streampark.console.core.service.ApplicationService;

import org.apache.commons.lang3.StringUtils;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
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
  private final TransactionTemplate transactions;

  ManagedFlinkJobs(
      ManagedFlinkStore store,
      ManagedFlinkCatalog catalog,
      ApplicationService applications,
      PlatformTransactionManager manager) {
    this.store = store;
    this.catalog = catalog;
    this.applications = applications;
    this.transactions = new TransactionTemplate(manager);
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
        specArtifactId(input.getSpec()) == null && input.getSpec().getDependencyIds().isEmpty(),
        HttpStatus.BAD_REQUEST,
        "Create the job before attaching an artifact");
    String engine = catalog.target(env).getEngineRef();
    if ("volcengine".equals(text(conn, "provider"))) {
      String issue = VolcengineManagedFlinkProvider.jobTypeIssue(input.getSpec(), engine);
      check(issue == null, HttpStatus.UNPROCESSABLE_ENTITY, issue);
      check(
          name.matches("[a-zA-Z0-9](?:[a-zA-Z0-9_.-]{0,46}[a-zA-Z0-9])?"),
          HttpStatus.BAD_REQUEST,
          "Volcengine job names must be 1-48 characters and end in a letter or digit");
    }
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
            engine,
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
    requireSameJobType(store.decode(text(job, "draft_spec"), Spec.class), spec);
    spec.getDependencyIds().forEach(id -> artifact(app, id));
    String artifactId = specArtifactId(spec);
    if (artifactId != null) {
      artifact(app, artifactId);
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
    result.put("cloudBound", text(job, "provider_job_id") != null);
    result.put(
        "directSubmission",
        text(job, "provider_job_id") != null && text(job, "provider_draft_id") == null);
    result.put("observedAt", job.get("observed_at"));
    Map<String, Object> env = catalog.environment(team, text(job, "environment_id"), false);
    Map<String, Object> conn = catalog.connection(team, text(env, "connection_id"));
    result.put("environmentName", text(env, "name"));
    result.put("provider", text(conn, "provider"));
    result.put("configVersion", number(env, "config_version"));
    Target target = catalog.target(env);
    Map<String, String> capabilities = catalog.capabilities(env, conn, target);
    result.put("capabilities", capabilities);
    Target runtime =
        job.get("active_release_id") == null
            ? target
            : releaseTarget(app, text(job, "active_release_id"));
    result.put(
        "runtimeCapabilities",
        runtime.equals(target) ? capabilities : catalog.capabilities(env, conn, runtime));
    return result;
  }

  public void validateDraft(long team, long app, Spec candidate) {
    catalog.authorize(team, "app:update");
    Map<String, Object> job = job(team, app, false);
    Spec saved = store.decode(text(job, "draft_spec"), Spec.class);
    Spec spec = candidate == null ? saved : candidate;
    validate(spec, true);
    requireSameJobType(saved, spec);
    Map<String, Object> environment = catalog.environment(team, text(job, "environment_id"), false);
    Map<String, Object> connection = catalog.connection(team, text(environment, "connection_id"));
    validateProviderSpec(
        catalog.provider(text(connection, "provider")), spec, text(job, "engine_ref"));
    spec.getDependencyIds().forEach(id -> artifact(app, id));
    publicationArtifact(app, spec);
  }

  String validateCloudSql(long team, long app, String sql) {
    catalog.authorize(team, "app:update");
    Map<String, Object> job = job(team, app, false);
    Spec spec = store.decode(text(job, "draft_spec"), Spec.class);
    check(spec.getType() == JobType.SQL, HttpStatus.BAD_REQUEST, "SQL draft required");
    required(sql, 1_000_000);
    spec.setSql(sql);
    String draft = text(job, "provider_draft_id");
    if (draft == null) {
      return "NOT_BOUND";
    }
    Map<String, Object> env = catalog.environment(team, text(job, "environment_id"), false);
    Map<String, Object> conn = catalog.connection(team, text(env, "connection_id"));
    return catalog
        .provider(text(conn, "provider"))
        .validateCloudSql(catalog.context(conn, env, null), draft, spec);
  }

  Map<String, Object> cloudInstance(long team, long app, String instanceId) {
    Map<String, Object> job = job(team, app, false);
    Map<String, Object> env = catalog.environment(team, text(job, "environment_id"), false);
    Map<String, Object> connection = catalog.connection(team, text(env, "connection_id"));
    return catalog
        .provider(text(connection, "provider"))
        .cloudInstance(
            catalog.jobContext(connection, env, null, job),
            text(job, "provider_job_id"),
            required(instanceId, 191));
  }

  Map<String, Object> cloudJob(long team, long app, int page) {
    check(page > 0, HttpStatus.BAD_REQUEST, "Invalid instance page");
    Map<String, Object> job = job(team, app, false);
    Map<String, Object> env = catalog.environment(team, text(job, "environment_id"), false);
    Map<String, Object> connection = catalog.connection(team, text(env, "connection_id"));
    return catalog
        .provider(text(connection, "provider"))
        .cloudJob(
            catalog.jobContext(connection, env, null, job), text(job, "provider_job_id"), page);
  }

  Map<String, Object> diagnostics(
      long team, long app, String runId, String source, Long from, Long until) {
    Map<String, Object> job = job(team, app, false);
    Map<String, Object> run =
        store.require(
            "select provider_instance_id from t_flink_managed_run where app_id=? and id=?",
            app,
            runId);
    Map<String, Object> env = catalog.environment(team, text(job, "environment_id"), false);
    Map<String, Object> connection = catalog.connection(team, text(env, "connection_id"));
    return catalog
        .provider(text(connection, "provider"))
        .diagnostics(
            catalog.jobContext(connection, env, null, job),
            text(job, "provider_job_id"),
            text(run, "provider_instance_id"),
            source,
            from,
            until);
  }

  Map<String, Object> logs(
      long team,
      long app,
      String runId,
      Long from,
      Long until,
      String component,
      String level,
      String keyword,
      String cursor) {
    Map<String, Object> job = job(team, app, false);
    Map<String, Object> run =
        store.require(
            "select provider_instance_id from t_flink_managed_run where app_id=? and id=?",
            app,
            runId);
    long actor = catalog.authorize(team, "app:log");
    Map<String, Object> env = catalog.environment(team, text(job, "environment_id"), false);
    Map<String, Object> connection = catalog.connection(team, text(env, "connection_id"), false);
    return catalog
        .provider(text(connection, "provider"))
        .logs(
            catalog.jobContext(connection, env, null, job),
            text(job, "provider_job_id"),
            text(run, "provider_instance_id"),
            new LogQuery(actor, from, until, component, level, keyword, cursor));
  }

  @Transactional(isolation = Isolation.READ_COMMITTED)
  public String enqueue(long team, long app, CommandType type, String key, Command cmd) {
    long actor = catalog.authorize(team, permission(type));
    if (type == CommandType.SUBMIT) {
      catalog.authorize(team, "app:start");
      catalog.authorize(team, "app:create");
    }
    if (type == CommandType.RESTART) {
      catalog.authorize(team, "app:cancel");
    }
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
    String release = text(job, "active_release_id");
    boolean publishing =
        type == CommandType.SUBMIT || type == CommandType.PUBLISH || type == CommandType.ROLLBACK;
    Target target =
        publishing || release == null ? catalog.target(env) : releaseTarget(app, release);
    Map<String, String> capabilities = catalog.baseCapabilities(env, conn, target);
    FrozenCommand frozen = new FrozenCommand();
    frozen.setRequest(cmd);
    frozen.setTarget(target);
    frozen.setEngineRef(text(job, "engine_ref"));
    frozen.setJobName(
        text(store.require("select job_name from t_flink_app where id=?", app), "job_name"));

    Map<String, Object> current = currentRun(job);
    if (publishing) {
      if (type == CommandType.SUBMIT) {
        capability(capabilities, "SUBMIT_INSTANCE");
        check(
            text(job, "provider_draft_id") == null,
            HttpStatus.CONFLICT,
            "A published cloud draft already exists");
        if (release != null) {
          check(
              cmd.getRestore() != null && cmd.getRestore().isConfirmedNew(),
              HttpStatus.UNPROCESSABLE_ENTITY,
              "Confirm a fresh start when submitting a new version");
        }
        Restore restore = new Restore();
        restore.setMode(RestoreMode.FROM_NEW);
        Command submitRequest = store.decode(store.encode(cmd), Command.class);
        submitRequest.setRestore(restore);
        frozen.setRequest(submitRequest);
      } else {
        check(
            text(job, "provider_job_id") == null || text(job, "provider_draft_id") != null,
            HttpStatus.CONFLICT,
            "Use direct submission for this job");
      }
      release = freezeRelease(app, type, cmd, job, env, provider, capabilities, frozen, current);
    } else if (type == CommandType.START || type == CommandType.RESTART) {
      boolean restart = type == CommandType.RESTART;
      if (restart) {
        capability(capabilities, "STOP");
        check(
            isActive(current) && Objects.equals(cmd.getExpectedRunId(), text(current, "id")),
            HttpStatus.CONFLICT,
            "Expected the current instance to restart");
        frozen.setProviderInstance(text(current, "provider_instance_id"));
      }
      prepareStart(app, restart, env, capabilities, cmd, frozen, release, current);
      if (restart
          && !cmd.isSubmitInstance()
          && text(job, "provider_draft_id") != null
          && "AVAILABLE".equals(capabilities.get("CLOUD_RESTART"))) {
        frozen.setActions(Collections.singletonList(Action.RESTART));
      }
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
            now + (frozen.getActions().contains(Action.SNAPSHOT) ? 15 : 5) * 60_000L,
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
    String priorRelease = text(job, "active_release_id");
    if (priorRelease != null) {
      frozen.setProviderVersion(text(release(app, priorRelease), "provider_version"));
    }
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
      if ("volcengine".equals(provider.id()) && spec.getPriority() == null) {
        // Freeze the product default on new releases. Older releases keep their original priority.
        spec.setPriority(3);
      }
    }
    validate(spec, true);
    validateProviderSpec(provider, spec, frozen.getEngineRef());
    capability(capabilities, "PUBLISH_" + spec.getType().name());
    if (spec.getType() == JobType.SQL
        && (spec.getUdfArtifactId() != null || !spec.getDependencyIds().isEmpty())) {
      capability(capabilities, "PUBLISH_SQL_UDF");
    }
    Map<String, Object> artifact = publicationArtifact(app, spec);
    if (artifact != null) {
      frozen.setArtifactRef(text(artifact, "object_ref"));
      frozen.setArtifactDigest(text(artifact, "digest"));
      frozen.setArtifactSize(number(artifact, "size_bytes"));
    }
    frozen.setSpec(spec);
    String encoded = store.encode(spec);
    frozen.setSpecHash(hash(encoded));
    List<Action> actions =
        new ArrayList<>(
            provider.publishSteps(spec.getType(), text(job, "provider_draft_id") != null));
    if (spec.getUdfArtifactId() != null) {
      actions.remove(Action.STAGE_ARTIFACT);
      actions.add(0, Action.STAGE_ARTIFACT);
    }
    int index = 0;
    for (String dependencyId : spec.getDependencyIds()) {
      Map<String, Object> row = artifact(app, dependencyId);
      DependencyArtifact dep = new DependencyArtifact();
      dep.setId(dependencyId);
      dep.setDigest(text(row, "digest"));
      dep.setSize(number(row, "size_bytes"));
      dep.setLocalRef(text(row, "object_ref"));
      frozen.getDependencies().add(dep);
      actions.add(index++, Action.STAGE_DEPENDENCY);
    }
    if (type == CommandType.SUBMIT) {
      actions.removeIf(
          action -> action != Action.STAGE_ARTIFACT && action != Action.STAGE_DEPENDENCY);
      actions.add(Action.SUBMIT);
    }
    frozen.setActions(actions);
    String release = id();
    frozen.setReleaseMarker(release);
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
      boolean restart,
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
            && (restart || !isActive(current)),
        HttpStatus.CONFLICT,
        "Expected published release and matching instance state");
    Map<String, Object> version = release(app, release);
    check(
        "PUBLISHED".equals(text(version, "status")), HttpStatus.CONFLICT, "Release is unverified");
    Restore restore = cmd.getRestore();
    check(
        restore != null && restore.getMode() != null,
        HttpStatus.BAD_REQUEST,
        "Choose a restore mode");
    capability(capabilities, "START");
    if (cmd.isSubmitInstance()) {
      capability(capabilities, "SUBMIT_INSTANCE");
    }
    if (restore.getMode() == RestoreMode.FROM_NEW) {
      check(
          current == null || restore.isConfirmedNew(),
          HttpStatus.UNPROCESSABLE_ENTITY,
          "Confirm a fresh start for a previously started job");
    } else if (restore.getMode() == RestoreMode.FROM_CLOUD_LATEST) {
      capability(capabilities, "RESTORE_FROM_CLOUD_LATEST");
    } else if (restore.getMode() == RestoreMode.FROM_CLOUD_SNAPSHOT) {
      capability(capabilities, "RESTORE_FROM_CLOUD_SNAPSHOT");
      // The provider verifies membership and availability against this exact job before dispatch.
      frozen.setRestoreRef(required(restore.getSnapshotId(), 191));
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
    frozen.setSpec(store.decode(text(version, "spec"), Spec.class));
    frozen.setReleaseMarker(release);
    frozen.setArtifactRef(text(version, "artifact_ref"));
    String artifactId = specArtifactId(frozen.getSpec());
    if (artifactId != null) {
      Map<String, Object> releasedArtifact = artifact(app, artifactId);
      frozen.setArtifactDigest(text(releasedArtifact, "digest"));
      frozen.setArtifactSize(number(releasedArtifact, "size_bytes"));
    }
    if (!frozen.getSpec().getDependencyIds().isEmpty()) {
      // Each publication creates its own release ID. Its frozen command records each confirmed
      // staging result, including when the first run of a published release failed.
      Map<String, Object> publication =
          store.require(
              "select command from t_flink_managed_operation where app_id=? and release_id=? "
                  + "and (command_type in ('PUBLISH','ROLLBACK') and status='SUCCEEDED'"
                  + " or command_type='SUBMIT' and status in ('SUCCEEDED','FAILED'))",
              app,
              release);
      frozen.setDependencies(
          store.decode(text(publication, "command"), FrozenCommand.class).getDependencies());
      check(
          frozen.getDependencies().size() == frozen.getSpec().getDependencyIds().size()
              && frozen.getDependencies().stream().allMatch(dep -> dep.getRemoteRef() != null),
          HttpStatus.CONFLICT,
          "Release dependencies are incomplete");
    }
    frozen.setSpecHash(text(version, "spec_hash"));
    frozen.setProviderVersion(text(version, "provider_version"));
    frozen.setActions(
        restart
            ? Arrays.asList(Action.STOP, Action.START)
            : Collections.singletonList(Action.START));
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

  @Transactional(propagation = Propagation.NOT_SUPPORTED)
  public void archive(long team, long app) {
    catalog.authorize(team, "app:delete");
    Map<String, Object> snapshot = job(team, app, false);
    requireArchivable(snapshot);
    String providerJob = text(snapshot, "provider_job_id");
    String environmentId = text(snapshot, "environment_id");
    Map<String, Object> env =
        providerJob == null ? null : catalog.environment(team, environmentId, false);
    Map<String, Object> conn =
        env == null ? null : catalog.connection(team, text(env, "connection_id"));
    if (providerJob != null) {
      requireCloudStopped(app, snapshot, env, conn);
    }

    transactions.executeWithoutResult(
        status -> {
          Map<String, Object> current = job(team, app, true);
          requireArchivable(current);
          check(
              Arrays.asList(
                      "provider_job_id",
                      "active_release_id",
                      "current_run_id",
                      "observed_at",
                      "environment_id")
                  .stream()
                  .allMatch(field -> Objects.equals(text(snapshot, field), text(current, field))),
              HttpStatus.CONFLICT,
              "Managed Flink job changed during archive");
          if (env != null) {
            Map<String, Object> lockedEnv = catalog.environment(team, environmentId, true);
            String connectionId = text(lockedEnv, "connection_id");
            Map<String, Object> lockedConn = catalog.connection(team, connectionId, true);
            check(
                Objects.equals(text(env, "connection_id"), connectionId)
                    && number(env, "config_version") == number(lockedEnv, "config_version")
                    && number(conn, "credential_version")
                        == number(lockedConn, "credential_version"),
                HttpStatus.CONFLICT,
                "Managed Flink environment changed during archive");
          }
          store
              .jdbc()
              .update(
                  "update t_flink_managed_job set archived=true where team_id=? and app_id=?",
                  team,
                  app);
        });
  }

  private void requireArchivable(Map<String, Object> job) {
    check(
        text(job, "pending_operation_id") == null
            && !isActive(currentRun(job))
            && !"DRIFTED".equals(text(job, "sync_status")),
        HttpStatus.CONFLICT,
        "Job must be stopped and reconciled");
  }

  private void requireCloudStopped(
      long app, Map<String, Object> job, Map<String, Object> env, Map<String, Object> conn) {
    String releaseId = text(job, "active_release_id");
    check(releaseId != null, HttpStatus.CONFLICT, "Cloud version could not be confirmed");
    Map<String, Object> release = release(app, releaseId);
    String version = text(release, "provider_version");
    check(
        version != null && "PUBLISHED".equals(text(release, "status")),
        HttpStatus.CONFLICT,
        "Cloud version could not be confirmed");
    String providerJob = text(job, "provider_job_id");
    CloudState state;
    try {
      Target target = store.decode(text(release, "target"), Target.class);
      state =
          catalog
              .provider(text(conn, "provider"))
              .inspect(catalog.jobContext(conn, env, target, job), providerJob);
    } catch (RuntimeException error) {
      throw new ResponseStatusException(
          HttpStatus.CONFLICT, "Cloud job stop could not be confirmed");
    }
    check(
        state != null
            && state.getStatus() == ReadStatus.FOUND
            && providerJob.equals(state.getJobId())
            && version.equals(state.getVersion())
            && (state.getInstanceId() == null
                ? state.getRunState() == null
                : Arrays.asList("STOPPED", "FAILED", "FINISHED").contains(state.getRunState())),
        HttpStatus.CONFLICT,
        "Cloud job stop could not be confirmed");
  }

  Map<String, Object> job(long team, long app, boolean lock) {
    return store.require(
        "select * from t_flink_managed_job where app_id=? and team_id=? "
            + "and archived=false"
            + (lock ? " for update" : ""),
        app,
        team);
  }

  Target releaseTarget(long app, String release) {
    return store.decode(
        text(
            store.require(
                "select target from t_flink_managed_release where app_id=? and id=?", app, release),
            "target"),
        Target.class);
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
      case RESTART:
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

  private void requireSameJobType(Spec old, Spec spec) {
    check(
        old.getType() == spec.getType() && old.isBatchMode() == spec.isBatchMode(),
        HttpStatus.CONFLICT,
        "Job type is fixed");
  }

  private Map<String, Object> publicationArtifact(long app, Spec spec) {
    String artifactId = specArtifactId(spec);
    if (artifactId == null) {
      return null;
    }
    Map<String, Object> artifact = artifact(app, artifactId);
    if (spec.getType() == JobType.JAR) {
      String mainClass =
          StringUtils.defaultIfBlank(spec.getMainClass(), text(artifact, "main_class"));
      check(
          StringUtils.isNotBlank(mainClass) && mainClass.length() <= 255,
          HttpStatus.BAD_REQUEST,
          "Set a JAR main class or provide a valid Main-Class manifest entry");
      spec.setMainClass(mainClass);
    }
    return artifact;
  }

  private String specArtifactId(Spec spec) {
    return spec.getType() == JobType.SQL ? spec.getUdfArtifactId() : spec.getArtifactId();
  }

  private void validate(Spec spec, boolean complete) {
    check(
        spec != null && spec.getType() != null,
        HttpStatus.BAD_REQUEST,
        "Expected a SQL or JAR definition");
    check(
        spec.getPriority() == null || spec.getPriority() >= 0,
        HttpStatus.BAD_REQUEST,
        "Invalid scheduling priority");
    String policy = VolcengineExecutable.schedulePolicy(spec);
    check(
        "DRF".equals(policy) || ("GANG".equals(policy) && !spec.isBatchMode()),
        HttpStatus.BAD_REQUEST,
        "Invalid scheduling policy for the processing mode");
    check(
        spec.getScheduleTimeout() == null
            || ("GANG".equals(policy) && spec.getScheduleTimeout() >= 0),
        HttpStatus.BAD_REQUEST,
        "Only GANG scheduling accepts a non-negative timeout");
    if (spec.getType() == JobType.SQL) {
      if (complete) {
        required(spec.getSql(), 1_000_000);
      }
      check(spec.getArtifactId() == null, HttpStatus.BAD_REQUEST, "SQL cannot have a main JAR");
      if (spec.getUdfArtifactId() != null) {
        required(spec.getUdfArtifactId(), 36);
      }
    } else {
      check(spec.getSql() == null, HttpStatus.BAD_REQUEST, "JAR cannot have SQL text");
      check(
          spec.getUdfArtifactId() == null, HttpStatus.BAD_REQUEST, "UDF dependencies require SQL");
      if (complete) {
        required(spec.getArtifactId(), 36);
      }
    }
    List<String> dependencies = spec.getDependencyIds();
    check(
        dependencies != null
            && dependencies.size() <= 32
            && dependencies.stream()
                .allMatch(
                    id ->
                        StringUtils.isNotBlank(id)
                            && id.length() <= 36
                            && !id.equals(specArtifactId(spec)))
            && dependencies.stream().distinct().count() == dependencies.size(),
        HttpStatus.BAD_REQUEST,
        "Invalid or duplicate dependency JAR");
    check(
        spec.getArgs() != null
            && spec.getArgs().size() <= 128
            && spec.getArgs().stream()
                .allMatch(
                    arg ->
                        arg != null
                            && arg.length() <= 4096
                            && arg.codePoints().noneMatch(Character::isISOControl)),
        HttpStatus.BAD_REQUEST,
        "Invalid program arguments");
    List<String> allowed =
        Arrays.asList(
            "parallelism.default",
            "execution.checkpointing.interval",
            "execution.checkpointing.timeout",
            "taskmanager.numberOfTaskSlots",
            "kubernetes.jobmanager.cpu",
            "kubernetes.taskmanager.cpu",
            "jobmanager.memory.process.size",
            "taskmanager.memory.process.size");
    check(
        spec.getConfiguration() != null
            && spec.getConfiguration().size() <= 32
            && spec.getConfiguration().entrySet().stream()
                .allMatch(
                    entry ->
                        entry.getKey() != null
                            && (allowed.contains(entry.getKey())
                                || VolcengineExecutable.RESTART_SETTINGS.contains(entry.getKey())
                                || ("pipeline.global-job-parameters".equals(entry.getKey())
                                    && spec.getType() == JobType.SQL)
                                || ("args.base64.enabled".equals(entry.getKey())
                                    && spec.getType() == JobType.JAR)
                                || entry
                                    .getKey()
                                    .matches(
                                        "containerized\\.(master|taskmanager)\\.env\\.[A-Za-z_][A-Za-z0-9_]{0,63}"))
                            && entry.getValue() != null
                            && entry.getValue().length() <= 512
                            && entry.getValue().codePoints().noneMatch(Character::isISOControl)
                            && validRuntimeSetting(entry.getKey(), entry.getValue())),
        HttpStatus.BAD_REQUEST,
        "Unsupported Flink configuration");
  }

  static boolean validRuntimeSetting(String key, String value) {
    switch (key) {
      case "parallelism.default":
      case "taskmanager.numberOfTaskSlots":
        return value.matches("[1-9][0-9]{0,8}");
      case "kubernetes.jobmanager.cpu":
      case "kubernetes.taskmanager.cpu":
        return value.matches("(?:[1-9][0-9]{0,3}(?:\\.[0-9]{1,3})?|0\\.[0-9]{1,3})")
            && Double.parseDouble(value) > 0;
      case "jobmanager.memory.process.size":
      case "taskmanager.memory.process.size":
        return value.matches("(?i)[1-9][0-9]{0,8}\\s*(b|k|kb|kib|m|mb|mib|g|gb|gib|t|tb|tib)");
      case "restart.attempt.max.count":
      case "restart.attempt.interval.min":
        return value.matches("[0-9]{1,10}") && Long.parseLong(value) <= Integer.MAX_VALUE;
      case "restart.attempt.enable":
      case "args.base64.enabled":
        return "true".equals(value) || "false".equals(value);
      case "execution.checkpointing.interval":
      case "execution.checkpointing.timeout":
        return value.matches("[1-9][0-9]{0,5} ?(ms|s|min)");
      default:
        return true;
    }
  }

  private void validateProviderSpec(ManagedFlinkProvider provider, Spec spec, String engine) {
    String issue = provider.specIssue(spec, engine);
    check(issue == null, HttpStatus.UNPROCESSABLE_ENTITY, issue);
  }
}
