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

import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Command;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CommandType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.ConnectionInput;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.EnvironmentInput;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobInput;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Spec;

import lombok.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.ServletRequestBindingException;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.check;

@RestController
@RequestMapping("/flink/managed")
@ConditionalOnProperty(name = "streampark.managed-flink.enabled", havingValue = "true")
public class ManagedFlinkController {

  private final ManagedFlinkCatalog catalog;
  private final ManagedFlinkJobs jobs;
  private final ManagedFlinkArtifacts artifacts;
  private final ManagedFlinkStore store;

  ManagedFlinkController(
      ManagedFlinkCatalog catalog,
      ManagedFlinkJobs jobs,
      ManagedFlinkArtifacts artifacts,
      ManagedFlinkStore store) {
    this.catalog = catalog;
    this.jobs = jobs;
    this.artifacts = artifacts;
    this.store = store;
  }

  @GetMapping("/connections")
  Body<List<Map<String, Object>>> connections(
      @RequestParam long teamId, @RequestParam(defaultValue = "0") int offset) {
    return ok(catalog.connections(teamId, offset(offset)));
  }

  @PostMapping("/connections")
  Body<String> createConnection(
      @RequestParam long teamId, @RequestBody ConnectionInput input, HttpServletRequest request) {
    check(
        input.getSecretKey() == null || input.getSecretKey().isEmpty() || request.isSecure(),
        HttpStatus.BAD_REQUEST,
        "An HTTPS request is required to submit credentials");
    return ok(catalog.createConnection(teamId, input));
  }

  @PatchMapping("/connections/{id}")
  Body<Void> renameConnection(
      @RequestParam long teamId, @PathVariable String id, @RequestBody ConnectionInput input) {
    catalog.renameConnection(teamId, id, input);
    return ok(null);
  }

  @PostMapping("/connections/{id}/rotate")
  Body<Map<String, String>> rotate(
      @RequestParam long teamId,
      @PathVariable String id,
      @RequestParam String environmentId,
      @RequestBody ConnectionInput input,
      HttpServletRequest request) {
    check(
        request.isSecure(),
        HttpStatus.BAD_REQUEST,
        "An HTTPS request is required to rotate credentials");
    return ok(catalog.rotateConnection(teamId, id, environmentId, input));
  }

  @GetMapping("/environments")
  Body<List<Map<String, Object>>> environments(
      @RequestParam long teamId, @RequestParam(defaultValue = "0") int offset) {
    return ok(catalog.environments(teamId, offset(offset)));
  }

  @PostMapping("/environments")
  Body<String> createEnvironment(@RequestParam long teamId, @RequestBody EnvironmentInput input) {
    return ok(catalog.saveEnvironment(teamId, null, input));
  }

  @PatchMapping("/environments/{id}")
  Body<String> updateEnvironment(
      @RequestParam long teamId, @PathVariable String id, @RequestBody EnvironmentInput input) {
    return ok(catalog.saveEnvironment(teamId, id, input));
  }

  @PostMapping("/environments/{id}/probe")
  Body<Map<String, String>> probe(@RequestParam long teamId, @PathVariable String id) {
    return ok(catalog.probe(teamId, id));
  }

  @GetMapping("/environments/{id}/capabilities")
  Body<Map<String, String>> capabilities(@RequestParam long teamId, @PathVariable String id) {
    return ok(catalog.capabilities(teamId, id));
  }

  @PostMapping("/apps")
  Body<String> create(@RequestParam long teamId, @RequestBody JobInput input) {
    return ok(Long.toString(jobs.create(teamId, input)));
  }

  @GetMapping("/apps/{appId}")
  Body<Map<String, Object>> detail(@RequestParam long teamId, @PathVariable long appId) {
    return ok(jobs.detail(teamId, appId));
  }

  @PatchMapping("/apps/{appId}")
  Body<Void> edit(
      @RequestParam long teamId, @PathVariable long appId, @RequestBody JobInput input) {
    jobs.edit(teamId, appId, input);
    return ok(null);
  }

  @DeleteMapping("/apps/{appId}")
  Body<Void> archive(@RequestParam long teamId, @PathVariable long appId) {
    jobs.archive(teamId, appId);
    return ok(null);
  }

  @PostMapping("/apps/{appId}/validate")
  Body<Map<String, String>> validate(
      @RequestParam long teamId,
      @PathVariable long appId,
      @RequestBody(required = false) Spec candidate) {
    jobs.validateDraft(teamId, appId, candidate);
    return ok(Collections.singletonMap("level", "STRUCTURAL"));
  }

  @PostMapping("/apps/{appId}/artifacts")
  Body<String> upload(
      @RequestParam long teamId, @PathVariable long appId, @RequestParam MultipartFile file)
      throws IOException {
    return ok(artifacts.upload(teamId, appId, file));
  }

  @PostMapping("/apps/{appId}/artifacts/project")
  Body<String> importBuild(
      @RequestParam long teamId, @PathVariable long appId, @RequestBody ProjectArtifact input)
      throws IOException {
    return ok(artifacts.importBuild(teamId, appId, input.projectId, input.module, input.jar));
  }

  @lombok.Data
  static class ProjectArtifact {
    private long projectId;
    private String module;
    private String jar;
  }

  @GetMapping("/apps/{appId}/artifacts/{id}")
  Body<Map<String, Object>> artifact(
      @RequestParam long teamId, @PathVariable long appId, @PathVariable String id) {
    requireRead(teamId, appId);
    Map<String, Object> artifact = jobs.artifact(appId, id);
    return ok(view(artifact, "id", "digest", "size_bytes", "main_class", "created_at"));
  }

  @GetMapping("/apps/{appId}/releases")
  Body<List<Map<String, Object>>> releases(
      @RequestParam long teamId,
      @PathVariable long appId,
      @RequestParam(defaultValue = "0") int offset) {
    requireRead(teamId, appId);
    return ok(
        store
            .jdbc()
            .queryForList(
                "select id, release_no, draft_revision, config_version, "
                    + "spec_hash, provider_version, status, created_at from t_flink_managed_release "
                    + "where app_id=? order by release_no desc limit 100 offset ?",
                appId,
                offset(offset)));
  }

  @GetMapping("/apps/{appId}/releases/{id}")
  Body<Map<String, Object>> release(
      @RequestParam long teamId, @PathVariable long appId, @PathVariable String id) {
    requireRead(teamId, appId);
    return ok(
        view(
            jobs.release(appId, id),
            "id",
            "release_no",
            "draft_revision",
            "config_version",
            "spec",
            "spec_hash",
            "provider_version",
            "status",
            "created_at"));
  }

  @GetMapping("/apps/{appId}/runs")
  Body<List<Map<String, Object>>> runs(
      @RequestParam long teamId,
      @PathVariable long appId,
      @RequestParam(defaultValue = "0") int offset) {
    requireRead(teamId, appId);
    return ok(
        store
            .jdbc()
            .queryForList(
                "select * from t_flink_managed_run where app_id=? "
                    + "order by started_at desc,id desc limit 100 offset ?",
                appId,
                offset(offset)));
  }

  @GetMapping("/apps/{appId}/runs/{id}")
  Body<Map<String, Object>> run(
      @RequestParam long teamId, @PathVariable long appId, @PathVariable String id) {
    requireRead(teamId, appId);
    return ok(
        store.require("select * from t_flink_managed_run where app_id=? and id=?", appId, id));
  }

  @GetMapping("/apps/{appId}/runs/{id}/{source:events|logs|metrics}")
  Body<Map<String, Object>> diagnostics(
      @RequestParam long teamId,
      @PathVariable long appId,
      @PathVariable String id,
      @PathVariable String source) {
    requireRead(teamId, appId);
    return ok(jobs.diagnostics(teamId, appId, id, source));
  }

  @GetMapping("/apps/{appId}/snapshots")
  Body<List<Map<String, Object>>> snapshots(
      @RequestParam long teamId,
      @PathVariable long appId,
      @RequestParam(defaultValue = "0") int offset) {
    requireRead(teamId, appId);
    return ok(
        store
            .jdbc()
            .queryForList(
                "select id,run_id,release_id,state,restorable,created_at,observed_at "
                    + "from t_flink_managed_snapshot where app_id=? order by created_at desc,id desc limit 100 offset ?",
                appId,
                offset(offset)));
  }

  @GetMapping("/apps/{appId}/restore-options")
  Body<Map<String, Object>> restoreOptions(@RequestParam long teamId, @PathVariable long appId) {
    requireRead(teamId, appId);
    Map<String, Object> job = jobs.job(teamId, appId, false);
    Map<String, Object> options = new LinkedHashMap<>();
    options.put("releaseId", job.get("active_release_id"));
    options.put(
        "snapshots",
        store
            .jdbc()
            .queryForList(
                "select id,run_id,release_id,created_at "
                    + "from t_flink_managed_snapshot where app_id=? and release_id=? and restorable=true "
                    + "and state='AVAILABLE' order by created_at desc,id desc limit 100",
                appId,
                job.get("active_release_id")));
    return ok(options);
  }

  @PostMapping("/apps/{appId}/{action:publish|rollback|start|stop|snapshots}")
  ResponseEntity<Body<Map<String, Object>>> command(
      @RequestParam long teamId,
      @PathVariable long appId,
      @PathVariable String action,
      @RequestHeader("Idempotency-Key") String key,
      @RequestBody Command cmd) {
    CommandType type =
        "snapshots".equals(action)
            ? CommandType.SNAPSHOT
            : CommandType.valueOf(action.toUpperCase(java.util.Locale.ROOT));
    String operation = jobs.enqueue(teamId, appId, type, key, cmd);
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("operationId", operation);
    data.put("appId", Long.toString(appId));
    data.put("status", operationRow(teamId, appId, operation).get("status"));
    data.put("pollAfterMs", 2000);
    return ResponseEntity.accepted().body(ok(data));
  }

  @GetMapping("/apps/{appId}/operations")
  Body<List<Map<String, Object>>> operations(
      @RequestParam long teamId,
      @PathVariable long appId,
      @RequestParam(defaultValue = "0") int offset) {
    requireRead(teamId, appId);
    return ok(
        store
            .jdbc()
            .queryForList(
                "select id,command_type,status,step_index,created_at,updated_at "
                    + "from t_flink_managed_operation where team_id=? and app_id=? "
                    + "order by created_at desc,id desc limit 100 offset ?",
                teamId,
                appId,
                offset(offset)));
  }

  @GetMapping("/apps/{appId}/operations/{id}")
  Body<Map<String, Object>> operation(
      @RequestParam long teamId, @PathVariable long appId, @PathVariable String id) {
    requireRead(teamId, appId);
    Map<String, Object> operation =
        view(
            operationRow(teamId, appId, id),
            "id",
            "command_type",
            "status",
            "step_index",
            "created_at",
            "updated_at",
            "created_by");
    operation.put(
        "steps",
        store
            .jdbc()
            .queryForList(
                "select step_index,action,state,request_id,error_code,"
                    + "started_at,finished_at from t_flink_managed_operation_step "
                    + "where operation_id=? order by step_index",
                id));
    return ok(operation);
  }

  @PostMapping("/apps/{appId}/operations/{id}/reconcile")
  Body<Void> reconcile(
      @RequestParam long teamId, @PathVariable long appId, @PathVariable String id) {
    catalog.authorize(teamId, "app:release");
    jobs.job(teamId, appId, false);
    Map<String, Object> row = operationRow(teamId, appId, id);
    long now = System.currentTimeMillis();
    // An existing attempt is required to guarantee that reconciliation takes the read branch.
    int count =
        store
            .jdbc()
            .update(
                "update t_flink_managed_operation set status='WAITING_PROVIDER',"
                    + "deadline_at=?,next_run_at=?,updated_at=? where id=? and team_id=? and app_id=? "
                    + "and status='VERIFY_REQUIRED' and exists (select 1 from t_flink_managed_operation_step "
                    + "where operation_id=? and step_index=?)",
                now + 300_000,
                now,
                now,
                id,
                teamId,
                appId,
                id,
                row.get("step_index"));
    check(count == 1, HttpStatus.CONFLICT, "No dispatched step available for reconciliation");
    return ok(null);
  }

  private Map<String, Object> operationRow(long team, long app, String id) {
    return store.require(
        "select id,command_type,status,step_index,created_at,updated_at,created_by "
            + "from t_flink_managed_operation where team_id=? and app_id=? and id=?",
        team,
        app,
        id);
  }

  private void requireRead(long team, long app) {
    catalog.authorize(team, "app:detail");
    jobs.job(team, app, false);
  }

  private int offset(int value) {
    check(value >= 0 && value <= 100_000, HttpStatus.BAD_REQUEST, "Invalid page offset");
    return value;
  }

  private Map<String, Object> view(Map<String, Object> row, String... columns) {
    Map<String, Object> result = new LinkedHashMap<>();
    for (String column : columns) {
      result.put(column, row.get(column));
    }
    return result;
  }

  private static <T> Body<T> ok(T data) {
    return new Body<>("success", "200", null, data);
  }

  @Value
  static class Body<T> {
    String status;
    String code;
    String message;
    T data;
  }

  @ExceptionHandler(ResponseStatusException.class)
  ResponseEntity<Body<Void>> status(ResponseStatusException error) {
    return failure(error.getStatus(), error.getReason());
  }

  @ExceptionHandler(DuplicateKeyException.class)
  ResponseEntity<Body<Void>> conflict() {
    return failure(HttpStatus.CONFLICT, "A managed Flink object with this key already exists");
  }

  @ExceptionHandler({
    HttpMessageNotReadableException.class,
    ServletRequestBindingException.class,
    MethodArgumentTypeMismatchException.class
  })
  ResponseEntity<Body<Void>> invalid() {
    return failure(HttpStatus.BAD_REQUEST, "Invalid managed Flink request");
  }

  @ExceptionHandler(IOException.class)
  ResponseEntity<Body<Void>> unreadableArtifact() {
    return failure(HttpStatus.UNPROCESSABLE_ENTITY, "The managed artifact is unavailable");
  }

  private ResponseEntity<Body<Void>> failure(HttpStatus status, String message) {
    return ResponseEntity.status(status)
        .body(new Body<>("error", Integer.toString(status.value()), message, null));
  }
}
