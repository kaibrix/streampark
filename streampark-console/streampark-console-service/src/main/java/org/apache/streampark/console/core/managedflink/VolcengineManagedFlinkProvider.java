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

import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CloudState;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Context;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Outcome;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.ReadStatus;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.RestoreMode;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Target;
import org.apache.streampark.console.core.managedflink.VolcengineOpenApi.Reply;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.volcengine.sign.Credentials;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.streampark.console.core.managedflink.VolcengineExecutable.revision;
import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.FLINK_VERSION;
import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.GWS_VERSION;
import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.object;
import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.text;

@Component
class VolcengineManagedFlinkProvider implements ManagedFlinkProvider {

  private static final List<String> ENGINES =
      Arrays.asList("FLINK_VERSION_1_16", "FLINK_VERSION_1_17", "FLINK_VERSION_1_20");
  private static final List<String> TERMINAL = Arrays.asList("STOPPED", "FINISHED", "FAILED");
  private static final DateTimeFormatter CLOUD_TIME =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private static final List<String> FEATURES =
      Arrays.asList(
          "IDENTITY",
          "DIRECTORY_READ",
          "RESOURCE_POOL_READ",
          "PROJECT_JOB_READ",
          "PUBLISH_SQL",
          "PUBLISH_SQL_UDF",
          "PUBLISH_JAR",
          "START",
          "STOP",
          "SNAPSHOT",
          "SNAPSHOT_THEN_STOP",
          "RESTORE_FROM_SNAPSHOT",
          "RESTORE_FROM_LATEST",
          "ARTIFACT_UPLOAD",
          "ARTIFACT_RUNTIME_READ",
          "LOG_SEARCH",
          "METRICS");

  private final VolcengineProfileBinding binding;

  VolcengineManagedFlinkProvider(VolcengineProfileBinding binding) {
    this.binding = binding;
  }

  @Override
  public String id() {
    return "volcengine";
  }

  @Override
  public boolean permitsServerCredential(long team, String account) {
    return binding.permitsConnection(team, account);
  }

  @Override
  public Map<String, String> capabilities(Context context) {
    Map<String, String> report = new LinkedHashMap<>();
    for (String feature : FEATURES) {
      report.put(feature, "UNVERIFIED");
    }
    if (!"cn-beijing".equals(context.getRegion())
        || !ENGINES.contains(context.getTarget().getEngineRef())) {
      for (String feature : FEATURES) {
        report.put(feature, "UNSUPPORTED");
      }
    } else if (binding.permitsSqlWrites(context)) {
      report.put("PUBLISH_SQL", "AVAILABLE");
      report.put("START", "AVAILABLE");
      report.put("STOP", "AVAILABLE");
      if (binding.permitsSnapshots(context)) {
        report.put("SNAPSHOT", "AVAILABLE");
        report.put("SNAPSHOT_THEN_STOP", "AVAILABLE");
        report.put("RESTORE_FROM_SNAPSHOT", "AVAILABLE");
      }
    }
    return report;
  }

  @Override
  public Map<String, String> probe(Context context) {
    Map<String, String> report = capabilities(context);
    try {
      VolcengineOpenApi api = client(context);
      if (api == null) {
        return report;
      }
      try {
        Reply identity = api.identity();
        report.put(
            "IDENTITY",
            identity.ok() && same(identity.data(), "AccountId", context.getAccountRef())
                ? "AVAILABLE"
                : "NEEDS_CONFIGURATION");
      } catch (IOException e) {
        report.put("IDENTITY", "NEEDS_CONFIGURATION");
      }
      Target target = context.getTarget();
      Reply pools =
          api.call(
              "ListGMCSResources",
              FLINK_VERSION,
              Collections.emptyMap(),
              fields(
                  "ProjectId",
                  target.getProjectId(),
                  "Name",
                  target.getResourcePoolName(),
                  "PageNum",
                  1,
                  "PageSize",
                  50));
      boolean matched =
          rows(pools.data().get("DataList")).stream()
              .anyMatch(
                  pool ->
                      same(pool, "Id", target.getResourcePoolId())
                          && same(pool, "ProjectId", target.getProjectId())
                          && same(pool, "Status", "RUNNING"));
      report.put("RESOURCE_POOL_READ", pools.ok() && matched ? "AVAILABLE" : "NEEDS_CONFIGURATION");
      Map<String, String> query = scope(target);
      query.put("Type", "JOB");
      Reply directories = api.call("ListGWSDirectory", GWS_VERSION, query, new JsonObject());
      boolean directoryFound =
          directories.ok()
              && rows(directories.result).stream()
                  .anyMatch(dir -> same(dir, "Id", target.getDirectoryId()));
      report.put("DIRECTORY_READ", directoryFound ? "AVAILABLE" : "NEEDS_CONFIGURATION");
      Reply jobs = listJobs(api, target, null);
      report.put("PROJECT_JOB_READ", jobs.ok() ? "AVAILABLE" : "NEEDS_CONFIGURATION");
    } catch (IOException e) {
      report.put("PROJECT_JOB_READ", "NEEDS_CONFIGURATION");
    }
    return report;
  }

  @Override
  public CloudState inspect(Context context, String jobId) {
    try {
      VolcengineOpenApi api = client(context);
      if (api == null) {
        return readFailure(jobId, ReadStatus.AUTH_ERROR);
      }
      Target target = context.getTarget();
      JsonObject application = application(api, target, jobId);
      if (application == null) {
        return readFailure(jobId, ReadStatus.STALE);
      }
      Reply response = instances(api, target, jobId);
      JsonElement records = response.data().get("Records");
      if (!response.ok()
          || records == null
          || !records.isJsonArray()
          || !"1".equals(text(response.data(), "Current"))) {
        return readFailure(jobId, ReadStatus.STALE);
      }
      List<JsonObject> runs = rows(records);
      if (records.getAsJsonArray().size() != runs.size()
          || safeNumber(text(response.data(), "Total")) != runs.size()) {
        return readFailure(jobId, ReadStatus.STALE);
      }
      List<JsonObject> live =
          runs.stream()
              .filter(run -> !TERMINAL.contains(text(run, "State")))
              .collect(Collectors.toList());
      if (live.size() > 1 || runs.stream().anyMatch(run -> !isInstance(run, target, jobId))) {
        return readFailure(jobId, ReadStatus.STALE);
      }
      JsonObject current =
          live.isEmpty()
              ? runs.stream().max(Comparator.comparingLong(this::startedAt)).orElse(null)
              : live.get(0);
      return new CloudState(
          ReadStatus.FOUND,
          jobId,
          revision(application),
          current == null ? null : text(current, "Id"),
          current == null ? null : text(current, "ApplicationId"),
          current == null ? null : text(current, "State"),
          current == null ? 0 : startedAt(current));
    } catch (IOException | RuntimeException e) {
      return readFailure(jobId, ReadStatus.STALE);
    }
  }

  @Override
  public Observation dispatch(Context context, OperationStep step) {
    if (!binding.permitsSqlWrites(context)) {
      return Observation.outcome(Outcome.REJECTED, "CAPABILITY_UNVERIFIED");
    }
    try {
      VolcengineOpenApi api = client(context);
      Reply identity = api.identity();
      if (!identity.ok() || !same(identity.data(), "AccountId", context.getAccountRef())) {
        return rejected("CLOUD_IDENTITY_NOT_VERIFIED");
      }
      Target target = context.getTarget();
      switch (step.getAction()) {
        case CREATE_DRAFT:
          if (!supportedSpec(context, step)) {
            return rejected("UNVERIFIED_SPEC");
          }
          return ack(
              api.call(
                  "CreateGWSApplicationDraft",
                  GWS_VERSION,
                  scope(target),
                  draftFields(step, target)),
              "Id",
              null);
        case UPDATE_DRAFT:
          if (!supportedSpec(context, step)) {
            return rejected("UNVERIFIED_SPEC");
          }
          JsonObject draft = draft(api, target, step.getDraftId());
          if (draft == null) {
            return rejected("DRAFT_NOT_VERIFIED");
          }
          JsonObject update = draftFields(step, target);
          copy(
              draft,
              update,
              "JobId",
              "DirectoryName",
              "Version",
              "ResourceVersion",
              "Platform",
              "State",
              "ModifiedTime");
          update.addProperty("Id", step.getDraftId());
          update.addProperty("AppDraftId", step.getDraftId());
          VolcengineExecutable.writeDraft(update, step);
          return ack(
              api.call("UpdateGWSApplicationDraft", GWS_VERSION, scope(target), update),
              "Id",
              null);
        case PUBLISH:
          JsonObject desired = draft(api, target, step.getDraftId());
          if (!supportedSpec(context, step) || !VolcengineExecutable.matches(desired, step, true)) {
            return rejected("DRAFT_NOT_VERIFIED");
          }
          Map<String, String> query = scope(target);
          query.put("Id", step.getDraftId());
          return ack(
              api.call(
                  "DeployGWSApplicationDraft",
                  GWS_VERSION,
                  query,
                  fields(
                      "Queue",
                      target.getResourcePoolId(),
                      "ResourcePool",
                      target.getResourcePoolName(),
                      "Priority",
                      "1",
                      "SchedulePolicy",
                      "GANG",
                      "ScheduleTimeout",
                      60)),
              null,
              "Id");
        case START:
          return supportedSpec(context, step)
              ? start(api, target, step, binding.permitsSnapshots(context))
              : rejected("UNVERIFIED_SPEC");
        case SNAPSHOT:
          if (!binding.permitsSnapshots(context) || !isCurrentRunning(api, target, step)) {
            return rejected("SNAPSHOT_INSTANCE_NOT_VERIFIED");
          }
          return ack(
              api.call(
                  "CreateGWSSavepoint",
                  FLINK_VERSION,
                  Collections.emptyMap(),
                  fields("Id", step.getProviderJobId(), "Description", snapshotName(step))),
              null,
              null);
        case STOP:
          JsonObject running =
              instance(
                  api, target, step.getProviderJobId(), step.getCommand().getProviderInstance());
          if (running == null
              || !same(running, "State", "RUNNING")
              || application(api, target, step.getProviderJobId()) == null) {
            return rejected("INSTANCE_NOT_VERIFIED");
          }
          return ack(
              api.call(
                  "CancelApplicationInstance",
                  FLINK_VERSION,
                  Collections.emptyMap(),
                  fields("InstanceId", step.getCommand().getProviderInstance())),
              null,
              null);
        default:
          return rejected("CAPABILITY_UNVERIFIED");
      }
    } catch (IOException | RuntimeException e) {
      return Observation.outcome(Outcome.UNKNOWN, "RESULT_UNCERTAIN");
    }
  }

  @Override
  public Observation observe(Context context, OperationStep step, Observation previous) {
    try {
      VolcengineOpenApi api = client(context);
      Target target = context.getTarget();
      Observation observation = Observation.outcome(Outcome.CONFIRMED, null);
      switch (step.getAction()) {
        case CREATE_DRAFT:
          String draftId = previous == null ? null : previous.getDraftId();
          JsonObject matchedDraft = draft(api, target, draftId);
          if (matchedDraft == null
              || !same(matchedDraft, "JobName", step.getCommand().getJobName())
              || !same(
                  matchedDraft,
                  "JobType",
                  VolcengineExecutable.providerType(step.getCommand().getSpec().getType()))
              || !same(matchedDraft, "EngineVersion", step.getCommand().getEngineRef())) {
            return awaiting();
          }
          observation.setDraftId(draftId);
          return observation;
        case UPDATE_DRAFT:
          if (!VolcengineExecutable.matches(draft(api, target, step.getDraftId()), step, true)) {
            return awaiting();
          }
          observation.setDraftId(step.getDraftId());
          observation.setSpecHash(step.getCommand().getSpecHash());
          return observation;
        case PUBLISH:
          return observedPublish(api, target, step, previous, observation);
        case START:
          if (previous == null || previous.getInstanceId() == null) {
            return awaiting();
          }
          JsonObject instance =
              instance(api, target, step.getProviderJobId(), previous.getInstanceId());
          if (instance == null
              || !VolcengineExecutable.matches(instance, step, false)
              || !Objects.equals(revision(instance), step.getCommand().getProviderVersion())
              || !Arrays.asList("RUNNING", "FAILED").contains(text(instance, "State"))
              || text(instance, "VersionId") == null
              || !matchesRestore(api, step, instance)) {
            return awaiting();
          }
          observation.setJobId(step.getProviderJobId());
          observation.setInstanceId(previous.getInstanceId());
          observation.setRuntimeId(text(instance, "ApplicationId"));
          observation.setVersion(step.getCommand().getProviderVersion());
          observation.setRemoteVersionId(text(instance, "VersionId"));
          observation.setRunState(text(instance, "State"));
          observation.setRestoredFrom(step.getCommand().getRestoreRef());
          return observation;
        case SNAPSHOT:
          List<JsonObject> points = savepoints(api, step.getProviderJobId());
          List<JsonObject> matches =
              points.stream()
                  .filter(point -> same(point, "Description", snapshotName(step)))
                  .collect(Collectors.toList());
          if (matches.size() != 1
              || !availableSavepoint(matches.get(0))
              || !isCurrentRunning(api, target, step)) {
            return awaiting();
          }
          String savepoint = text(matches.get(0), "SavepointId");
          observation.setJobId(step.getProviderJobId());
          observation.setInstanceId(step.getCommand().getProviderInstance());
          observation.setRecordId(savepoint);
          observation.setRestoreRef(savepoint);
          return observation;
        case STOP:
          String stopped = step.getCommand().getProviderInstance();
          JsonObject completed = instance(api, target, step.getProviderJobId(), stopped);
          if (completed == null || !same(completed, "State", "STOPPED")) {
            return awaiting();
          }
          observation.setJobId(step.getProviderJobId());
          observation.setInstanceId(stopped);
          observation.setRunState("STOPPED");
          return observation;
        default:
          return awaiting();
      }
    } catch (IOException | RuntimeException e) {
      return awaiting();
    }
  }

  private Observation start(
      VolcengineOpenApi api, Target target, OperationStep step, boolean snapshotEnabled)
      throws IOException {
    RestoreMode mode = step.getCommand().getRequest().getRestore().getMode();
    if (mode != RestoreMode.FROM_NEW && (mode != RestoreMode.FROM_SNAPSHOT || !snapshotEnabled)) {
      return rejected("UNVERIFIED_START_CONTRACT");
    }
    JsonObject published = application(api, target, step.getProviderJobId());
    if (!VolcengineExecutable.matches(published, step, false)
        || !Objects.equals(revision(published), step.getCommand().getProviderVersion())) {
      return rejected("PUBLISHED_REVISION_MISMATCH");
    }
    JsonObject parameters =
        fields(
            "Id",
            step.getProviderJobId(),
            "Type",
            mode == RestoreMode.FROM_NEW ? "FROM_NEW" : "FROM_SAVEPOINT");
    if (mode == RestoreMode.FROM_SNAPSHOT) {
      String snapshot = step.getCommand().getRestoreRef();
      if (savepointPath(api, step.getProviderJobId(), snapshot) == null) {
        return rejected("RESTORE_SNAPSHOT_NOT_VERIFIED");
      }
      parameters.addProperty("SavepointId", snapshot);
    }
    Reply result = api.call("StartGWSApplication", GWS_VERSION, scope(target), parameters);
    Observation ack = ack(result, null, null);
    ack.setInstanceId(text(result.data(), "InstanceId"));
    return ack;
  }

  private boolean isCurrentRunning(VolcengineOpenApi api, Target target, OperationStep step)
      throws IOException {
    String instanceId = step.getCommand().getProviderInstance();
    JsonObject job = application(api, target, step.getProviderJobId());
    JsonObject current =
        instanceId == null ? null : instance(api, target, step.getProviderJobId(), instanceId);
    return job != null
        && current != null
        && same(job, "ApplicationId", instanceId)
        && same(current, "State", "RUNNING");
  }

  private String snapshotName(OperationStep step) {
    return "streampark-operation-" + step.getOperationId();
  }

  private List<JsonObject> savepoints(VolcengineOpenApi api, String job) throws IOException {
    Reply response =
        api.call("ListGWSSavepoint", FLINK_VERSION, Collections.emptyMap(), fields("Id", job));
    return response.ok() ? rows(response.data().get("SavepointInfos")) : Collections.emptyList();
  }

  private boolean availableSavepoint(JsonObject point) {
    return same(point, "Status", "AVAILABLE")
        && text(point, "SavepointId") != null
        && text(point, "SavepointPath") != null;
  }

  private String savepointPath(VolcengineOpenApi api, String job, String id) throws IOException {
    List<JsonObject> matches =
        savepoints(api, job).stream()
            .filter(point -> same(point, "SavepointId", id))
            .collect(Collectors.toList());
    return matches.size() == 1 && availableSavepoint(matches.get(0))
        ? text(matches.get(0), "SavepointPath")
        : null;
  }

  private boolean matchesRestore(VolcengineOpenApi api, OperationStep step, JsonObject instance)
      throws IOException {
    JsonObject config = VolcengineExecutable.objectValue(instance.get("Conf"));
    if (config == null) {
      return false;
    }
    String actual = text(config, "checkpoint.restore-savepoint-path");
    String requested = step.getCommand().getRestoreRef();
    if (requested == null) {
      return actual == null || actual.isEmpty();
    }
    String expected = savepointPath(api, step.getProviderJobId(), requested);
    return expected != null && expected.equals(actual);
  }

  private Observation observedPublish(
      VolcengineOpenApi api,
      Target target,
      OperationStep step,
      Observation previous,
      Observation result)
      throws IOException {
    String job = step.getProviderJobId();
    if (job == null && previous != null) {
      job = previous.getJobId();
    }
    if (job == null) {
      Reply listing = listJobs(api, target, step.getCommand().getJobName());
      List<JsonObject> records =
          rows(listing.data().get("Records")).stream()
              .filter(
                  row ->
                      same(row, "AppDraftId", step.getDraftId())
                          && same(row, "JobName", step.getCommand().getJobName()))
              .collect(Collectors.toList());
      if (!listing.ok() || records.size() != 1) {
        return awaiting();
      }
      job = text(records.get(0), "Id");
    }
    JsonObject published = application(api, target, job);
    if (published == null
        && step.getProviderJobId() == null
        && previous != null
        && previous.getJobId() != null) {
      Observation fallback = new Observation();
      fallback.setRequestId(previous.getRequestId());
      return observedPublish(api, target, step, fallback, result);
    }
    String version = revision(published);
    if (!VolcengineExecutable.matches(published, step, false)
        || !same(published, "AppDraftId", step.getDraftId())
        || version == null
        || Objects.equals(version, step.getCommand().getProviderVersion())) {
      return awaiting();
    }
    result.setJobId(job);
    result.setVersion(version);
    result.setRemoteVersionId(text(published, "VersionName"));
    result.setSpecHash(step.getCommand().getSpecHash());
    return result;
  }

  private VolcengineOpenApi client(Context context) throws IOException {
    if (!"cn-beijing".equals(context.getRegion())
        || !ENGINES.contains(context.getTarget().getEngineRef())) {
      return null;
    }
    try {
      Credentials credentials = binding.credentials(context);
      return credentials == null ? null : new VolcengineOpenApi(context.getRegion(), credentials);
    } catch (Exception e) {
      throw new IOException("Volcengine credentials unavailable");
    }
  }

  private JsonObject draft(VolcengineOpenApi api, Target target, String id) throws IOException {
    if (id == null) {
      return null;
    }
    Reply reply = api.call("GetGWSApplicationDraft", GWS_VERSION, scope(target), fields("Id", id));
    JsonObject record = reply.data();
    return reply.ok()
            && scoped(record, target, id)
            && same(record, "DirectoryId", target.getDirectoryId())
        ? record
        : null;
  }

  private JsonObject application(VolcengineOpenApi api, Target target, String id)
      throws IOException {
    if (id == null) {
      return null;
    }
    Reply reply =
        api.call(
            "GetGWSApplication",
            GWS_VERSION,
            Collections.emptyMap(),
            fields("Id", id, "ProjectId", target.getProjectId()));
    JsonObject application = reply.data();
    if (!reply.ok()
        || !scoped(application, target, id)
        || !same(object(application, "DeployRequest"), "Queue", target.getResourcePoolId())
        || draft(api, target, text(application, "AppDraftId")) == null) {
      return null;
    }
    return application;
  }

  private JsonObject instance(VolcengineOpenApi api, Target target, String job, String id)
      throws IOException {
    Reply reply =
        api.call(
            "GetApplicationInstance",
            FLINK_VERSION,
            Collections.emptyMap(),
            fields("InstanceId", id));
    return reply.ok() && same(reply.data(), "Id", id) && isInstance(reply.data(), target, job)
        ? reply.data()
        : null;
  }

  private Reply instances(VolcengineOpenApi api, Target target, String job) throws IOException {
    return api.call(
        "ListApplicationInstance",
        FLINK_VERSION,
        Collections.emptyMap(),
        fields(
            "ProjectId",
            target.getProjectId(),
            "JobId",
            Long.parseLong(job),
            "PageNum",
            "1",
            "PageSize",
            "50"));
  }

  private Reply listJobs(VolcengineOpenApi api, Target target, String name) throws IOException {
    return api.call(
        "ListGWSApplication",
        FLINK_VERSION,
        Collections.emptyMap(),
        fields(
            "ProjectId",
            target.getProjectId(),
            "ResourcePool",
            target.getResourcePoolName(),
            "JobName",
            name,
            "PageNum",
            1,
            "PageSize",
            50));
  }

  private JsonObject draftFields(OperationStep step, Target target) {
    return fields(
        "ProjectId",
        target.getProjectId(),
        "DirectoryId",
        target.getDirectoryId(),
        "EngineVersion",
        step.getCommand().getEngineRef(),
        "JobName",
        step.getCommand().getJobName(),
        "JobType",
        VolcengineExecutable.providerType(step.getCommand().getSpec().getType()));
  }

  private boolean supportedSpec(Context context, OperationStep step) {
    return step.getCommand().getSpec().getType() == JobType.SQL
        && step.getCommand().getSpec().getUdfArtifactId() == null
        && VolcengineExecutable.supportedConfiguration(step.getCommand().getSpec())
        && step.getCommand().getReleaseMarker() != null;
  }

  private boolean same(JsonObject record, String field, String expected) {
    return expected != null && expected.equals(text(record, field));
  }

  private boolean scoped(JsonObject record, Target target, String id) {
    return same(record, "Id", id) && same(record, "ProjectId", target.getProjectId());
  }

  private boolean isInstance(JsonObject row, Target target, String job) {
    return same(row, "ProjectId", target.getProjectId())
        && same(row, "JobId", job)
        && text(row, "Id") != null;
  }

  private Map<String, String> scope(Target target) {
    Map<String, String> query = new LinkedHashMap<>();
    query.put("ProjectId", target.getProjectId());
    return query;
  }

  private JsonObject fields(Object... entries) {
    JsonObject result = new JsonObject();
    for (int i = 0; i < entries.length; i += 2) {
      Object value = entries[i + 1];
      if (value instanceof Number) {
        result.addProperty((String) entries[i], (Number) value);
      } else if (value != null) {
        result.addProperty((String) entries[i], value.toString());
      }
    }
    return result;
  }

  private void copy(JsonObject source, JsonObject target, String... columns) {
    for (String column : columns) {
      if (source.has(column)) {
        target.add(column, source.get(column));
      }
    }
  }

  private List<JsonObject> rows(JsonElement elements) {
    List<JsonObject> rows = new ArrayList<>();
    if (elements != null && elements.isJsonArray()) {
      JsonArray array = elements.getAsJsonArray();
      for (JsonElement entry : array) {
        if (entry.isJsonObject()) {
          rows.add(entry.getAsJsonObject());
        }
      }
    }
    return rows;
  }

  private long safeNumber(String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private long startedAt(JsonObject run) {
    return LocalDateTime.parse(text(run, "StartTime"), CLOUD_TIME)
        .atZone(ZoneId.of("Asia/Shanghai"))
        .toInstant()
        .toEpochMilli();
  }

  private CloudState readFailure(String job, ReadStatus status) {
    return new CloudState(status, job, null, null, null, null, 0);
  }

  private Observation ack(Reply reply, String draftField, String jobField) {
    if (!reply.ok() || "false".equals(text(reply.data(), "Success"))) {
      Observation unknown = Observation.outcome(Outcome.UNKNOWN, "VOLCENGINE_WRITE_UNCERTAIN");
      unknown.setRequestId(reply.requestId);
      return unknown;
    }
    Observation result = Observation.outcome(Outcome.WAITING, null);
    result.setRequestId(reply.requestId);
    if (draftField != null) {
      result.setDraftId(text(reply.data(), draftField));
    }
    if (jobField != null) {
      result.setJobId(text(reply.data(), jobField));
    }
    return result;
  }

  private Observation rejected(String code) {
    return Observation.outcome(Outcome.REJECTED, code);
  }

  private Observation awaiting() {
    return Observation.outcome(Outcome.WAITING, "EVIDENCE_PENDING");
  }
}
