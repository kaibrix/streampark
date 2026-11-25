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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.DependencyArtifact;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.LogQuery;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Outcome;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.ReadStatus;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.RestoreMode;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Target;
import org.apache.streampark.console.core.managedflink.VolcengineOpenApi.ReadFailure;
import org.apache.streampark.console.core.managedflink.VolcengineOpenApi.Reply;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.volcengine.flink20250101.model.AppForStartApplicationInstanceInput;
import com.volcengine.flink20250101.model.DependencyForStartApplicationInstanceInput;
import com.volcengine.flink20250101.model.DeployRequestForStartApplicationInstanceInput;
import com.volcengine.flink20250101.model.GetGWSStartMethodResponse;
import com.volcengine.flink20250101.model.RestoreStrategyForStartApplicationInstanceInput;
import com.volcengine.flink20250101.model.StartApplicationInstanceRequest;
import com.volcengine.sign.Credentials;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.apache.streampark.console.core.managedflink.VolcengineExecutable.revision;
import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.FLINK_VERSION;
import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.GWS_VERSION;
import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.object;
import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.text;

@Component
class VolcengineManagedFlinkProvider implements ManagedFlinkProvider {

  private static final String FLINK_2_2 = "FLINK_VERSION_2_2";
  private static final List<String> ENGINES =
      Arrays.asList(
          "FLINK_VERSION_1_11",
          "FLINK_VERSION_1_16",
          "FLINK_VERSION_1_17",
          "FLINK_VERSION_1_20",
          FLINK_2_2);
  private static final List<String> TERMINAL = Arrays.asList("STOPPED", "FINISHED", "FAILED");
  private static final int INSTANCE_PAGE_SIZE = 50;
  private static final long INSTANCE_SCAN_BUDGET = TimeUnit.SECONDS.toNanos(15);
  private static final DateTimeFormatter CLOUD_TIME =
      DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").withResolverStyle(ResolverStyle.STRICT);
  private static final List<String> FEATURES =
      Arrays.asList(
          "IDENTITY",
          "DIRECTORY_READ",
          "RESOURCE_POOL_READ",
          "PROJECT_JOB_READ",
          "PUBLISH_SQL",
          "PUBLISH_SQL_UDF",
          "PUBLISH_JAR",
          "SUBMIT_INSTANCE",
          "START",
          "CLOUD_RESTART",
          "STOP",
          "SNAPSHOT",
          "SNAPSHOT_THEN_STOP",
          "RESTORE_FROM_SNAPSHOT",
          "RESTORE_FROM_LATEST",
          "RESTORE_FROM_CLOUD_LATEST",
          "RESTORE_FROM_CLOUD_SNAPSHOT",
          "ARTIFACT_UPLOAD",
          "ARTIFACT_RUNTIME_READ",
          "LOG_SEARCH",
          "LOG_LEVEL_FILTER",
          "METRICS");

  private final VolcengineProfileBinding binding;
  private final VolcengineArtifactStore artifacts;
  private final VolcengineTlsLogs tlsLogs;

  VolcengineManagedFlinkProvider(VolcengineProfileBinding binding, VolcengineTlsLogs tlsLogs) {
    this.binding = binding;
    this.artifacts = new VolcengineArtifactStore();
    this.tlsLogs = tlsLogs;
  }

  @Override
  public String id() {
    return "volcengine";
  }

  @Override
  public boolean permitsServerCredential(long team, String account) {
    return binding.permitsConnection(team, account);
  }

  static String jobTypeIssue(ManagedFlinkModels.Spec spec, String engine) {
    return FLINK_2_2.equals(engine) && (spec.getType() != JobType.SQL || spec.isBatchMode())
        ? "This Volcengine engine supports streaming SQL jobs"
        : null;
  }

  @Override
  public String specIssue(ManagedFlinkModels.Spec spec, String engine) {
    String issue = jobTypeIssue(spec, engine);
    if (issue != null) {
      return issue;
    }
    if (!VolcengineExecutable.supportedConfiguration(spec)) {
      return "Flink configuration exceeds the configured Volcengine runtime settings";
    }
    if (spec.getType() == JobType.JAR
        && spec.getArgs().size() == 1
        && spec.getArgs().get(0).isEmpty()) {
      return "A single empty Volcengine program argument cannot be distinguished from no arguments";
    }
    if (spec.getType() == JobType.JAR && !VolcengineExecutable.hasSupportedArgs(spec)) {
      return "Volcengine splits text arguments at whitespace. Use args.base64.enabled=true "
          + "with a main class that decodes the Base64 argument vector";
    }
    return null;
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
      report.put("CLOUD_RESTART", "AVAILABLE");
      report.put("SUBMIT_INSTANCE", "AVAILABLE");
      report.put("STOP", "AVAILABLE");
      if (binding.permitsArtifacts(context)) {
        report.put("PUBLISH_SQL_UDF", "AVAILABLE");
        report.put(
            "PUBLISH_JAR",
            FLINK_2_2.equals(context.getTarget().getEngineRef()) ? "UNSUPPORTED" : "AVAILABLE");
        report.put("ARTIFACT_UPLOAD", "AVAILABLE");
        report.put("ARTIFACT_RUNTIME_READ", "AVAILABLE");
      }
      if (binding.permitsSnapshots(context)) {
        report.put("SNAPSHOT", "AVAILABLE");
        report.put("SNAPSHOT_THEN_STOP", "AVAILABLE");
        report.put("RESTORE_FROM_SNAPSHOT", "AVAILABLE");
        report.put("RESTORE_FROM_LATEST", "AVAILABLE");
        report.put("RESTORE_FROM_CLOUD_LATEST", "AVAILABLE");
        report.put("RESTORE_FROM_CLOUD_SNAPSHOT", "AVAILABLE");
      }
    }
    if (binding.permitsMetricReads(context)) {
      report.put("METRICS", "AVAILABLE");
    }
    report.replace("LOG_SEARCH", "UNVERIFIED", logCapability(context));
    if ("AVAILABLE".equals(report.get("LOG_SEARCH")) && tlsLogs.permitsLevelFilter()) {
      report.put("LOG_LEVEL_FILTER", "AVAILABLE");
    }
    return report;
  }

  private String logCapability(Context context) {
    if (!context.isServerCredential() || !binding.permits(context)) {
      return "UNVERIFIED";
    }
    return tlsLogs.configured(context) ? "AVAILABLE" : "NEEDS_CONFIGURATION";
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
      if (!"AVAILABLE".equals(report.get("IDENTITY"))) {
        return report;
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
          directories.ok() && includesDirectory(directories.result, target.getDirectoryId());
      report.put("DIRECTORY_READ", directoryFound ? "AVAILABLE" : "NEEDS_CONFIGURATION");
      Reply jobs = listJobs(api, target, null);
      report.put("PROJECT_JOB_READ", jobs.ok() ? "AVAILABLE" : "NEEDS_CONFIGURATION");
    } catch (IOException e) {
      report.replace("IDENTITY", "UNVERIFIED", "NEEDS_CONFIGURATION");
      report.put("PROJECT_JOB_READ", "NEEDS_CONFIGURATION");
    }
    return report;
  }

  @Override
  public CloudState inspect(Context context, String jobId) {
    try {
      VolcengineOpenApi api = verifiedClient(context);
      if (api == null) {
        return readFailure(jobId, ReadStatus.AUTH_ERROR);
      }
      Target target = context.getTarget();
      JsonObject application = application(api, target, jobId);
      if (application == null) {
        return readFailure(jobId, ReadStatus.STALE);
      }
      List<JsonObject> runs = instanceHistory(api, target, jobId);
      if (runs == null) {
        return readFailure(jobId, ReadStatus.STALE);
      }
      List<JsonObject> live =
          runs.stream()
              .filter(run -> !TERMINAL.contains(runState(run)))
              .collect(Collectors.toList());
      if (live.size() > 1) {
        return readFailure(jobId, ReadStatus.STALE);
      }
      // The provider lists instances in descending start-time order.
      JsonObject current = live.isEmpty() ? (runs.isEmpty() ? null : runs.get(0)) : live.get(0);
      return new CloudState(
          ReadStatus.FOUND,
          jobId,
          revision(application),
          current == null ? null : text(current, "Id"),
          current == null ? null : text(current, "ApplicationId"),
          current == null ? null : runState(current),
          current == null ? 0 : displayStartTime(current),
          0,
          runs.stream()
              .filter(run -> TERMINAL.contains(runState(run)))
              .collect(Collectors.toMap(run -> text(run, "Id"), this::runState)));
    } catch (ReadFailure failure) {
      return new CloudState(
          failure.status, jobId, null, null, null, null, 0, failure.retryNotBefore);
    } catch (IOException | RuntimeException e) {
      return readFailure(jobId, ReadStatus.STALE);
    }
  }

  @Override
  public Map<String, Object> diagnostics(
      Context context, String jobId, String instanceId, String source) {
    return diagnostics(context, jobId, instanceId, source, null, null);
  }

  @Override
  public Map<String, Object> diagnostics(
      Context context, String jobId, String instanceId, String source, Long from, Long until) {
    if (!"metrics".equals(source) || !binding.permitsMetricReads(context)) {
      return ManagedFlinkProvider.super.diagnostics(context, jobId, instanceId, source);
    }
    try {
      VolcengineOpenApi api = verifiedClient(context);
      Target target = context.getTarget();
      JsonObject published = api == null ? null : application(api, target, jobId);
      JsonObject run = published == null ? null : instance(api, target, jobId, instanceId);
      return run == null
          ? VolcengineMonitor.status("STALE")
          : VolcengineMonitor.read(api, run, from, until);
    } catch (ResponseStatusException e) {
      throw e;
    } catch (IOException | RuntimeException e) {
      return VolcengineMonitor.status("STALE");
    }
  }

  @Override
  public Map<String, Object> cloudInstance(Context context, String job, String id) {
    try {
      VolcengineOpenApi api = verifiedClient(context);
      Target target = context.getTarget();
      if (api == null || application(api, target, job) == null) {
        return Collections.singletonMap("status", "STALE");
      }
      JsonObject run = instance(api, target, job, id);
      if (run == null) {
        return Collections.singletonMap("status", "STALE");
      }
      Map<String, Object> result = new LinkedHashMap<>();
      result.put("status", "AVAILABLE");
      result.put("id", id);
      result.put("state", text(run, "State"));
      result.put("engine", text(run, "EngineVersion"));
      result.put("runtimeId", text(run, "ApplicationId"));
      result.put("dashboard", dashboardUrl(text(run, "CompleteRestUrl")));
      return result;
    } catch (ReadFailure e) {
      return Collections.singletonMap("status", e.status.name());
    } catch (IOException | RuntimeException e) {
      return Collections.singletonMap("status", "STALE");
    }
  }

  static String dashboardUrl(String value) {
    com.squareup.okhttp.HttpUrl url =
        value == null ? null : com.squareup.okhttp.HttpUrl.parse(value);
    return url != null
            && url.isHttps()
            && url.fragment() == null
            && url.host().endsWith(".openstudio.ivolces.com")
            && url.username().isEmpty()
            && url.password().isEmpty()
            && url.query() == null
        ? url.toString()
        : null;
  }

  @Override
  public Map<String, Object> cloudJobs(Context context, int page, String name) {
    final int size = 10;
    try {
      VolcengineOpenApi api = client(context);
      if (api == null) {
        return Collections.singletonMap("status", "NEEDS_CONFIGURATION");
      }
      Reply identity = api.identity();
      if (!identity.ok() || !same(identity.data(), "AccountId", context.getAccountRef())) {
        return Collections.singletonMap("status", "AUTH_ERROR");
      }
      Target target = context.getTarget();
      Reply reply = listJobs(api, target, name, page, size);
      reply.checkReadBackoff();
      JsonObject data = reply.data();
      long total = safeNumber(text(data, "Total"));
      if (!reply.ok()
          || total < 0
          || safeNumber(text(data, "Current")) != page
          || safeNumber(text(data, "Size")) != size
          || !data.has("Records")
          || !data.get("Records").isJsonArray()) {
        return Collections.singletonMap("status", "STALE");
      }
      List<JsonObject> candidates = rows(data.get("Records"));
      long expected = Math.min(size, Math.max(0, total - (page - 1L) * size));
      if (candidates.size() != expected || data.getAsJsonArray("Records").size() != expected) {
        return Collections.singletonMap("status", "STALE");
      }
      List<JsonObject> matches = new ArrayList<>();
      for (JsonObject row : candidates) {
        // ListGWSApplication filters by pool, but only the draft read verifies its directory.
        if (same(row, "ProjectId", target.getProjectId())
            && same(object(row, "DeployRequest"), "Queue", target.getResourcePoolId())
            && draft(api, target, text(row, "AppDraftId")) != null) {
          matches.add(row);
        }
      }
      Map<String, Object> result = new LinkedHashMap<>();
      result.put("status", "AVAILABLE");
      result.put("jobs", publicFields(matches, "Id", "JobName", "JobType", "State"));
      result.put("page", page);
      result.put("hasNext", (long) page * size < total);
      return result;
    } catch (ReadFailure e) {
      return Collections.singletonMap("status", e.status.name());
    } catch (IOException | RuntimeException e) {
      return Collections.singletonMap("status", "STALE");
    }
  }

  @Override
  public Map<String, Object> cloudJob(Context context, String job, int page) {
    if (job == null) {
      return Collections.singletonMap("status", "NOT_PUBLISHED");
    }
    try {
      VolcengineOpenApi api = verifiedClient(context);
      Target target = context.getTarget();
      JsonObject published = api == null ? null : application(api, target, job);
      if (published == null) {
        return Collections.singletonMap("status", "STALE");
      }
      Reply reply = instances(api, target, job, page);
      long total = safeNumber(text(reply.data(), "Total"));
      List<JsonObject> runs = instancePage(reply, target, job, page, total);
      if (total < 0 || runs == null) {
        return Collections.singletonMap("status", "STALE");
      }
      Map<String, Object> view = new LinkedHashMap<>();
      view.put("status", "AVAILABLE");
      view.put("jobId", job);
      view.put("jobType", text(published, "JobType"));
      view.put("startMethods", startMethods(api, job));
      view.put("instances", publicFields(runs, "Id", "State", "StartTime"));
      view.put("page", page);
      view.put("hasNext", (long) page * INSTANCE_PAGE_SIZE < total);
      view.put(
          "savepoints", publicFields(savepoints(api, job), "SavepointId", "Status", "CreateTime"));
      return view;
    } catch (ReadFailure e) {
      return Collections.singletonMap("status", e.status.name());
    } catch (IOException | RuntimeException e) {
      return Collections.singletonMap("status", "STALE");
    }
  }

  private List<Map<String, String>> publicFields(List<JsonObject> records, String... fields) {
    return records.stream()
        .map(
            record -> {
              Map<String, String> result = new LinkedHashMap<>();
              for (String field : fields) {
                result.put(field, text(record, field));
              }
              return result;
            })
        .collect(Collectors.toList());
  }

  @Override
  public Map<String, Object> logs(
      Context context, String jobId, String instanceId, LogQuery query) {
    String capability = logCapability(context);
    if (!"AVAILABLE".equals(capability)) {
      return VolcengineTlsLogs.status(capability);
    }
    try {
      VolcengineOpenApi api = verifiedClient(context);
      if (api == null) {
        return VolcengineTlsLogs.status("AUTH_ERROR");
      }
      Target target = context.getTarget();
      JsonObject published = application(api, target, jobId);
      JsonObject run = published == null ? null : instance(api, target, jobId, instanceId);
      return run == null
          ? VolcengineTlsLogs.status("STALE")
          : tlsLogs.read(
              new VolcengineTlsApi(target.getTlsRegion(), api.credentials()),
              context,
              jobId,
              run,
              query);
    } catch (IOException | DateTimeParseException e) {
      return VolcengineTlsLogs.status("STALE");
    }
  }

  @Override
  public Observation dispatch(Context context, OperationStep step) {
    return dispatch(context, step, () -> true);
  }

  @Override
  public Observation dispatch(Context context, OperationStep step, BooleanSupplier permit) {
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
        case STAGE_DEPENDENCY:
        case STAGE_ARTIFACT:
          return binding.permitsArtifacts(context)
              ? artifacts.stage(context, step, api.credentials(), permit)
              : rejected("CAPABILITY_UNVERIFIED");
        case CREATE_DRAFT:
          if (!supportedSpec(context, step)) {
            return rejected("UNVERIFIED_SPEC");
          }
          JsonObject create = draftFields(step, target);
          return write(
              permit,
              () ->
                  ack(
                      api.call("CreateGWSApplicationDraft", FLINK_VERSION, scope(target), create),
                      "Id",
                      null));
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
          return write(
              permit,
              () ->
                  ack(
                      api.call("UpdateGWSApplicationDraft", FLINK_VERSION, scope(target), update),
                      "Id",
                      null));
        case PUBLISH:
          if (!supportedSpec(context, step) || !matchesDraftForPublish(api, target, step)) {
            return rejected("DRAFT_NOT_VERIFIED");
          }
          if (step.getCommand().getSpec().getType() == JobType.SQL) {
            Observation syntaxFailure = sqlSyntaxFailure(api, target, step);
            if (syntaxFailure != null) {
              return syntaxFailure;
            }
            if (!matchesDraftForPublish(api, target, step)) {
              return rejected("DRAFT_NOT_VERIFIED");
            }
          }
          Map<String, String> query = scope(target);
          query.put("Id", step.getDraftId());
          JsonObject deployment = deployment(target, step.getCommand().getSpec());
          deployment.addProperty("DraftId", step.getDraftId());
          deployment.addProperty("ProjectId", target.getProjectId());
          return write(
              permit,
              () ->
                  ack(
                      api.call("DeployGWSApplicationDraft", FLINK_VERSION, query, deployment),
                      null,
                      "Id"));
        case SUBMIT:
          if (!supportedSpec(context, step) || !canSubmitInstance(context, step)) {
            return rejected("UNVERIFIED_SPEC");
          }
          if (step.getProviderJobId() != null
              && (!Objects.equals(step.getProviderJobId(), target.getDirectJobId())
                  || step.getCommand().getProviderVersion() == null
                  || !step.getCommand()
                      .getProviderVersion()
                      .equals(revision(application(api, target, step.getProviderJobId()))))) {
            return rejected("DIRECT_JOB_NOT_VERIFIED");
          }
          JsonObject direct =
              instanceRequest(
                  context,
                  step,
                  deployment(target, step.getCommand().getSpec()),
                  fields("Type", "FROM_NEW"));
          return write(permit, () -> submitInstance(api, direct));
        case RESTART:
        case START:
          return start(api, context, step, permit);
        case SNAPSHOT:
          if (!binding.permitsSnapshots(context) || !isCurrentRunning(api, target, step)) {
            return rejected("SNAPSHOT_INSTANCE_NOT_VERIFIED");
          }
          JsonObject snapshot =
              fields("Id", step.getProviderJobId(), "Description", snapshotName(step));
          return write(
              permit,
              () ->
                  ack(
                      api.call(
                          "CreateGWSSavepoint", FLINK_VERSION, Collections.emptyMap(), snapshot),
                      null,
                      null));
        case STOP:
          JsonObject running =
              instance(
                  api, target, step.getProviderJobId(), step.getCommand().getProviderInstance());
          JsonObject deployed = application(api, target, step.getProviderJobId());
          if (running == null || !same(running, "State", "RUNNING") || deployed == null) {
            return rejected("INSTANCE_NOT_VERIFIED");
          }
          if (step.getCommand().getActions().contains(ManagedFlinkModels.Action.START)) {
            Observation rejection = checkStart(api, context, step, deployed);
            if (rejection != null) {
              return rejection;
            }
          }
          JsonObject cancel = fields("InstanceId", step.getCommand().getProviderInstance());
          return write(
              permit,
              () ->
                  ack(
                      api.call(
                          "CancelApplicationInstance",
                          FLINK_VERSION,
                          Collections.emptyMap(),
                          cancel),
                      null,
                      null));
        default:
          return rejected("CAPABILITY_UNVERIFIED");
      }
    } catch (IOException | RuntimeException e) {
      return rejected("VOLCENGINE_PREFLIGHT_UNAVAILABLE");
    }
  }

  private Observation write(BooleanSupplier permit, CloudWrite command) {
    if (!permit.getAsBoolean()) {
      return rejected("DISPATCH_NOT_AUTHORIZED");
    }
    try {
      return command.send();
    } catch (IOException | RuntimeException e) {
      return Observation.outcome(Outcome.UNKNOWN, "RESULT_UNCERTAIN");
    }
  }

  @FunctionalInterface
  private interface CloudWrite {
    Observation send() throws IOException;
  }

  private boolean matchesDraftForPublish(VolcengineOpenApi api, Target target, OperationStep step) {
    try {
      return VolcengineExecutable.matches(draft(api, target, step.getDraftId()), step, true);
    } catch (IOException | RuntimeException e) {
      // Publish has not been dispatched when the draft cannot be read or matched.
      return false;
    }
  }

  private Reply validateSql(
      VolcengineOpenApi api, Target target, String draftId, ManagedFlinkModels.Spec spec)
      throws IOException {
    return api.call(
        "ValidateGWSApplicationDraft",
        GWS_VERSION,
        scope(target),
        fields(
            "Id",
            draftId,
            "JobType",
            VolcengineExecutable.providerType(spec),
            "SqlText",
            spec.getSql()));
  }

  @Override
  public String validateCloudSql(Context context, String draftId, ManagedFlinkModels.Spec spec) {
    try {
      VolcengineOpenApi api = client(context);
      if (api == null) {
        return "UNAVAILABLE";
      }
      Reply identity = api.identity();
      if (!identity.ok() || !same(identity.data(), "AccountId", context.getAccountRef())) {
        return "UNAVAILABLE";
      }
      JsonObject record = draft(api, context.getTarget(), draftId);
      if (!same(record, "JobType", VolcengineExecutable.providerType(spec))
          || !same(record, "EngineVersion", context.getTarget().getEngineRef())) {
        return "UNAVAILABLE";
      }
      return sqlCheckStatus(validateSql(api, context.getTarget(), draftId, spec));
    } catch (IOException | RuntimeException e) {
      return "UNAVAILABLE";
    }
  }

  static String sqlCheckStatus(Reply reply) {
    JsonElement valid = reply.data().get("Data");
    if (!reply.ok()
        || valid == null
        || !valid.isJsonPrimitive()
        || !valid.getAsJsonPrimitive().isBoolean()) {
      return "UNAVAILABLE";
    }
    if (valid.getAsBoolean()) {
      return "VALID";
    }
    if (rejectsSqlSyntax(reply)) {
      return "SYNTAX_ERROR";
    }
    return "SQL containing user-defined functions must be validated with deep-validate."
            .equals(text(reply.data(), "Message"))
        ? "DEEP_CHECK_REQUIRED"
        : "CHECK_FAILED";
  }

  private Observation sqlSyntaxFailure(VolcengineOpenApi api, Target target, OperationStep step) {
    try {
      Reply validation = validateSql(api, target, step.getDraftId(), step.getCommand().getSpec());
      if (rejectsSqlSyntax(validation)) {
        Observation failure = rejected("SQL_SYNTAX_REJECTED");
        failure.setRequestId(validation.requestId);
        return failure;
      }
    } catch (IOException | RuntimeException ignored) {
      // When validation is unavailable, publish and startup keep their own provider checks.
    }
    return null;
  }

  static boolean rejectsSqlSyntax(Reply validation) {
    if (!validation.ok()) {
      return false;
    }
    JsonObject data = validation.data();
    JsonElement valid = data.get("Data");
    String message = text(data, "Message");
    // VERIFY_FAILED also covers UDF SQL that needs deep validation and can run correctly.
    return valid != null
        && valid.isJsonPrimitive()
        && valid.getAsJsonPrimitive().isBoolean()
        && !valid.getAsBoolean()
        && "VERIFY_FAILED".equals(text(data, "Type"))
        && message != null
        && message
            .trim()
            .startsWith("com.bytedance.openplatform.gts.common.model.exception.SqlParseException:");
  }

  @Override
  public Observation observe(Context context, OperationStep step, Observation previous) {
    String reportedJob = previous == null ? null : previous.getJobId();
    boolean restart = step.getAction() == ManagedFlinkModels.Action.RESTART;
    if (restart
        && (step.getProviderJobId() == null
            || (reportedJob == null && step.getDispatchedAt() <= 0)
            || (reportedJob != null && !reportedJob.equals(step.getProviderJobId())))) {
      return awaiting();
    }
    try {
      VolcengineOpenApi api = verifiedClient(context);
      if (api == null) {
        return awaiting();
      }
      Target target = context.getTarget();
      Observation observation = Observation.outcome(Outcome.CONFIRMED, null);
      switch (step.getAction()) {
        case STAGE_DEPENDENCY:
        case STAGE_ARTIFACT:
          return binding.permitsArtifacts(context)
              ? artifacts.stage(context, step, api.credentials(), null)
              : awaiting();
        case CREATE_DRAFT:
          String draftId = previous == null ? null : previous.getDraftId();
          JsonObject matchedDraft = draft(api, target, draftId);
          if (matchedDraft == null
              || !same(matchedDraft, "JobName", step.getCommand().getJobName())
              || !same(
                  matchedDraft,
                  "JobType",
                  VolcengineExecutable.providerType(step.getCommand().getSpec()))
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
        case SUBMIT:
        case START:
        case RESTART:
          String acknowledgedInstance = previous == null ? null : previous.getInstanceId();
          String submittedJob = step.getProviderJobId();
          if (submittedJob != null && reportedJob != null && !submittedJob.equals(reportedJob)) {
            return awaiting();
          }
          String expectedVersion = step.getCommand().getProviderVersion();
          if (step.getAction() == ManagedFlinkModels.Action.SUBMIT) {
            if (submittedJob == null) {
              submittedJob = reportedJob == null ? findDirectJob(api, target, step) : reportedJob;
            }
            if (submittedJob == null) {
              return awaiting();
            }
            JsonObject direct =
                application(api, target, submittedJob, step.getCommand().getReleaseMarker());
            if (!VolcengineExecutable.matches(direct, step, false)
                || !VolcengineExecutable.matchesScheduling(direct, step.getCommand().getSpec())) {
              return awaiting();
            }
            expectedVersion = revision(direct);
            observation.setSpecHash(step.getCommand().getSpecHash());
          }
          if (restart || acknowledgedInstance == null) {
            String prior =
                restart
                    ? step.getCommand().getProviderInstance()
                    : step.getCommand().getInstanceBeforeDispatch();
            acknowledgedInstance =
                successor(
                    api,
                    target,
                    submittedJob,
                    prior,
                    step.getDispatchedAt(),
                    restart,
                    restart && reportedJob != null,
                    step.getAction() == ManagedFlinkModels.Action.SUBMIT
                        ? step.getCommand().getReleaseMarker()
                        : null);
          }
          if (acknowledgedInstance == null) {
            return awaiting();
          }
          JsonObject instance = instance(api, target, submittedJob, acknowledgedInstance);
          if (instance == null
              || !VolcengineExecutable.matches(instance, step, false)
              || expectedVersion == null
              || !expectedVersion.equals(revision(instance))
              || !("RUNNING".equals(runState(instance)) || TERMINAL.contains(runState(instance)))
              || (step.getAction() != ManagedFlinkModels.Action.SUBMIT
                  && target.getDirectJobId() == null
                  && text(instance, "VersionId") == null)
              || !matchesRestore(api, step, instance)) {
            return awaiting();
          }
          observation.setJobId(submittedJob);
          observation.setInstanceId(acknowledgedInstance);
          observation.setRuntimeId(text(instance, "ApplicationId"));
          observation.setInstanceStartedAt(displayStartTime(instance));
          observation.setVersion(expectedVersion);
          observation.setRemoteVersionId(text(instance, "VersionId"));
          observation.setRunState(runState(instance));
          observation.setRestoredFrom(step.getCommand().getRestoreRef());
          return observation;
        case SNAPSHOT:
          List<JsonObject> points = savepoints(api, step.getProviderJobId());
          List<JsonObject> matches =
              points.stream()
                  .filter(point -> same(point, "Description", snapshotName(step)))
                  .collect(Collectors.toList());
          if (matches.size() == 1
              && same(matches.get(0), "Status", "FAIL")
              && application(api, target, step.getProviderJobId()) != null) {
            return rejected("CLOUD_SAVEPOINT_FAILED");
          }
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
          if (completed == null || !TERMINAL.contains(runState(completed))) {
            return awaiting();
          }
          observation.setJobId(step.getProviderJobId());
          observation.setInstanceId(stopped);
          observation.setRunState(runState(completed));
          return observation;
        default:
          return awaiting();
      }
    } catch (ReadFailure failure) {
      Observation result =
          Observation.outcome(Outcome.WAITING, "VOLCENGINE_READ_" + failure.status);
      long fallback = System.currentTimeMillis() + 15_000;
      result.setRetryNotBefore(Math.max(fallback, failure.retryNotBefore));
      return result;
    } catch (IOException | RuntimeException e) {
      return awaiting();
    }
  }

  private List<String> startMethods(VolcengineOpenApi api, String job) throws IOException {
    Reply reply =
        api.call("GetGWSStartMethod", FLINK_VERSION, Collections.emptyMap(), fields("Id", job));
    reply.checkReadBackoff();
    if (!reply.ok()) {
      throw new IOException("Unable to read available Flink start methods");
    }
    GetGWSStartMethodResponse methods =
        new Gson().fromJson(reply.data(), GetGWSStartMethodResponse.class);
    return methods.getEnableStartMethods() == null
        ? Collections.emptyList()
        : methods.getEnableStartMethods();
  }

  private Observation checkStart(
      VolcengineOpenApi api, Context context, OperationStep step, JsonObject published)
      throws IOException {
    if (!supportedSpec(context, step)) {
      return rejected("UNVERIFIED_SPEC");
    }
    for (DependencyArtifact dep : step.getCommand().getDependencies()) {
      if (!binding.permitsArtifacts(context)) {
        return rejected("CAPABILITY_UNVERIFIED");
      }
      Observation proof = artifacts.stage(context, step, dep, api.credentials(), null);
      if (proof.getOutcome() != Outcome.CONFIRMED
          || !Objects.equals(dep.getRemoteRef(), proof.getArtifactRef())) {
        return rejected("RELEASE_ARTIFACT_NOT_VERIFIED");
      }
    }
    if (VolcengineExecutable.artifact(step) != null) {
      if (!binding.permitsArtifacts(context)) {
        return rejected("CAPABILITY_UNVERIFIED");
      }
      Observation verified = artifacts.stage(context, step, api.credentials(), null);
      if (verified.getOutcome() != Outcome.CONFIRMED
          || !Objects.equals(verified.getArtifactRef(), step.getCommand().getArtifactRef())) {
        Observation failure = rejected("RELEASE_ARTIFACT_NOT_VERIFIED");
        failure.setRequestId(verified.getRequestId());
        return failure;
      }
    }
    if (!VolcengineExecutable.matchesScheduling(published, step.getCommand().getSpec())
        || !VolcengineExecutable.matches(published, step, false)
        || !Objects.equals(revision(published), step.getCommand().getProviderVersion())) {
      return rejected("PUBLISHED_REVISION_MISMATCH");
    }
    if ((step.getCommand().getRequest().isSubmitInstance()
            || context.getTarget().getDirectJobId() != null)
        && !canSubmitInstance(context, step)) {
      return rejected("UNSUPPORTED_INSTANCE_ENGINE");
    }
    RestoreMode mode = step.getCommand().getRequest().getRestore().getMode();
    if (mode == RestoreMode.FROM_NEW) {
      return null;
    }
    if (!binding.permitsSnapshots(context)) {
      return rejected("UNVERIFIED_START_CONTRACT");
    }
    if (mode == RestoreMode.FROM_CLOUD_LATEST) {
      return startMethods(api, step.getProviderJobId()).contains("FROM_LATEST")
          ? null
          : rejected("LATEST_STATE_UNAVAILABLE");
    }
    return savepointPath(api, step.getProviderJobId(), step.getCommand().getRestoreRef()) == null
        ? rejected("RESTORE_SNAPSHOT_NOT_VERIFIED")
        : null;
  }

  private Observation start(
      VolcengineOpenApi api, Context context, OperationStep step, BooleanSupplier permit)
      throws IOException {
    Target target = context.getTarget();
    JsonObject published = application(api, target, step.getProviderJobId());
    Observation rejection = checkStart(api, context, step, published);
    if (rejection != null) {
      return rejection;
    }
    RestoreMode mode = step.getCommand().getRequest().getRestore().getMode();
    JsonObject parameters = fields("Id", step.getProviderJobId());
    if (mode == RestoreMode.FROM_NEW || mode == RestoreMode.FROM_CLOUD_LATEST) {
      parameters.addProperty("Type", mode == RestoreMode.FROM_NEW ? "FROM_NEW" : "FROM_LATEST");
    } else {
      parameters.addProperty("Type", "FROM_SAVEPOINT");
      parameters.addProperty("SavepointId", step.getCommand().getRestoreRef());
    }
    if (step.getAction() == ManagedFlinkModels.Action.RESTART) {
      if (!isCurrentRunning(api, target, step)) {
        return rejected("RESTART_INSTANCE_NOT_VERIFIED");
      }
      return write(
          permit,
          () ->
              ack(
                  api.call(
                      "RestartGWSApplication", FLINK_VERSION, Collections.emptyMap(), parameters),
                  null,
                  "Id"));
    }
    boolean submit =
        step.getCommand().getRequest().isSubmitInstance() || target.getDirectJobId() != null;
    final JsonObject payload =
        submit
            ? instanceRequest(context, step, object(published, "DeployRequest"), parameters)
            : parameters;
    return write(
        permit,
        () -> {
          if (submit) {
            return submitInstance(api, payload);
          }
          Reply result = api.call("StartGWSApplication", GWS_VERSION, scope(target), payload);
          Observation acknowledgement = ack(result, null, null);
          acknowledgement.setInstanceId(text(result.data(), "InstanceId"));
          return acknowledgement;
        });
  }

  private String findDirectJob(VolcengineOpenApi api, Target target, OperationStep step)
      throws IOException {
    Reply reply = listJobs(api, target, step.getCommand().getJobName());
    reply.checkReadBackoff();
    List<JsonObject> records = rows(reply.data().get("Records"));
    if (!reply.ok() || safeNumber(text(reply.data(), "Total")) != records.size()) {
      return null;
    }
    String found = null;
    for (JsonObject row : records) {
      String id = text(row, "Id");
      if (same(row, "JobName", step.getCommand().getJobName())
          && application(api, target, id, step.getCommand().getReleaseMarker()) != null) {
        if (found != null) {
          return null;
        }
        found = id;
      }
    }
    return found;
  }

  private String successor(
      VolcengineOpenApi api,
      Target target,
      String job,
      String prior,
      long dispatchedAt,
      boolean restart,
      boolean acknowledgedRestart,
      String directRelease)
      throws IOException {
    if (job == null || (prior == null && (restart || dispatchedAt <= 0))) {
      return null;
    }
    boolean hasPrior = StringUtils.isNotEmpty(prior);
    JsonObject old = hasPrior ? instance(api, target, job, prior) : null;
    if (hasPrior
        && (old == null
            || !TERMINAL.contains(runState(old))
            || (restart && !same(old, "State", "STOPPED")))) {
      return null;
    }
    JsonObject published =
        directRelease == null
            ? application(api, target, job)
            : application(api, target, job, directRelease);
    Reply reply = instances(api, target, job, 1);
    long total = safeNumber(text(reply.data(), "Total"));
    List<JsonObject> recent = instancePage(reply, target, job, 1, total);
    if (published == null || recent == null || recent.isEmpty()) {
      return null;
    }
    // The response is ordered by descending start time. Never infer uniqueness across a
    // truncated page whose oldest entry could still be a successor.
    // Steps dispatched by older Consoles only have a local dispatch timestamp. Require the
    // candidate to start strictly later, since cloud timestamps have second precision.
    long cutoff = prior == null ? dispatchedAt + 1 : Long.MIN_VALUE;
    final long earliest = old == null ? cutoff : startedAt(old);
    if (total > recent.size() && startedAt(recent.get(recent.size() - 1)) >= earliest) {
      return null;
    }
    List<JsonObject> successors =
        recent.stream()
            .filter(
                run ->
                    !same(run, "Id", prior)
                        && (earliest == Long.MIN_VALUE || startedAt(run) >= earliest))
            .collect(Collectors.toList());
    if (successors.size() != 1) {
      return null;
    }
    JsonObject next = successors.get(0);
    // A tie with the baseline could be another instance already present before dispatch.
    if (!acknowledgedRestart && old != null && startedAt(next) == earliest) {
      return null;
    }
    String runtime = text(published, "ApplicationId");
    if ((restart || runtime != null) && !same(next, "ApplicationId", runtime)) {
      return null;
    }
    return text(next, "Id");
  }

  private Observation submitInstance(VolcengineOpenApi api, JsonObject payload) throws IOException {
    Reply result =
        api.call("StartApplicationInstance", FLINK_VERSION, Collections.emptyMap(), payload);
    Observation acknowledgement = ack(result, null, "Id");
    acknowledgement.setInstanceId(text(result.data(), "InstanceId"));
    return acknowledgement;
  }

  private JsonObject deployment(Target target, ManagedFlinkModels.Spec spec) {
    JsonObject deployment =
        fields(
            "Queue",
            target.getResourcePoolId(),
            "ResourcePool",
            target.getResourcePoolName(),
            "Priority",
            Objects.toString(spec.getPriority(), "1"),
            "SchedulePolicy",
            VolcengineExecutable.schedulePolicy(spec));
    if ("GANG".equals(VolcengineExecutable.schedulePolicy(spec))) {
      deployment.addProperty(
          "ScheduleTimeout", spec.getScheduleTimeout() == null ? 60 : spec.getScheduleTimeout());
    }
    return deployment;
  }

  private boolean canSubmitInstance(Context context, OperationStep step) {
    String engine = step.getCommand().getEngineRef();
    return ENGINES.contains(engine) && Objects.equals(engine, context.getTarget().getEngineRef());
  }

  private JsonObject instanceRequest(
      Context context, OperationStep step, JsonObject deployment, JsonObject restore) {
    ManagedFlinkModels.Spec spec = step.getCommand().getSpec();
    Gson gson = new Gson();
    AppForStartApplicationInstanceInput app =
        new AppForStartApplicationInstanceInput()
            .projectId(context.getTarget().getProjectId())
            .accountId(context.getAccountRef())
            .jobName(step.getCommand().getJobName())
            .jobType(
                AppForStartApplicationInstanceInput.JobTypeEnum.fromValue(
                    VolcengineExecutable.providerType(spec)))
            .deployRequest(
                gson.fromJson(deployment, DeployRequestForStartApplicationInstanceInput.class))
            .conf(VolcengineExecutable.settings(step).toString());
    if (step.getAction() == ManagedFlinkModels.Action.SUBMIT && step.getProviderJobId() == null) {
      // Without UniqueKey the service can start an instance without creating a managed job.
      app.uniqueKey(step.getCommand().getReleaseMarker());
    }
    if (spec.getType() == JobType.SQL) {
      app.sqlText(spec.getSql());
    } else {
      app.jar(VolcengineExecutable.artifact(step))
          .mainClass(spec.getMainClass())
          .args(VolcengineExecutable.jarArgs(spec));
    }
    List<String> dependencies = VolcengineExecutable.dependencies(step);
    if (!dependencies.isEmpty()) {
      app.dependency(new DependencyForStartApplicationInstanceInput().jars(dependencies));
    }
    StartApplicationInstanceRequest request =
        new StartApplicationInstanceRequest()
            .id(step.getProviderJobId())
            .app(app)
            .restoreStrategy(
                new RestoreStrategyForStartApplicationInstanceInput()
                    .type(text(restore, "Type"))
                    .savepointId(text(restore, "SavepointId")));
    JsonObject payload = gson.toJsonTree(request).getAsJsonObject();
    // The SDK enum predates newer engines available through the same API.
    payload.getAsJsonObject("App").addProperty("EngineVersion", step.getCommand().getEngineRef());
    return payload;
  }

  private boolean isCurrentRunning(VolcengineOpenApi api, Target target, OperationStep step)
      throws IOException {
    String instanceId = step.getCommand().getProviderInstance();
    JsonObject job = application(api, target, step.getProviderJobId());
    JsonObject current =
        instanceId == null ? null : instance(api, target, step.getProviderJobId(), instanceId);
    return job != null
        && current != null
        && same(job, "ApplicationId", text(current, "ApplicationId"))
        && same(current, "State", "RUNNING");
  }

  private String snapshotName(OperationStep step) {
    return "streampark-operation-" + step.getOperationId();
  }

  private List<JsonObject> savepoints(VolcengineOpenApi api, String job) throws IOException {
    Reply response =
        api.call("ListGWSSavepoint", FLINK_VERSION, Collections.emptyMap(), fields("Id", job));
    response.checkReadBackoff();
    if (!response.ok()
        || !response.data().has("SavepointInfos")
        || !response.data().get("SavepointInfos").isJsonArray()) {
      throw new IOException("Invalid Volcengine savepoint list");
    }
    return rows(response.data().get("SavepointInfos"));
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
    if (step.getCommand().getRequest().getRestore().getMode() == RestoreMode.FROM_CLOUD_LATEST) {
      return actual != null && !actual.isEmpty();
    }
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
      listing.checkReadBackoff();
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
    if (!VolcengineExecutable.matchesScheduling(published, step.getCommand().getSpec())
        || !VolcengineExecutable.matches(published, step, false)
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

  private VolcengineOpenApi verifiedClient(Context context) throws IOException {
    VolcengineOpenApi api = client(context);
    if (api != null) {
      Reply identity = api.identity();
      identity.checkReadBackoff();
      if (identity.ok() && same(identity.data(), "AccountId", context.getAccountRef())) {
        return api;
      }
    }
    return null;
  }

  VolcengineOpenApi client(Context context) throws IOException {
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
    reply.checkReadBackoff();
    JsonObject record = reply.data();
    return reply.ok()
            && scoped(record, target, id)
            && same(record, "DirectoryId", target.getDirectoryId())
        ? record
        : null;
  }

  private JsonObject application(VolcengineOpenApi api, Target target, String id)
      throws IOException {
    return application(
        api,
        target,
        id,
        id != null && id.equals(target.getDirectJobId()) ? target.getDirectReleaseId() : null);
  }

  private JsonObject application(
      VolcengineOpenApi api, Target target, String id, String directRelease) throws IOException {
    if (id == null) {
      return null;
    }
    Reply reply =
        api.call(
            "GetGWSApplication",
            GWS_VERSION,
            Collections.emptyMap(),
            fields("Id", id, "ProjectId", target.getProjectId()));
    reply.checkReadBackoff();
    JsonObject application = reply.data();
    if (!reply.ok()
        || !scoped(application, target, id)
        || !same(object(application, "DeployRequest"), "Queue", target.getResourcePoolId())) {
      return null;
    }
    String draftId = text(application, "AppDraftId");
    if (directRelease != null) {
      JsonObject conf = VolcengineExecutable.objectValue(application.get("Conf"));
      return draftId == null && same(conf, "pipeline.name", "streampark-release-" + directRelease)
          ? application
          : null;
    }
    return draft(api, target, draftId) == null ? null : application;
  }

  private JsonObject instance(VolcengineOpenApi api, Target target, String job, String id)
      throws IOException {
    Reply reply =
        api.call(
            "GetApplicationInstance",
            FLINK_VERSION,
            Collections.emptyMap(),
            fields("InstanceId", id));
    reply.checkReadBackoff();
    return reply.ok() && same(reply.data(), "Id", id) && isInstance(reply.data(), target, job)
        ? reply.data()
        : null;
  }

  private List<JsonObject> instanceHistory(VolcengineOpenApi api, Target target, String job)
      throws IOException {
    long deadline = System.nanoTime() + INSTANCE_SCAN_BUDGET;
    Reply firstReply = instances(api, target, job, 1);
    long total = safeNumber(text(firstReply.data(), "Total"));
    if (total < 0) {
      return null;
    }
    List<JsonObject> first = instancePage(firstReply, target, job, 1, total);
    if (System.nanoTime() - deadline >= 0) {
      return null;
    }
    if (first == null || total <= INSTANCE_PAGE_SIZE) {
      return first;
    }
    List<JsonObject> history = new ArrayList<>(first);
    Set<String> ids = first.stream().map(row -> text(row, "Id")).collect(Collectors.toSet());
    for (int page = 2; history.size() < total; page++) {
      if (System.nanoTime() - deadline >= 0) {
        return null;
      }
      List<JsonObject> next =
          instancePage(instances(api, target, job, page), target, job, page, total);
      if (next == null || next.stream().anyMatch(row -> !ids.add(text(row, "Id")))) {
        return null;
      }
      history.addAll(next);
    }

    // Offset pagination has no snapshot token. Reject a new or changed head during the scan.
    if (System.nanoTime() - deadline >= 0) {
      return null;
    }
    List<JsonObject> head = instancePage(instances(api, target, job, 1), target, job, 1, total);
    return System.nanoTime() - deadline < 0
            && head != null
            && readbackKeys(first).equals(readbackKeys(head))
        ? history
        : null;
  }

  private List<JsonObject> instancePage(
      Reply reply, Target target, String job, int page, long total) {
    JsonObject data = reply.data();
    JsonElement records = data.get("Records");
    long expected = Math.min(INSTANCE_PAGE_SIZE, total - (page - 1L) * INSTANCE_PAGE_SIZE);
    if (!reply.ok()
        || safeNumber(text(data, "Current")) != page
        || safeNumber(text(data, "Size")) != INSTANCE_PAGE_SIZE
        || safeNumber(text(data, "Total")) != total
        || records == null
        || !records.isJsonArray()
        || records.getAsJsonArray().size() != expected) {
      return null;
    }
    List<JsonObject> result = rows(records);
    Set<String> ids = new HashSet<>();
    return result.size() == expected
            && result.stream()
                .allMatch(
                    row ->
                        isInstance(row, target, job)
                            && StringUtils.isNotBlank(text(row, "Id"))
                            && ids.add(text(row, "Id")))
        ? result
        : null;
  }

  private List<List<String>> readbackKeys(List<JsonObject> instances) {
    return instances.stream()
        .map(
            row ->
                Arrays.asList(
                    text(row, "Id"),
                    text(row, "State"),
                    text(row, "StartTime"),
                    text(row, "ApplicationId")))
        .collect(Collectors.toList());
  }

  private Reply instances(VolcengineOpenApi api, Target target, String job, int page)
      throws IOException {
    Reply reply =
        api.call(
            "ListApplicationInstance",
            FLINK_VERSION,
            Collections.emptyMap(),
            fields(
                "ProjectId",
                target.getProjectId(),
                "JobId",
                Long.parseLong(job),
                "PageNum",
                Integer.toString(page),
                "PageSize",
                Integer.toString(INSTANCE_PAGE_SIZE),
                "SortField",
                "StartTime",
                "SortOrder",
                "desc"));
    reply.checkReadBackoff();
    return reply;
  }

  private Reply listJobs(VolcengineOpenApi api, Target target, String name) throws IOException {
    return listJobs(api, target, name, 1, 50);
  }

  private Reply listJobs(VolcengineOpenApi api, Target target, String name, int page, int size)
      throws IOException {
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
            page,
            "PageSize",
            size));
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
        VolcengineExecutable.providerType(step.getCommand().getSpec()));
  }

  private boolean supportedSpec(Context context, OperationStep step) {
    ManagedFlinkModels.Spec spec = step.getCommand().getSpec();
    if (step.getCommand().getReleaseMarker() == null
        || specIssue(spec, context.getTarget().getEngineRef()) != null) {
      return false;
    }
    if (spec.getType() == JobType.SQL
        && spec.getUdfArtifactId() == null
        && spec.getDependencyIds().isEmpty()) {
      return true;
    }
    if (!binding.permitsArtifacts(context) || !VolcengineExecutable.hasSupportedArgs(spec)) {
      return false;
    }
    String prefix =
        "tos://"
            + context.getTarget().getArtifactBucket()
            + "/"
            + context.getTarget().getArtifactPrefix()
            + "/"
            + context.getTeamId()
            + "/"
            + step.getAppId()
            + "/";
    if (step.getCommand().getDependencies().size() != spec.getDependencyIds().size()
        || step.getCommand().getDependencies().stream()
            .anyMatch(dep -> !(prefix + dep.getDigest() + ".jar").equals(dep.getRemoteRef()))) {
      return false;
    }
    if (spec.getType() == JobType.SQL && spec.getUdfArtifactId() == null) {
      return true;
    }
    String artifact = VolcengineExecutable.artifact(step);
    return artifact != null
        && artifact.startsWith(prefix)
        && artifact.substring(prefix.length()).matches("[a-f0-9]{64}\\.jar");
  }

  private boolean same(JsonObject record, String field, String expected) {
    return expected != null && expected.equals(text(record, field));
  }

  private boolean scoped(JsonObject record, Target target, String id) {
    return same(record, "Id", id) && same(record, "ProjectId", target.getProjectId());
  }

  private String runState(JsonObject instance) {
    String state = text(instance, "State");
    return "SUCCEEDED".equals(state) ? "FINISHED" : state;
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

  private boolean includesDirectory(JsonElement nodes, String directoryId) {
    for (JsonObject directory : rows(nodes)) {
      if (same(directory, "Id", directoryId)
          || includesDirectory(directory.get("ChildDirectoryDtoList"), directoryId)) {
        return true;
      }
    }
    return false;
  }

  private long safeNumber(String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private long displayStartTime(JsonObject run) {
    if (text(run, "StartTime") == null) {
      return 0;
    }
    try {
      return startedAt(run);
    } catch (DateTimeParseException | ArithmeticException ignored) {
      return 0;
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
    // A structured authorization rejection is definitive. A transport or server failure
    // may arrive after the write and still requires readback.
    if ((reply.httpStatus == 401 || reply.httpStatus == 403)
        && StringUtils.isNotBlank(reply.errorCode)
        && StringUtils.isNotBlank(reply.requestId)) {
      Observation denied = rejected("CLOUD_AUTHORIZATION_REJECTED");
      denied.setRequestId(reply.requestId);
      return denied;
    }
    if (!reply.ok() || "false".equals(text(reply.data(), "Success"))) {
      Observation unknown = Observation.outcome(Outcome.UNKNOWN, "VOLCENGINE_WRITE_UNCERTAIN");
      unknown.setRequestId(reply.requestId);
      unknown.setRetryNotBefore(reply.retryNotBefore);
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
