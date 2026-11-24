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

import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Action;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Command;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Context;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.FrozenCommand;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Outcome;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Restore;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.RestoreMode;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Spec;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Target;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Protocol;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.ResponseBody;
import com.volcengine.sign.Credentials;
import okio.Buffer;
import org.junit.jupiter.api.Test;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class VolcengineDispatchTest {
  private static final String ENGINE = "FLINK_VERSION_1_17";
  private static final String SAVEPOINT = "fixture-savepoint";
  private static final String LAST_READ = "ListGWSSavepoint";
  private static final String START = "StartGWSApplication";

  @Test
  void transportFailuresDuringScopedReadsDoNotRequestWritePermission() {
    for (String read :
        Arrays.asList(
            "GetCallerIdentity", "GetGWSApplication", "GetGWSApplicationDraft", LAST_READ)) {
      Fixture fixture = new Fixture(read);
      AtomicBoolean permissionRequested = new AtomicBoolean();
      Observation result =
          fixture.provider.dispatch(
              fixture.context,
              fixture.step,
              () -> {
                permissionRequested.set(true);
                return true;
              });
      assertThat(result.getOutcome()).as(read).isEqualTo(Outcome.REJECTED);
      assertThat(result.getCode()).as(read).isEqualTo("VOLCENGINE_PREFLIGHT_UNAVAILABLE");
      assertThat(permissionRequested).as(read).isFalse();
      assertThat(fixture.actions).as(read).doesNotContain(START);
    }
  }

  @Test
  void deniedPermissionAfterAllReadsNeverSendsStart() {
    Fixture fixture = new Fixture(null);
    Observation result =
        fixture.provider.dispatch(
            fixture.context,
            fixture.step,
            () -> {
              assertThat(fixture.actions).endsWith(LAST_READ);
              return false;
            });
    assertThat(result.getOutcome()).isEqualTo(Outcome.REJECTED);
    assertThat(result.getCode()).isEqualTo("DISPATCH_NOT_AUTHORIZED");
    assertThat(fixture.actions).doesNotContain(START);
  }

  @Test
  void lostStartResponseIsUncertainAndNeverChangesTheRequestedRestoreMode() {
    Fixture fixture = new Fixture(START);
    Observation result =
        fixture.provider.dispatch(
            fixture.context,
            fixture.step,
            () -> {
              assertThat(fixture.actions).endsWith(LAST_READ);
              return true;
            });
    assertThat(result.getOutcome()).isEqualTo(Outcome.UNKNOWN);
    assertThat(result.getCode()).isEqualTo("RESULT_UNCERTAIN");
    assertThat(fixture.actions)
        .containsExactly(
            "GetCallerIdentity", "GetGWSApplication", "GetGWSApplicationDraft", LAST_READ, START);
  }

  @Test
  void nativeRestartNeverAdoptsAnInstanceFromAnotherAcknowledgedJob() {
    Fixture fixture = new Fixture(null);
    OperationStep step =
        new OperationStep(
            "operation", 2L, Action.RESTART, fixture.step.getCommand(), "draft", "3", null);
    Observation other = Observation.outcome(Outcome.WAITING, null);
    other.setJobId("another-job");
    other.setInstanceId("4");
    assertThat(fixture.provider.observe(fixture.context, step, other).getOutcome())
        .isEqualTo(Outcome.WAITING);
    assertThat(fixture.actions).isEmpty();
  }

  @Test
  void publicInstanceSubmissionSendsFrozenSqlAndExplicitRestoreStrategy() {
    Fixture fixture = new Fixture(null);
    fixture.step.getCommand().getRequest().setSubmitInstance(true);
    Observation ack = fixture.provider.dispatch(fixture.context, fixture.step, () -> true);
    assertThat(ack.getInstanceId()).isEqualTo("4");
    assertThat(fixture.actions).endsWith("StartApplicationInstance").doesNotContain(START);
    JsonObject app = fixture.submitted.getAsJsonObject("App");
    assertThat(app.get("ProjectId").getAsString()).isEqualTo("project");
    assertThat(app.get("SqlText").getAsString()).isEqualTo("SELECT 1");
    assertThat(app.get("JobType").getAsString()).isEqualTo("FLINK_STREAMING_SQL");
    assertThat(app.getAsJsonObject("DeployRequest").get("Queue").getAsString()).isEqualTo("pool");
    assertThat(
            fixture.submitted.getAsJsonObject("RestoreStrategy").get("SavepointId").getAsString())
        .isEqualTo(SAVEPOINT);
  }

  @Test
  void providerLatestRequiresCloudStartMethodAndSendsNoSavepointId() {
    Fixture fixture = new Fixture(null);
    fixture.step.getCommand().getRequest().getRestore().setMode(RestoreMode.FROM_CLOUD_LATEST);
    fixture.step.getCommand().setRestoreRef(null);
    assertThat(fixture.provider.dispatch(fixture.context, fixture.step, () -> true).getInstanceId())
        .isEqualTo("4");
    assertThat(fixture.actions).endsWith("GetGWSStartMethod", START).doesNotContain(LAST_READ);
  }

  @Test
  void restartChecksLatestRestoreBeforeStoppingTheCurrentInstance() {
    Fixture fixture = new Fixture(null);
    FrozenCommand cmd = fixture.step.getCommand();
    cmd.setActions(Arrays.asList(Action.STOP, Action.START));
    cmd.setProviderInstance("7");
    cmd.getRequest().getRestore().setMode(RestoreMode.FROM_CLOUD_LATEST);
    cmd.setRestoreRef(null);
    OperationStep stop = new OperationStep("restart", 2L, Action.STOP, cmd, "draft", "3", null);

    fixture.latestAvailable = false;
    Observation result = fixture.provider.dispatch(fixture.context, stop, () -> true);
    assertThat(result.getCode()).isEqualTo("LATEST_STATE_UNAVAILABLE");
    assertThat(fixture.actions).doesNotContain("CancelApplicationInstance");

    fixture.latestAvailable = true;
    fixture.provider.dispatch(fixture.context, stop, () -> true);
    assertThat(fixture.actions).endsWith("GetGWSStartMethod", "CancelApplicationInstance");
  }

  @Test
  void validatesUnsavedSqlAgainstTheBoundDraftWithoutChangingTheCloudDraft() {
    Fixture fixture = new Fixture(null);
    Spec spec = fixture.step.getCommand().getSpec();
    spec.setSql("SELECT 2");
    assertThat(fixture.provider.validateCloudSql(fixture.context, "draft", spec))
        .isEqualTo("VALID");
    assertThat(fixture.actions)
        .containsExactly(
            "GetCallerIdentity", "GetGWSApplicationDraft", "ValidateGWSApplicationDraft");
    assertThat(fixture.submitted.get("SqlText").getAsString()).isEqualTo("SELECT 2");
    assertThat(fixture.submitted.get("Id").getAsString()).isEqualTo("draft");

    fixture.actions.clear();
    assertThat(fixture.provider.validateCloudSql(fixture.context, "different-draft", spec))
        .isEqualTo("UNAVAILABLE");
    assertThat(fixture.actions).doesNotContain("ValidateGWSApplicationDraft");
  }

  @Test
  void directSubmissionOmitsJobIdAndRequiresTheAcknowledgedJobAndReleaseMarker() {
    Fixture fixture = new Fixture(null);
    FrozenCommand cmd = fixture.step.getCommand();
    cmd.getRequest().getRestore().setMode(RestoreMode.FROM_NEW);
    cmd.setRestoreRef(null);
    cmd.setSpecHash("fixture-hash");
    OperationStep direct = new OperationStep("direct", 2L, Action.SUBMIT, cmd, null, null, null);
    Observation ack = fixture.provider.dispatch(fixture.context, direct, () -> true);
    assertThat(ack.getJobId()).isEqualTo("3");
    assertThat(ack.getInstanceId()).isEqualTo("4");
    assertThat(fixture.submitted.has("Id")).isFalse();
    assertThat(fixture.submitted.getAsJsonObject("App").get("UniqueKey").getAsString())
        .isEqualTo(cmd.getReleaseMarker());
    assertThat(fixture.actions).containsExactly("GetCallerIdentity", "StartApplicationInstance");
    assertThat(fixture.provider.observe(fixture.context, direct, ack).getOutcome())
        .isEqualTo(Outcome.WAITING);
    fixture.publication.remove("AppDraftId");
    fixture.directInstance = true;
    Observation confirmed = fixture.provider.observe(fixture.context, direct, ack);
    assertThat(confirmed.getOutcome()).isEqualTo(Outcome.CONFIRMED);
    assertThat(confirmed.getInstanceStartedAt()).isEqualTo(1704067200000L);
    for (String invalid :
        java.util.Arrays.asList("invalid", "2024-02-30 08:00:00", "+300000000-01-01 00:00:00")) {
      fixture.startTime = invalid;
      Observation withoutStart = fixture.provider.observe(fixture.context, direct, ack);
      assertThat(withoutStart.getOutcome()).isEqualTo(Outcome.CONFIRMED);
      assertThat(withoutStart.getInstanceStartedAt()).isZero();
    }
    fixture
        .publication
        .getAsJsonObject("Conf")
        .addProperty("pipeline.name", "someone-elses-release");
    assertThat(fixture.provider.observe(fixture.context, direct, ack).getOutcome())
        .isEqualTo(Outcome.WAITING);
  }

  @Test
  void recoversLostDirectReceiptOnlyForAUniqueInstanceOfTheMarkedRelease() {
    Fixture fixture = new Fixture(null);
    FrozenCommand cmd = fixture.step.getCommand();
    cmd.setJobName("test-job");
    cmd.setInstanceBeforeDispatch("");
    cmd.getRequest().getRestore().setMode(RestoreMode.FROM_NEW);
    cmd.setRestoreRef(null);
    OperationStep direct = new OperationStep("direct", 2L, Action.SUBMIT, cmd, null, null, null);
    fixture.publication.remove("AppDraftId");
    fixture.directInstance = true;
    Observation observed = fixture.provider.observe(fixture.context, direct, null);
    assertThat(observed.getOutcome()).isEqualTo(Outcome.CONFIRMED);
    assertThat(observed.getInstanceId()).isEqualTo("4");
    fixture.ambiguous = true;
    assertThat(fixture.provider.observe(fixture.context, direct, null).getOutcome())
        .isEqualTo(Outcome.WAITING);
    assertThat(fixture.actions).doesNotContain("StartApplicationInstance");
  }

  @Test
  void failedSavepointOnlyEndsTheMatchingOperation() {
    Fixture fixture = new Fixture(null);
    fixture.savepointStatus = "FAIL";
    FrozenCommand cmd = fixture.step.getCommand();
    OperationStep step = new OperationStep("other", 2L, Action.SNAPSHOT, cmd, "draft", "3", null);
    assertThat(fixture.provider.observe(fixture.context, step, null).getOutcome())
        .isEqualTo(Outcome.WAITING);
    step = new OperationStep("operation", 2L, Action.SNAPSHOT, cmd, "draft", "3", null);
    assertThat(fixture.provider.observe(fixture.context, step, null).getCode())
        .isEqualTo("CLOUD_SAVEPOINT_FAILED");
  }

  private static final class Fixture {
    private boolean ambiguous;
    private String savepointStatus;
    private boolean directInstance;
    private String startTime = "2024-01-01 08:00:00";
    private boolean latestAvailable = true;
    private JsonObject submitted;
    private final Context context;
    private final OperationStep step;
    private final VolcengineManagedFlinkProvider provider;
    private final List<String> actions = new ArrayList<>();
    private final JsonObject publication;

    Fixture(String failingAction) {
      Target target = new Target();
      target.setEngineRef(ENGINE);
      target.setProjectId("project");
      target.setDirectoryId("directory");
      target.setResourcePoolId("pool");
      target.setResourcePoolName("test-pool");
      context = new Context(1, "cn-beijing", "account", target, null);

      Spec spec = new Spec();
      spec.setType(JobType.SQL);
      spec.setSql("SELECT 1");
      Restore restore = new Restore();
      restore.setMode(RestoreMode.FROM_SNAPSHOT);
      Command request = new Command();
      request.setRestore(restore);
      FrozenCommand command = new FrozenCommand();
      command.setRequest(request);
      command.setSpec(spec);
      command.setEngineRef(ENGINE);
      command.setReleaseMarker("frozen-release");
      command.setRestoreRef(SAVEPOINT);
      step = new OperationStep("operation", 2L, Action.START, command, "draft", "3", null);
      publication =
          object(
              "{\"Id\":\"3\",\"ProjectId\":\"project\",\"AppDraftId\":\"draft\","
                  + "\"DeployRequest\":{\"Queue\":\"pool\"},\"JobType\":\"FLINK_STREAMING_SQL\","
                  + "\"EngineVersion\":\""
                  + ENGINE
                  + "\",\"SqlText\":\"SELECT 1\"}");
      publication.add("Conf", VolcengineExecutable.settings(step));
      command.setProviderVersion(VolcengineExecutable.revision(publication));

      OkHttpClient transport = new OkHttpClient();
      transport
          .interceptors()
          .add(
              chain -> {
                String action = chain.request().httpUrl().queryParameter("Action");
                actions.add(action);
                if ("StartApplicationInstance".equals(action)) {
                  Buffer body = new Buffer();
                  chain.request().body().writeTo(body);
                  submitted = object(body.readUtf8());
                  assertThat(chain.request().httpUrl().queryParameter("Version"))
                      .isEqualTo("2025-01-01");
                }
                if ("ValidateGWSApplicationDraft".equals(action)) {
                  Buffer body = new Buffer();
                  chain.request().body().writeTo(body);
                  submitted = object(body.readUtf8());
                  assertThat(chain.request().httpUrl().queryParameter("ProjectId"))
                      .isEqualTo("project");
                }
                if (START.equals(action)) {
                  Buffer body = new Buffer();
                  chain.request().body().writeTo(body);
                  JsonObject parameters = object(body.readUtf8());
                  if (step.getCommand().getRequest().getRestore().getMode()
                      == RestoreMode.FROM_CLOUD_LATEST) {
                    assertThat(parameters.get("Type").getAsString()).isEqualTo("FROM_LATEST");
                    assertThat(parameters.has("SavepointId")).isFalse();
                  } else {
                    assertThat(parameters.get("Type").getAsString()).isEqualTo("FROM_SAVEPOINT");
                    assertThat(parameters.get("SavepointId").getAsString()).isEqualTo(SAVEPOINT);
                  }
                }
                if (action.equals(failingAction)) {
                  throw new SocketTimeoutException("fixture transport failure");
                }
                JsonObject metadata = new JsonObject();
                metadata.addProperty("RequestId", "fixture-request");
                JsonObject envelope = new JsonObject();
                envelope.add("ResponseMetadata", metadata);
                envelope.add("Result", response(action));
                return new Response.Builder()
                    .request(chain.request())
                    .protocol(Protocol.HTTP_1_1)
                    .code(200)
                    .message("OK")
                    .body(
                        ResponseBody.create(
                            MediaType.parse("application/json"), envelope.toString()))
                    .build();
              });
      VolcengineOpenApi api =
          new VolcengineOpenApi(
              context.getRegion(),
              Credentials.getCredentials("fixture-ak", "fixture-sk"),
              transport);
      VolcengineProfileBinding binding =
          new VolcengineProfileBinding(
              "fixture-profile",
              1,
              "account",
              "project",
              "directory",
              "pool",
              "test-pool",
              ENGINE,
              true,
              true,
              false,
              false,
              "",
              "");
      provider =
          new VolcengineManagedFlinkProvider(binding, null) {
            @Override
            VolcengineOpenApi client(Context ignored) {
              return api;
            }
          };
    }

    private JsonObject response(String action) {
      switch (action) {
        case "ListGWSApplication":
          return object("{\"Total\":1,\"Records\":[{\"Id\":\"3\",\"JobName\":\"test-job\"}]}");
        case "ListApplicationInstance":
          JsonObject page = object("{\"Current\":1,\"Size\":50}");
          com.google.gson.JsonArray records = new com.google.gson.JsonArray();
          records.add(response("GetApplicationInstance"));
          if (ambiguous) {
            JsonObject other = response("GetApplicationInstance");
            other.addProperty("Id", "5");
            records.add(other);
          }
          page.add("Records", records);
          page.addProperty("Total", records.size());
          return page;
        case "GetCallerIdentity":
          return object("{\"AccountId\":\"account\"}");
        case "GetApplicationInstance":
          if (directInstance) {
            JsonObject instance = publication.deepCopy();
            instance.addProperty("Id", "4");
            instance.addProperty("JobId", "3");
            // Direct instances have no provider draft VersionId.
            instance.addProperty("State", "SUCCEEDED");
            instance.addProperty("StartTime", startTime);
            return instance;
          }
          return object(
              "{\"Id\":\"7\",\"ProjectId\":\"project\",\"JobId\":\"3\",\"State\":\"RUNNING\"}");
        case "CancelApplicationInstance":
          return object("{\"Success\":true}");
        case "GetGWSApplication":
          return publication;
        case "ValidateGWSApplicationDraft":
          return object("{\"Data\":true}");
        case "GetGWSApplicationDraft":
          return object(
              "{\"Id\":\"draft\",\"ProjectId\":\"project\",\"DirectoryId\":\"directory\",\"JobType\":\"FLINK_STREAMING_SQL\",\"EngineVersion\":\""
                  + ENGINE
                  + "\"}");
        case "GetGWSStartMethod":
          return latestAvailable
              ? object("{\"EnableStartMethods\":[\"FROM_LATEST\"]}")
              : object("{\"EnableStartMethods\":[]}");
        case LAST_READ:
          if (savepointStatus != null) {
            return object(
                "{\"SavepointInfos\":[{\"Description\":\"streampark-operation-operation\","
                    + "\"Status\":\""
                    + savepointStatus
                    + "\"}]}");
          }
          return object(
              "{\"SavepointInfos\":[{\"SavepointId\":\""
                  + SAVEPOINT
                  + "\",\"Status\":\"AVAILABLE\",\"SavepointPath\":\"fixture-path\"}]}");
        case "StartApplicationInstance":
          return object("{\"Id\":\"3\",\"InstanceId\":\"4\"}");
        case START:
          return object("{\"InstanceId\":\"4\"}");
        default:
          throw new AssertionError("Unexpected action " + action);
      }
    }
  }

  private static JsonObject object(String value) {
    return JsonParser.parseString(value).getAsJsonObject();
  }
}
