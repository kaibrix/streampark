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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CloudState;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Context;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.FrozenCommand;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Outcome;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.ReadStatus;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Target;

import com.google.gson.JsonArray;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

class VolcengineInstanceHistoryTest {

  @Test
  void findsTheOnlyActiveInstanceOnTheSecondPage() {
    Fixture fixture =
        new Fixture(
            page -> page == 1 ? history(1, 51, 101, 50) : history(2, 51, 151, 1, "RUNNING"));

    assertThat(fixture.inspect())
        .extracting(CloudState::getStatus, CloudState::getInstanceId, CloudState::getRunState)
        .containsExactly(ReadStatus.FOUND, "151", "RUNNING");
    assertThat(fixture.requestedPages).containsExactly(1, 2, 1);
  }

  @Test
  void usesNewestTerminalInstanceAfterReadingAllPagesAndAcceptsAnEmptyHistory() {
    Fixture stopped =
        new Fixture(page -> page == 1 ? history(1, 51, 101, 50) : history(2, 51, 151, 1));

    assertThat(stopped.inspect())
        .extracting(CloudState::getStatus, CloudState::getInstanceId, CloudState::getRunState)
        .containsExactly(ReadStatus.FOUND, "101", "STOPPED");
    assertThat(stopped.requestedPages).containsExactly(1, 2, 1);

    Fixture empty = new Fixture(page -> history(1, 0, 0, 0));
    assertThat(empty.inspect())
        .extracting(CloudState::getStatus, CloudState::getInstanceId)
        .containsExactly(ReadStatus.FOUND, null);
    assertThat(empty.requestedPages).containsExactly(1);
  }

  @Test
  void leavesRepeatedTruncatedAndForeignPagesStale() {
    JsonObject foreign = history(2, 51, 151, 1);
    foreign.getAsJsonArray("Records").get(0).getAsJsonObject().addProperty("ProjectId", "other");
    JsonObject wrongSize = history(2, 51, 151, 1);
    wrongSize.addProperty("Size", 1);
    for (JsonObject invalid :
        List.of(
            history(2, 51, 150, 1),
            history(2, 51, 151, 0),
            history(2, 52, 151, 1),
            history(1, 51, 151, 1),
            foreign,
            wrongSize)) {
      Fixture fixture = new Fixture(page -> page == 1 ? history(1, 51, 101, 50) : invalid);
      assertThat(fixture.inspect().getStatus()).isEqualTo(ReadStatus.STALE);
      assertThat(fixture.requestedPages).containsExactly(1, 2);
    }
  }

  @Test
  void leavesChangedHeadsMultipleActiveInstancesAndIncompleteReadsStale() {
    List<JsonObject> pages = new ArrayList<>();
    pages.add(history(1, 51, 101, 50));
    pages.add(history(2, 51, 151, 1));
    pages.add(history(1, 51, 101, 50, "STARTING"));
    Fixture changed = new Fixture(page -> pages.remove(0));
    assertThat(changed.inspect().getStatus()).isEqualTo(ReadStatus.STALE);
    assertThat(changed.requestedPages).containsExactly(1, 2, 1);

    Fixture multiple =
        new Fixture(
            page ->
                page == 1 ? history(1, 51, 101, 50, "UNKNOWN") : history(2, 51, 151, 1, "RUNNING"));
    assertThat(multiple.inspect().getStatus()).isEqualTo(ReadStatus.STALE);

    Fixture failed = new Fixture(page -> page == 1 ? history(1, 51, 101, 50) : null);
    assertThat(failed.inspect().getStatus()).isEqualTo(ReadStatus.STALE);
    assertThat(failed.requestedPages).containsExactly(1, 2);

    Fixture overBudget = new Fixture(page -> history(1, 1001, 101, 50));
    assertThat(overBudget.inspect().getStatus()).isEqualTo(ReadStatus.STALE);
    assertThat(overBudget.requestedPages).containsExactly(1);
  }

  @Test
  void propagatesThrottlingAuthorizationAndRetryAfterThroughEveryRead() {
    long now = System.currentTimeMillis();
    Fixture throttled =
        new Fixture(page -> history(1, 51, 101, 50), "ListApplicationInstance", 2, 429, "120");
    CloudState limited = throttled.inspect();
    assertThat(limited.getStatus()).isEqualTo(ReadStatus.THROTTLED);
    assertThat(limited.getRetryNotBefore())
        .isBetween(now + 120_000, System.currentTimeMillis() + 120_000);
    assertThat(throttled.requestedPages).containsExactly(1, 2);

    Fixture denied =
        new Fixture(page -> history(1, 0, 0, 0), "GetGWSApplicationDraft", 0, 403, null);
    assertThat(denied.inspect().getStatus()).isEqualTo(ReadStatus.AUTH_ERROR);
    assertThat(denied.requestedPages).isEmpty();

    Fixture unavailable =
        new Fixture(
            page -> history(1, 0, 0, 0),
            "GetGWSApplication",
            0,
            503,
            "Wed, 21 Oct 2037 07:28:00 GMT");
    CloudState stale = unavailable.inspect();
    assertThat(stale.getStatus()).isEqualTo(ReadStatus.STALE);
    assertThat(stale.getRetryNotBefore()).isEqualTo(2_139_722_880_000L);
  }

  @Test
  void throttledStopReadbackWaitsForEvidence() {
    Fixture fixture =
        new Fixture(page -> history(1, 0, 0, 0), "GetApplicationInstance", 0, 429, "90");
    FrozenCommand command = new FrozenCommand();
    command.setProviderInstance("125");
    OperationStep step = new OperationStep("stop-once", 1L, Action.STOP, command, null, "31", null);
    long before = System.currentTimeMillis();
    Observation evidence = fixture.provider.observe(fixture.context, step, null);

    assertThat(evidence.getOutcome()).isEqualTo(Outcome.WAITING);
    assertThat(evidence.getCode()).isEqualTo("VOLCENGINE_READ_THROTTLED");
    assertThat(evidence.getRetryNotBefore())
        .isBetween(before + 90_000, System.currentTimeMillis() + 90_000);
  }

  @Test
  void ignoresInvalidExpiredAndOverflowingRetryAfter() {
    long now = 2_139_722_880_000L;
    assertThat(VolcengineOpenApi.retryNotBefore(" 20 ", now)).isEqualTo(now + 20_000);
    for (String value :
        List.of("", "0", "-1", "3.5", "9223372036854775807", "Wed, 21 Oct 2015 07:28:00 GMT")) {
      assertThat(VolcengineOpenApi.retryNotBefore(value, now)).isZero();
    }
  }

  private static JsonObject history(int page, int total, int first, int size) {
    return history(page, total, first, size, "STOPPED");
  }

  private static JsonObject history(int page, int total, int first, int size, String firstState) {
    JsonObject result = new JsonObject();
    result.addProperty("Current", page);
    result.addProperty("Total", total);
    result.addProperty("Size", 50);
    JsonArray records = new JsonArray();
    for (int offset = 0; offset < size; offset++) {
      int id = first + offset;
      JsonObject instance = new JsonObject();
      instance.addProperty("Id", Integer.toString(id));
      instance.addProperty("ProjectId", "project");
      instance.addProperty("JobId", "31");
      instance.addProperty("ApplicationId", "s-" + id);
      instance.addProperty("State", offset == 0 ? firstState : "STOPPED");
      instance.addProperty("StartTime", id == 101 ? "2026-01-10 00:00:02" : "2026-01-10 00:00:01");
      records.add(instance);
    }
    result.add("Records", records);
    return result;
  }

  private static final class Fixture {
    private final Context context;
    private final VolcengineManagedFlinkProvider provider;
    private final List<Integer> requestedPages = new ArrayList<>();

    Fixture(IntFunction<JsonObject> responses) {
      this(responses, null, 0, 0, null);
    }

    Fixture(
        IntFunction<JsonObject> responses,
        String failingAction,
        int failingPage,
        int status,
        String retryAfter) {
      Target target = new Target();
      target.setProjectId("project");
      target.setDirectoryId("dir");
      target.setResourcePoolId("pool");
      context = new Context("cn-beijing", "account", target, null);
      OkHttpClient transport = new OkHttpClient();
      transport
          .interceptors()
          .add(
              chain -> {
                String action = chain.request().httpUrl().queryParameter("Action");
                Response.Builder response =
                    new Response.Builder().request(chain.request()).protocol(Protocol.HTTP_1_1);
                JsonObject result;
                if (action.equals(failingAction) && failingPage == 0) {
                  return error(response, status, retryAfter);
                }
                switch (action) {
                  case "GetGWSApplication":
                    result =
                        json(
                            "{\"Id\":\"31\",\"ProjectId\":\"project\",\"AppDraftId\":\"draft\","
                                + "\"DeployRequest\":{\"Queue\":\"pool\"},\"SqlText\":\"SELECT 1\"}");
                    break;
                  case "GetGWSApplicationDraft":
                    result =
                        json(
                            "{\"Id\":\"draft\",\"ProjectId\":\"project\",\"DirectoryId\":\"dir\"}");
                    break;
                  case "ListApplicationInstance":
                    Buffer body = new Buffer();
                    chain.request().body().writeTo(body);
                    JsonObject query = json(body.readUtf8());
                    assertThat(query.get("ProjectId").getAsString()).isEqualTo("project");
                    assertThat(query.get("JobId").getAsLong()).isEqualTo(31);
                    assertThat(query.get("PageSize").getAsInt()).isEqualTo(50);
                    assertThat(query.get("SortField").getAsString()).isEqualTo("StartTime");
                    assertThat(query.get("SortOrder").getAsString()).isEqualTo("desc");
                    int page = query.get("PageNum").getAsInt();
                    requestedPages.add(page);
                    if (action.equals(failingAction) && page == failingPage) {
                      return error(response, status, retryAfter);
                    }
                    result = responses.apply(page);
                    if (result == null) {
                      throw new IOException("Fixture page read failed");
                    }
                    break;
                  default:
                    throw new AssertionError("Unexpected cloud action " + action);
                }
                JsonObject envelope = json("{\"ResponseMetadata\":{\"RequestId\":\"fixture\"}}");
                envelope.add("Result", result);
                return response
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
      provider =
          new VolcengineManagedFlinkProvider(null, null) {
            @Override
            VolcengineOpenApi client(Context ignored) {
              return api;
            }
          };
    }

    CloudState inspect() {
      return provider.inspect(context, "31");
    }

    private static Response error(Response.Builder response, int status, String retryAfter) {
      if (retryAfter != null) {
        response.header("Retry-After", retryAfter);
      }
      return response
          .code(status)
          .message("Unavailable")
          .body(ResponseBody.create(MediaType.parse("text/plain"), "Gateway failure"))
          .build();
    }
  }

  private static JsonObject json(String value) {
    return JsonParser.parseString(value).getAsJsonObject();
  }
}
