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

import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Context;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.LogQuery;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Target;
import org.apache.streampark.console.core.managedflink.VolcengineOpenApi.Reply;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.squareup.okhttp.HttpUrl;
import com.volcengine.sign.Credentials;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VolcengineTlsLogsTest {
  private static final String TOPIC = "00000000-0000-0000-0000-000000000001";
  private static final String REGION = "cn-beijing";
  private static final String HOST = "https://tls-cn-beijing.volces.com";
  private static final ZoneId BEIJING = ZoneId.of("Asia/Shanghai");
  private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
  private static final String INDEX =
      "{\"KeyValue\":[{\"Key\":\"application_id\",\"Value\":{\"ValueType\":\"text\","
          + "\"CaseSensitive\":true,\"Delimiter\":\"\"}},{\"Key\":\"deployment_id\",\"Value\":{"
          + "\"ValueType\":\"text\",\"CaseSensitive\":true,\"Delimiter\":\"\"}}]}";
  private final VolcengineTlsLogs logs =
      new VolcengineTlsLogs(
          true,
          REGION,
          TOPIC,
          "application_id",
          "deployment_id",
          "component",
          "severity",
          "message");
  private final VolcengineTlsLogs.SearchApi api = mock(VolcengineTlsLogs.SearchApi.class);

  @Test
  void signsFixedTopicRequestsLikeTheOfficialVolcengineSigner() throws Exception {
    Credentials fake =
        Credentials.getCredentials("FIXTURE_ACCESS", "FIXTURE_SECRET", "FIXTURE_TOKEN");
    Instant time = Instant.parse("2026-11-22T01:02:03Z");
    String body =
        "{\"TopicId\":\""
            + TOPIC
            + "\",\"Query\":\"application_id:\\\"s-42\\\"\","
            + "\"StartTime\":1795309200000,\"EndTime\":1795309260000,\"Limit\":2,\"Sort\":\"desc\"}";
    String authorization =
        "HMAC-SHA256 Credential=FIXTURE_ACCESS/20261122/cn-beijing/TLS/request,"
            + " SignedHeaders=content-type;host;x-content-sha256;x-date;x-security-token;x-tls-apiversion,"
            + " Signature=";
    assertThat(
            VolcengineTlsApi.headers(
                "POST", HttpUrl.parse(HOST + "/SearchLogs"), REGION, fake, body, time))
        .containsEntry("x-tls-apiversion", "0.3.0")
        .containsEntry("content-md5", "DB9B3F570B76274DFAD6CF37B6E72FA6")
        .containsEntry(
            "authorization",
            authorization.replace("SignedHeaders=", "SignedHeaders=content-md5;")
                + "9e835d4e6b13ced7b1fa20b8a1de128f992fe1c5ab86abc6d7dd05c82838e48f");
    assertThat(
            VolcengineTlsApi.headers(
                "GET",
                HttpUrl.parse(HOST + "/DescribeIndex?TopicId=" + TOPIC),
                REGION,
                fake,
                "",
                time))
        .containsEntry(
            "authorization",
            authorization + "8cdfca9bfc498fe41ba8dcade902874b192ba32a7de2f0843fb71dca1bd8b00c");
  }

  @Test
  void pinsCursorToInstanceActorFiltersAndTimeWithExactPostFiltering() throws Exception {
    when(api.topic(TOPIC)).thenReturn(success("{\"Ttl\":7}"));
    when(api.index(TOPIC)).thenReturn(success(INDEX));
    JsonObject run = run(0);
    long rowTime = Instant.now().minusSeconds(150).toEpochMilli();
    JsonObject vendor =
        json(
            "{\"ResultStatus\":\"complete\",\"ListOver\":false,"
                + "\"Context\":\"vendor-cursor\",\"Analysis\":false,\"Logs\":[]}");
    vendor.getAsJsonArray("Logs").add(row("s-42", "deployment-1", rowTime));
    vendor.getAsJsonArray("Logs").add(row("s-43", "deployment-1", rowTime));
    vendor.getAsJsonArray("Logs").add(row("s-42", "deployment-2", rowTime));
    vendor.getAsJsonArray("Logs").add(row("s-42", "deployment-1", rowTime - 200_000));
    when(api.search(any()))
        .thenReturn(new Reply(200, vendor, "fixture-request-123456", null, true))
        .thenReturn(
            success(
                "{\"ResultStatus\":\"incomplete\",\"ListOver\":true,"
                    + "\"Analysis\":false,\"Logs\":[]}"));
    LogQuery first = new LogQuery(4, null, null, null, null, "request-42", null);
    Context context = context(1, TOPIC);
    assertThat(logs.configured(context)).isTrue();
    assertThat(logs.configured(context(1, "different-topic"))).isFalse();
    Map<String, Object> page = logs.read(api, context, "job1", run, first);
    assertThat(page)
        .containsEntry("status", "AVAILABLE")
        .containsEntry("requestIdSuffix", "123456")
        .containsEntry(
            "rows",
            List.of(
                Map.of(
                    "timestamp", rowTime, "component", "taskmanager", "message", "fixture line")));
    String cursor = (String) page.get("nextCursor");
    assertThat(cursor).isNotBlank().isNotEqualTo("vendor-cursor");
    for (LogQuery wrong :
        List.of(
            new LogQuery(5, null, null, null, null, "request-42", cursor),
            new LogQuery(4, null, null, null, null, "another-filter", cursor))) {
      assertThatThrownBy(() -> logs.read(api, context, "job1", run, wrong))
          .isInstanceOf(ResponseStatusException.class);
    }
    LogQuery next = new LogQuery(4, null, null, null, null, "request-42", cursor);
    assertThatThrownBy(() -> logs.read(api, context(2, TOPIC), "job1", run, next))
        .isInstanceOf(ResponseStatusException.class);
    assertThatThrownBy(() -> logs.read(api, context, "job2", run, next))
        .isInstanceOf(ResponseStatusException.class);
    assertThat(logs.read(api, context, "job1", run, next))
        .containsEntry("status", "PARTIAL")
        .containsEntry("from", page.get("from"))
        .containsEntry("until", page.get("until"))
        .doesNotContainKey("nextCursor");
    verify(api)
        .search(
            eq(
                VolcengineTlsLogs.request(
                    TOPIC,
                    "application_id:\"s-42\" AND deployment_id:\"deployment-1\" AND \"request-42\"",
                    (long) page.get("from"),
                    (long) page.get("until"),
                    "vendor-cursor")));
  }

  @Test
  void returnsDistinctRetentionAuthorizationAndUnverifiedIndexStatuses() throws Exception {
    Context context = context(1, TOPIC);
    LogQuery query = new LogQuery(1, null, null, null, null, null, null);
    when(api.topic(TOPIC)).thenReturn(new Reply(403, null, "request-abcdef", null, false));
    assertThat(logs.read(api, context, "job", run(0), query))
        .containsEntry("status", "NO_ACCESS")
        .containsEntry("requestIdSuffix", "abcdef");
    when(api.topic(TOPIC)).thenReturn(success("{\"Ttl\":7}"));
    assertThat(logs.read(api, context, "job", run(9), query)).containsEntry("status", "EXPIRED");
    when(api.index(TOPIC))
        .thenReturn(success(INDEX.replace("\"Delimiter\":\"\"", "\"Delimiter\":\"-\"")));
    assertThat(logs.read(api, context, "job", run(0), query))
        .containsEntry("status", "NEEDS_CONFIGURATION");
    verify(api, never()).search(any());
  }

  @Test
  void rejectsAnalyticsSyntaxAndEscapesQuotedText() {
    assertThat(VolcengineTlsLogs.literal("a\" AND b\\c")).isEqualTo("\"a\\\" AND b\\\\c\"");
    assertThatThrownBy(() -> VolcengineTlsLogs.literal("abc | select *"))
        .isInstanceOf(ResponseStatusException.class);
  }

  @Test
  void redactsCredentialsSignedLinksAndExecutableInputFromMessages() {
    for (String line :
        List.of(
            "Authorization: HMAC-SHA256 value",
            "{\"session_token\": \"example\"}",
            "AK=example SK=value",
            "{\"SecretAccessKey\": \"example\"}",
            "entrypoint --password fixture",
            "https://fixture.invalid/object?X-Tos-Signature=value",
            "Execution failed for SELECT fixture FROM events",
            "mainArgs=--configuration example")) {
      assertThat(VolcengineTlsLogs.safeMessage(line)).isEqualTo("[redacted]");
    }
    assertThat(VolcengineTlsLogs.safeMessage("RequestId = test-fixture completed"))
        .isEqualTo("RequestId = test-fixture completed");
  }

  @Test
  void logLevelRequiresAnExactIndexAndIsCheckedOnReturnedRows() throws Exception {
    when(api.topic(TOPIC)).thenReturn(success("{\"Ttl\":7}"));
    when(api.index(TOPIC)).thenReturn(success(INDEX));
    LogQuery filter = new LogQuery(4, null, null, null, "WARN", null, null);
    Context context = context(1, TOPIC);
    JsonObject run = run(0);
    assertThat(logs.read(api, context, "job1", run, filter))
        .containsEntry("status", "NEEDS_CONFIGURATION");
    verify(api, never()).search(any());
    JsonObject withLevel = json(INDEX);
    JsonObject index = withLevel.getAsJsonArray("KeyValue").get(0).getAsJsonObject().deepCopy();
    index.addProperty("Key", "severity");
    withLevel.getAsJsonArray("KeyValue").add(index);
    when(api.index(TOPIC)).thenReturn(new Reply(200, withLevel, null, null, true));
    long timestamp = Instant.now().minusSeconds(150).toEpochMilli();
    JsonObject vendor =
        json(
            "{\"ResultStatus\":\"complete\",\"ListOver\":true,"
                + "\"Analysis\":false,\"Logs\":[]}");
    JsonObject warning = row("s-42", "deployment-1", timestamp);
    warning.addProperty("severity", "WARN");
    JsonObject info = warning.deepCopy();
    info.addProperty("severity", "INFO");
    vendor.getAsJsonArray("Logs").add(info);
    vendor.getAsJsonArray("Logs").add(warning);
    when(api.search(any())).thenReturn(new Reply(200, vendor, null, null, true));
    assertThat(logs.read(api, context, "job1", run, filter))
        .containsEntry(
            "rows",
            List.of(
                Map.of(
                    "timestamp",
                    timestamp,
                    "component",
                    "taskmanager",
                    "message",
                    "fixture line",
                    "level",
                    "WARN")));
  }

  private Context context(long team, String topic) {
    Target target = new Target();
    target.setTlsRegion(REGION);
    target.setTlsTopic(topic);
    target.setProjectId("project");
    target.setDirectoryId("directory");
    return new Context(team, REGION, "account", target, null);
  }

  private JsonObject run(int daysAgo) {
    JsonObject run =
        json(
            "{\"Id\":\"42\",\"ApplicationId\":\"s-42\","
                + "\"DeploymentId\":\"deployment-1\",\"State\":\"STOPPED\"}");
    LocalDateTime now = LocalDateTime.now(BEIJING).minusDays(daysAgo);
    run.addProperty("StartTime", DATE.format(now.minusMinutes(5)));
    run.addProperty("EndTime", DATE.format(now.minusMinutes(1)));
    return run;
  }

  private JsonObject row(String application, String deployment, long at) {
    JsonObject result = new JsonObject();
    result.addProperty("application_id", application);
    result.addProperty("deployment_id", deployment);
    result.addProperty("__time__", at);
    result.addProperty("component", "taskmanager");
    result.addProperty("message", "fixture line");
    return result;
  }

  private Reply success(String body) {
    return new Reply(200, json(body), null, null, true);
  }

  private JsonObject json(String body) {
    return JsonParser.parseString(body).getAsJsonObject();
  }
}
