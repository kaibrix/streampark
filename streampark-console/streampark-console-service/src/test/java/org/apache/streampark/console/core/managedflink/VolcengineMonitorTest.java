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
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VolcengineMonitorTest {
  private static final String METRIC = "job_resource_usage_cpu_all";

  @Test
  void metricsUseSharedMillisecondBoundsWithinTheObservedRun() throws IOException {
    JsonObject run = new JsonObject();
    run.addProperty("DeploymentId", "fixture");
    run.addProperty("StartTime", "2025-01-01 08:00:00");
    run.addProperty("EndTime", "2025-01-01 08:30:00");
    run.addProperty("State", "STOPPED");
    long from = VolcengineMonitor.time("2025-01-01 08:05:00") + 5;
    long until = VolcengineMonitor.time("2025-01-01 08:07:00");
    List<JsonObject> requests = new ArrayList<>();
    OkHttpClient transport = new OkHttpClient();
    transport
        .interceptors()
        .add(
            chain -> {
              Buffer body = new Buffer();
              chain.request().body().writeTo(body);
              requests.add(JsonParser.parseString(body.readUtf8()).getAsJsonObject());
              return new Response.Builder()
                  .request(chain.request())
                  .protocol(Protocol.HTTP_1_1)
                  .code(200)
                  .message("OK")
                  .body(
                      ResponseBody.create(
                          MediaType.parse("application/json"),
                          "{\"ResponseMetadata\":{\"RequestId\":\"fixture\"},\"Result\":{}}"))
                  .build();
            });
    VolcengineOpenApi api =
        new VolcengineOpenApi(
            "cn-beijing", Credentials.getCredentials("fixture-ak", "fixture-sk"), transport);

    assertThat(VolcengineMonitor.read(api, run, from, until))
        .containsEntry("from", from / 1000 + 1)
        .containsEntry("until", until / 1000 - 1);
    assertThat(requests)
        .hasSize(2)
        .allSatisfy(
            request -> {
              assertThat(request.get("StartTime").getAsLong()).isEqualTo(from / 1000 + 1);
              assertThat(request.get("EndTime").getAsLong()).isEqualTo(until / 1000 - 1);
            });

    assertThatThrownBy(() -> VolcengineMonitor.read(api, run, until, from))
        .isInstanceOf(ResponseStatusException.class);
    assertThat(requests).hasSize(2);
  }

  @Test
  void returnsOnlyVerifiedResourceAndInstanceWindow() {
    JsonObject data = new JsonObject();
    data.addProperty("Namespace", "VCM_Flink");
    data.addProperty("MetricName", METRIC);
    JsonArray series = new JsonArray();
    series.add(series("other-resource", point(200, 7)));
    series.add(series("fixture", point(199, 8), point(200, 3), point(250, 4), point(251, 9)));
    data.add("MetricDataResults", series);

    assertThat(VolcengineMonitor.points(data, METRIC, "fixture", 200, 250))
        .containsExactly(
            Map.of("timestamp", 200L, "value", new BigDecimal("3")),
            Map.of("timestamp", 250L, "value", new BigDecimal("4")));
    assertThat(VolcengineMonitor.points(data, "wrong_metric", "fixture", 200, 250)).isEmpty();
  }

  private JsonObject series(String resource, JsonObject... points) {
    JsonObject dimension = new JsonObject();
    dimension.addProperty("Name", "ResourceID");
    dimension.addProperty("Value", resource);
    JsonArray identity = new JsonArray();
    identity.add(dimension);
    JsonArray values = new JsonArray();
    for (JsonObject point : points) {
      values.add(point);
    }
    JsonObject series = new JsonObject();
    series.add("DataPoints", values);
    series.add("Dimensions", identity);
    return series;
  }

  private JsonObject point(long time, int value) {
    JsonObject point = new JsonObject();
    point.addProperty("Timestamp", time);
    point.addProperty("Value", value);
    return point;
  }
}
