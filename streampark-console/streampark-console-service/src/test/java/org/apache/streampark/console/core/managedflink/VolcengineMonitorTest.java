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
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class VolcengineMonitorTest {
  private static final String METRIC = "job_resource_usage_cpu_all";

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
