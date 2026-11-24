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

import org.apache.streampark.console.core.managedflink.VolcengineOpenApi.Reply;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.check;
import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.object;
import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.text;

final class VolcengineMonitor {
  private static final List<String> USAGE =
      Arrays.asList("job_resource_usage_cpu_all", "job_resource_usage_memory_all");
  private static final DateTimeFormatter CLOUD_TIME =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private static final ZoneId BEIJING = ZoneId.of("Asia/Shanghai");
  private static final String NAMESPACE = "VCM_Flink";
  private static final long DEFAULT_WINDOW = TimeUnit.MINUTES.toMillis(15);
  private static final long MAX_WINDOW = TimeUnit.HOURS.toMillis(24);

  private VolcengineMonitor() {}

  static Map<String, Object> status(String status) {
    return Collections.singletonMap("status", status);
  }

  static Map<String, Object> read(VolcengineOpenApi api, JsonObject run, Long lower, Long upper)
      throws IOException {
    String deployment = text(run, "DeploymentId");
    Long start = time(text(run, "StartTime"));
    Long end = time(text(run, "EndTime"));
    if (deployment == null || start == null) {
      return status("NO_DATA");
    }
    if (end == null && !"RUNNING".equals(text(run, "State"))) {
      return status("NO_DATA");
    }
    Window window = window(start, end, lower, upper, Instant.now().toEpochMilli());
    if (window.from >= window.until) {
      return status("NO_DATA");
    }
    // Cloud Monitor uses epoch seconds. Keep its points inside the requested millisecond window.
    long from = (window.from + 999) / 1000;
    long until = (window.until - 1) / 1000;
    if (from > until) {
      return status("NO_DATA");
    }
    List<Map<String, Object>> series = new ArrayList<>();
    for (String metric : USAGE) {
      Reply response = api.metric(request(metric, deployment, from, until));
      if (response.httpStatus == 401
          || response.httpStatus == 403
          || "AccessDenied".equals(response.errorCode)) {
        return status("NO_ACCESS");
      }
      if (!response.ok()) {
        return status("STALE");
      }
      JsonObject data = object(response.data(), "Data");
      List<Map<String, Object>> points = points(data, metric, deployment, from, until);
      if (!points.isEmpty()) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("name", metric);
        entry.put("unit", text(data, "Unit"));
        entry.put("points", points);
        series.add(entry);
      }
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("status", series.isEmpty() ? "NO_DATA" : "AVAILABLE");
    result.put("source", "VOLCENGINE_CLOUD_MONITOR");
    result.put("from", from);
    result.put("until", until);
    result.put("series", series);
    return result;
  }

  static List<Map<String, Object>> points(
      JsonObject data, String metric, String deployment, long from, long until) {
    List<Map<String, Object>> result = new ArrayList<>();
    if (!NAMESPACE.equals(text(data, "Namespace")) || !metric.equals(text(data, "MetricName"))) {
      return result;
    }
    for (JsonElement series : array(data, "MetricDataResults")) {
      if (!series.isJsonObject()) {
        continue;
      }
      JsonObject entry = series.getAsJsonObject();
      JsonArray dimensions = array(entry, "Dimensions");
      if (dimensions.size() != 1 || !dimensions.get(0).isJsonObject()) {
        continue;
      }
      JsonObject dimension = dimensions.get(0).getAsJsonObject();
      if (!"ResourceID".equals(text(dimension, "Name"))
          || !deployment.equals(text(dimension, "Value"))) {
        continue;
      }
      for (JsonElement element : array(entry, "DataPoints")) {
        if (!element.isJsonObject()) {
          continue;
        }
        JsonObject point = element.getAsJsonObject();
        JsonElement time = point.get("Timestamp");
        JsonElement value = point.get("Value");
        if (time == null
            || !time.isJsonPrimitive()
            || !time.getAsJsonPrimitive().isNumber()
            || value == null
            || !value.isJsonPrimitive()
            || !value.getAsJsonPrimitive().isNumber()) {
          continue;
        }
        long timestamp = time.getAsLong();
        if (timestamp >= from && timestamp <= until) {
          Map<String, Object> row = new LinkedHashMap<>();
          row.put("timestamp", timestamp);
          row.put("value", value.getAsBigDecimal());
          result.add(row);
        }
      }
    }
    return result;
  }

  private static JsonObject request(String metric, String deployment, long start, long end) {
    JsonObject resource = new JsonObject();
    resource.addProperty("Name", "ResourceID");
    resource.addProperty("Value", deployment);
    JsonArray dimensions = new JsonArray();
    dimensions.add(resource);
    JsonObject scoped = new JsonObject();
    scoped.add("Dimensions", dimensions);
    JsonArray instances = new JsonArray();
    instances.add(scoped);
    JsonObject input = new JsonObject();
    input.addProperty("Namespace", NAMESPACE);
    input.addProperty("SubNamespace", "resource");
    input.addProperty("MetricName", metric);
    input.addProperty("StartTime", start);
    input.addProperty("EndTime", end);
    input.add("Instances", instances);
    return input;
  }

  private static JsonArray array(JsonObject object, String field) {
    JsonElement value = object.get(field);
    return value != null && value.isJsonArray() ? value.getAsJsonArray() : new JsonArray();
  }

  static long epoch(String time) {
    return LocalDateTime.parse(time, CLOUD_TIME).atZone(BEIJING).toEpochSecond();
  }

  static Long time(String value) {
    return StringUtils.isBlank(value) ? null : TimeUnit.SECONDS.toMillis(epoch(value));
  }

  static Window window(long start, Long end, Long lower, Long upper, long now) {
    check(
        (lower == null || lower >= 0)
            && (upper == null || upper >= 0)
            && (lower == null || upper == null || lower < upper),
        HttpStatus.BAD_REQUEST,
        "Invalid diagnostic window");
    long until = end == null ? now : Math.min(end, now);
    if (upper != null) {
      until = Math.min(until, upper);
    }
    long from = Math.max(start, lower == null ? until - DEFAULT_WINDOW : lower);
    check(until - from <= MAX_WINDOW, HttpStatus.BAD_REQUEST, "Diagnostic window exceeds 24 hours");
    return new Window(from, until);
  }

  static final class Window {
    final long from;
    final long until;

    Window(long from, long until) {
      this.from = from;
      this.until = until;
    }
  }
}
