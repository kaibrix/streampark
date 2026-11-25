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
import org.apache.streampark.console.core.managedflink.VolcengineOpenApi.Reply;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.check;
import static org.apache.streampark.console.core.managedflink.VolcengineMonitor.time;
import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.object;
import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.text;

@Component
final class VolcengineTlsLogs {
  private static final int PAGE_SIZE = 100;
  private static final Pattern SENSITIVE_LINE =
      Pattern.compile(
          "(?is)(?:\\b(?:authorization|credential|ak|sk|access[._ -]?key(?:[._ -]?id)?|"
              + "secret(?:[._ -]?access)?[._ -]?key|api[._ -]?key|password|signature|"
              + "(?:security|session)?[._ -]?token|x-[a-z-]*(?:token|signature|credential)|"
              + "program[._ -]?args|main[._ -]?args|argv|args|sql[._ -]?(?:text|statement)?)\\b"
              + "[\\\"']?\\s*[:=]|\\b(?:bearer\\s+|select\\s+|insert\\s+into|"
              + "create\\s+(?:temporary\\s+)?table)\\b"
              + "|--(?:password|token|access-key(?:-id)?|secret(?:-access)?-key|api-key)\\s+"
              + "|https?://[^\\s?#]+\\?[^\\s\"']+)");
  private final String region;
  private final String topic;
  private final String instanceField;
  private final String deploymentField;
  private final String componentField;
  private final String levelField;
  private final String messageField;
  private final boolean enabled;
  private final ManagedFlinkStore store;

  VolcengineTlsLogs(
      ManagedFlinkStore store,
      @Value("${streampark.managed-flink.volcengine.logs-enabled:false}") boolean enabled,
      @Value("${streampark.managed-flink.volcengine.logs-region:}") String region,
      @Value("${streampark.managed-flink.volcengine.logs-topic:}") String topic,
      @Value("${streampark.managed-flink.volcengine.logs-instance-field:}") String instanceField,
      @Value("${streampark.managed-flink.volcengine.logs-deployment-field:}")
          String deploymentField,
      @Value("${streampark.managed-flink.volcengine.logs-component-field:}") String componentField,
      @Value("${streampark.managed-flink.volcengine.logs-level-field:}") String levelField,
      @Value("${streampark.managed-flink.volcengine.logs-message-field:}") String messageField) {
    this.store = store;
    this.enabled = enabled;
    this.region = region;
    this.topic = topic;
    this.instanceField = instanceField;
    this.deploymentField = deploymentField;
    this.componentField = componentField;
    this.levelField = levelField;
    this.messageField = messageField;
  }

  boolean configured(Context context) {
    return enabled
        && region.matches("cn-[a-z0-9]+(?:-[a-z0-9]+)*")
        && topic.matches("[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}")
        && region.equals(context.getTarget().getTlsRegion())
        && topic.equals(context.getTarget().getTlsTopic())
        && Arrays.asList(instanceField, deploymentField, componentField, messageField).stream()
            .allMatch(VolcengineTlsLogs::validField)
        && Arrays.asList(instanceField, deploymentField, componentField, messageField).stream()
                .distinct()
                .count()
            == 4;
  }

  boolean permitsLevelFilter() {
    return validField(levelField)
        && !Arrays.asList(instanceField, deploymentField, componentField, messageField)
            .contains(levelField);
  }

  private static boolean validField(String field) {
    return field.matches("[a-zA-Z_][a-zA-Z0-9_.]{0,127}");
  }

  Map<String, Object> read(
      SearchApi api, Context context, String job, JsonObject run, LogQuery query)
      throws IOException {
    String runtime = text(run, "ApplicationId");
    String deployment = text(run, "DeploymentId");
    Long runStart = time(text(run, "StartTime"));
    Long runEnd = time(text(run, "EndTime"));
    if (runtime == null
        || deployment == null
        || runStart == null
        || (runEnd == null && !"RUNNING".equals(text(run, "State")))) {
      return status("NO_DATA");
    }
    String component = normalized(query.getComponent());
    String level = normalized(query.getLevel());
    String keyword = normalized(query.getKeyword());
    check(
        component.length() <= 64 && level.length() <= 16 && keyword.length() <= 256,
        HttpStatus.BAD_REQUEST,
        "Log filter is too long");
    if (!level.isEmpty() && !permitsLevelFilter()) {
      return status("NEEDS_CONFIGURATION");
    }
    String filter =
        instanceField
            + ":"
            + literal(runtime)
            + " AND "
            + deploymentField
            + ":"
            + literal(deployment);
    if (!component.isEmpty()) {
      filter += " AND " + componentField + ":" + literal(component);
    }
    if (!level.isEmpty()) {
      filter += " AND " + levelField + ":" + literal(level);
    }
    if (!keyword.isEmpty()) {
      filter += " AND " + literal(keyword);
    }

    List<Object> scope =
        Arrays.asList(
            context.getTeamId(),
            context.getAccountRef(),
            region,
            topic,
            query.getActor(),
            context.getTarget().getProjectId(),
            context.getTarget().getDirectoryId(),
            job,
            text(run, "Id"),
            runtime,
            deployment,
            filter,
            query.getFrom(),
            query.getUntil());
    String scopeHash = ManagedFlinkStore.hash(store.encode(scope));
    long now = Instant.now().toEpochMilli();
    VolcengineMonitor.Window bounds;
    String previousContext = null;
    if (query.getCursor() == null) {
      bounds = VolcengineMonitor.window(runStart, runEnd, query.getFrom(), query.getUntil(), now);
    } else {
      Map<String, Object> page =
          store.find(
              "select window_from,window_until,provider_context from t_flink_managed_log_cursor "
                  + "where id=? and scope_hash=? and expires_at>?",
              query.getCursor(),
              scopeHash,
              now);
      check(page != null, HttpStatus.BAD_REQUEST, "Invalid or expired log cursor");
      bounds =
          new VolcengineMonitor.Window(
              ManagedFlinkStore.number(page, "window_from"),
              ManagedFlinkStore.number(page, "window_until"));
      previousContext = ManagedFlinkStore.text(page, "provider_context");
    }
    long from = bounds.from;
    long until = bounds.until;
    if (from >= until) {
      return status("NO_DATA");
    }

    Reply topicInfo = api.topic(topic);
    if (!topicInfo.ok()) {
      return failure(topicInfo);
    }
    Long days = number(text(topicInfo.data(), "Ttl"));
    if (days != null && days > 0 && days <= 36500 && until < now - TimeUnit.DAYS.toMillis(days)) {
      return status("EXPIRED");
    }
    Reply schema = api.index(topic);
    if (!schema.ok()) {
      return failure(schema);
    }
    if (!exactIndex(schema.data(), instanceField)
        || !exactIndex(schema.data(), deploymentField)
        || (!component.isEmpty() && !exactIndex(schema.data(), componentField))
        || (!level.isEmpty() && !exactIndex(schema.data(), levelField))) {
      return status("NEEDS_CONFIGURATION");
    }
    Reply response = api.search(request(topic, filter, from, until, previousContext));
    if (!response.ok()) {
      return failure(response);
    }

    Map<String, Object> result = new LinkedHashMap<>();
    result.put("source", "VOLCENGINE_TLS");
    JsonObject data = response.data();
    String state = text(data, "ResultStatus");
    if (!Arrays.asList("complete", "incomplete", "time_out").contains(state)
        || !"false".equals(text(data, "Analysis"))) {
      return withRequestId(status("STALE"), response);
    }
    List<Map<String, Object>> rows =
        rows(data, runtime, deployment, component, level, keyword, from, until);
    String contextToken = text(data, "Context");
    if ("false".equals(text(data, "ListOver"))
        && contextToken != null
        && !contextToken.isEmpty()
        && contextToken.length() <= 2048
        && !contextToken.equals(previousContext)) {
      String token = ManagedFlinkStore.id();
      long issuedAt = System.currentTimeMillis();
      store.jdbc().update("delete from t_flink_managed_log_cursor where expires_at<=?", issuedAt);
      store
          .jdbc()
          .update(
              "insert into t_flink_managed_log_cursor (id,scope_hash,window_from,window_until,provider_context,expires_at) "
                  + "values (?,?,?,?,?,?)",
              token,
              scopeHash,
              from,
              until,
              contextToken,
              issuedAt + TimeUnit.MINUTES.toMillis(5));
      result.put("nextCursor", token);
    }
    result.put("rows", rows);
    result.put("from", from);
    result.put("until", until);
    result.put(
        "status",
        "time_out".equals(state)
            ? "TIMEOUT"
            : "incomplete".equals(state)
                ? "PARTIAL"
                : rows.isEmpty() && !result.containsKey("nextCursor") ? "NO_DATA" : "AVAILABLE");
    return withRequestId(result, response);
  }

  static JsonObject request(String topic, String query, long from, long until, String context) {
    JsonObject body = new JsonObject();
    body.addProperty("TopicId", topic);
    body.addProperty("Query", query);
    body.addProperty("StartTime", from);
    body.addProperty("EndTime", until);
    body.addProperty("Limit", PAGE_SIZE);
    body.addProperty("Sort", "desc");
    body.addProperty("HighLight", false);
    if (context != null) {
      body.addProperty("Context", context);
    }
    return body;
  }

  static String literal(String value) {
    check(
        value.indexOf('|') == -1 && value.chars().noneMatch(Character::isISOControl),
        HttpStatus.BAD_REQUEST,
        "Unsupported character in log filter");
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  List<Map<String, Object>> rows(
      JsonObject data,
      String runtime,
      String deployment,
      String component,
      String level,
      String keyword,
      long from,
      long until) {
    List<Map<String, Object>> result = new ArrayList<>();
    boolean includeLevel = permitsLevelFilter();
    String needle = keyword.toLowerCase(Locale.ROOT);
    for (JsonElement element : array(data, "Logs")) {
      if (!element.isJsonObject()) {
        continue;
      }
      JsonObject row = element.getAsJsonObject();
      Long at = number(text(row, "__time__"));
      String message = text(row, messageField);
      String role = text(row, componentField);
      String severity = includeLevel ? text(row, levelField) : null;
      if (at == null
          || at < from
          || at >= until
          || message == null
          || !runtime.equals(text(row, instanceField))
          || !deployment.equals(text(row, deploymentField))
          || (!level.isEmpty() && !level.equals(severity))
          || (!component.isEmpty() && !component.equals(role))) {
        continue;
      }
      String visibleMessage = safeMessage(message);
      if (!needle.isEmpty()
          && ("[redacted]".equals(visibleMessage)
              || !visibleMessage.toLowerCase(Locale.ROOT).contains(needle))) {
        continue;
      }
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("timestamp", at);
      entry.put("component", role != null && role.matches("[A-Za-z0-9_.-]{1,64}") ? role : null);
      if (severity != null && severity.matches("[A-Za-z]{1,16}")) {
        entry.put("level", severity);
      }
      entry.put("message", visibleMessage);
      result.add(entry);
      if (result.size() == PAGE_SIZE) {
        break;
      }
    }
    return result;
  }

  static String safeMessage(String message) {
    String excerpt = message.substring(0, Math.min(message.length(), 8192));
    if (SENSITIVE_LINE.matcher(excerpt).find()) {
      return "[redacted]";
    }
    return excerpt;
  }

  static boolean exactIndex(JsonObject schema, String field) {
    for (JsonElement element : array(schema, "KeyValue")) {
      if (element.isJsonObject()) {
        JsonObject key = element.getAsJsonObject();
        JsonObject value = object(key, "Value");
        if (field.equals(text(key, "Key"))
            && "text".equals(text(value, "ValueType"))
            && "".equals(text(value, "Delimiter"))
            && "true".equals(text(value, "CaseSensitive"))) {
          return true;
        }
      }
    }
    return false;
  }

  private static JsonArray array(JsonObject root, String field) {
    JsonElement value = root.get(field);
    return value != null && value.isJsonArray() ? value.getAsJsonArray() : new JsonArray();
  }

  private static Long number(String value) {
    try {
      return value == null ? null : Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  private static String normalized(String value) {
    return value == null ? "" : value.trim();
  }

  private static Map<String, Object> failure(Reply response) {
    String status;
    switch (response.httpStatus) {
      case 401:
      case 403:
        status = "NO_ACCESS";
        break;
      case 404:
        status = "NEEDS_CONFIGURATION";
        break;
      case 429:
        status = "THROTTLED";
        break;
      default:
        status = "STALE";
    }
    return withRequestId(status(status), response);
  }

  static Map<String, Object> status(String status) {
    return Collections.singletonMap("status", status);
  }

  interface SearchApi {
    Reply topic(String topic) throws IOException;

    Reply index(String topic) throws IOException;

    Reply search(JsonObject body) throws IOException;
  }

  private static Map<String, Object> withRequestId(Map<String, Object> result, Reply reply) {
    String id = reply.requestId;
    if (id == null || !id.matches("[a-zA-Z0-9_-]{6,128}")) {
      return result;
    }
    Map<String, Object> copy = new LinkedHashMap<>(result);
    copy.put("requestIdSuffix", id.substring(id.length() - 6));
    return copy;
  }
}
