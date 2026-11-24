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

import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Spec;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.text;

final class VolcengineExecutable {
  private static final String RELEASE_OPTION = "pipeline.name";
  private static final String BASE64_ARGS = "args.base64.enabled";

  static String providerType(Spec spec) {
    return (spec.isBatchMode() ? "FLINK_BATCH_" : "FLINK_STREAMING_") + spec.getType().name();
  }

  static String schedulePolicy(Spec spec) {
    return spec.getSchedulePolicy() == null
        ? (spec.isBatchMode() ? "DRF" : "GANG")
        : spec.getSchedulePolicy();
  }

  static boolean matchesScheduling(JsonObject remote, Spec spec) {
    if (remote == null) {
      return false;
    }
    JsonObject deployment = VolcengineOpenApi.object(remote, "DeployRequest");
    return (spec.getPriority() == null
            || spec.getPriority().toString().equals(text(deployment, "Priority")))
        && ((!spec.isBatchMode() && spec.getSchedulePolicy() == null)
            || schedulePolicy(spec).equals(text(deployment, "SchedulePolicy")))
        && (spec.getScheduleTimeout() == null
            || spec.getScheduleTimeout().toString().equals(text(deployment, "ScheduleTimeout")));
  }

  static JsonObject resources() {
    JsonObject config = new JsonObject();
    config.addProperty("jobmanager.memory.process.size", "2048mb");
    config.addProperty("taskmanager.memory.process.size", "2048mb");
    config.addProperty("kubernetes.jobmanager.cpu", "1");
    config.addProperty("kubernetes.taskmanager.cpu", "1");
    config.addProperty("parallelism.default", "1");
    config.addProperty("taskmanager.numberOfTaskSlots", "1");
    return config;
  }

  static JsonObject settings(OperationStep step) {
    JsonObject settings = resources();
    step.getCommand().getSpec().getConfiguration().forEach(settings::addProperty);
    settings.addProperty(RELEASE_OPTION, releaseMarker(step));
    return settings;
  }

  private static String releaseMarker(OperationStep step) {
    return "streampark-release-" + step.getCommand().getReleaseMarker();
  }

  static String artifact(OperationStep step) {
    return step.getStagedArtifact() == null
        ? step.getCommand().getArtifactRef()
        : step.getStagedArtifact();
  }

  static String jarArgs(Spec spec) {
    if (spec.getArgs().isEmpty()) {
      return "";
    }
    if (base64Arguments(spec)) {
      String payload = String.join("\u0000", spec.getArgs());
      return Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    }
    return String.join(" ", spec.getArgs());
  }

  private static boolean base64Arguments(Spec spec) {
    return "true".equals(spec.getConfiguration().get(BASE64_ARGS));
  }

  static boolean supportedConfiguration(Spec spec) {
    JsonObject permittedResources = resources();
    return spec.getConfiguration().entrySet().stream()
        .allMatch(
            entry -> {
              String key = entry.getKey();
              String value = entry.getValue();
              if (permittedResources.has(key)) {
                return ManagedFlinkJobs.validRuntimeSetting(key, value);
              }
              if (key.startsWith("execution.checkpointing.")) {
                return value.matches("[1-9][0-9]{0,5} (ms|s|min)");
              }
              if ("pipeline.global-job-parameters".equals(key)) {
                return spec.getType() == JobType.SQL && globalJobParameters(value);
              }
              return key.startsWith("containerized.")
                  || (BASE64_ARGS.equals(key) && spec.getType() == JobType.JAR);
            });
  }

  private static boolean globalJobParameters(String encoded) {
    // Use the common map syntax understood by all three supported engines.
    Set<String> keys = new HashSet<>();
    for (String entry : encoded.split(",", -1)) {
      int separator = entry.indexOf(':');
      if (separator == -1 || separator != entry.lastIndexOf(':')) {
        return false;
      }
      String key = entry.substring(0, separator);
      String value = entry.substring(separator + 1);
      if (!key.matches("[A-Za-z_][A-Za-z0-9_.-]{0,63}")
          || !keys.add(key)
          || value.isEmpty()
          || !value.equals(value.trim())
          || value.indexOf('\'') != -1
          || value.indexOf('"') != -1) {
        return false;
      }
    }
    return true;
  }

  static boolean hasSupportedArgs(Spec spec) {
    if (spec.getArgs().size() == 1 && spec.getArgs().get(0).isEmpty()) {
      return false;
    }
    return base64Arguments(spec)
        || spec.getArgs().stream()
            .allMatch(
                value -> !value.isEmpty() && value.codePoints().noneMatch(Character::isWhitespace));
  }

  static List<String> dependencies(OperationStep step) {
    List<String> refs = new ArrayList<>();
    if (step.getCommand().getSpec().getUdfArtifactId() != null) {
      refs.add(artifact(step));
    }
    step.getCommand().getDependencies().forEach(dep -> refs.add(dep.getRemoteRef()));
    return refs;
  }

  static void writeDraft(JsonObject draft, OperationStep step) {
    Spec spec = step.getCommand().getSpec();
    String artifact = artifact(step);
    JsonArray jars = new JsonArray();
    dependencies(step).forEach(jars::add);
    JsonObject dependency = new JsonObject();
    dependency.add("jars", jars);
    dependency.add("dependencyVersions", new JsonObject());
    dependency.add("archives", new JsonArray());
    dependency.add("pyLibraries", new JsonArray());
    draft.addProperty("Dependency", dependency.toString());
    draft.addProperty("Options", "{}");
    draft.addProperty("DynamicOptions", settings(step).toString());
    if (spec.getType() == JobType.SQL) {
      draft.addProperty("SqlText", spec.getSql());
    } else {
      draft.addProperty("Jar", artifact);
      draft.addProperty("MainClass", spec.getMainClass());
      draft.addProperty("Args", jarArgs(spec));
      draft.add("MainArgs", new JsonObject());
    }
  }

  static boolean matches(JsonObject remote, OperationStep step, boolean draft) {
    if (remote == null || step.getCommand().getReleaseMarker() == null) {
      return false;
    }
    Spec spec = step.getCommand().getSpec();
    if (!Objects.equals(providerType(spec), text(remote, "JobType"))
        || !Objects.equals(step.getCommand().getEngineRef(), text(remote, "EngineVersion"))) {
      return false;
    }
    JsonObject config = objectValue(remote.get(draft ? "DynamicOptions" : "Conf"));
    if (config == null) {
      return false;
    }
    JsonObject observed = config.deepCopy();
    if (!draft) {
      observed.remove("snapshot.namespace");
      observed.remove("checkpoint.restore-savepoint-path");
    }
    if (!settings(step).equals(observed) || (draft && !empty(remote.get("Options")))) {
      return false;
    }
    String artifact = artifact(step);
    List<String> jars = dependencies(step);
    if (jars.contains(null) || !matchesDependency(remote.get("Dependency"), jars)) {
      return false;
    }
    if (spec.getType() == JobType.JAR) {
      return artifact != null
          && artifact.equals(text(remote, "Jar"))
          && Objects.equals(spec.getMainClass(), text(remote, "MainClass"))
          && jarArgs(spec).equals(Objects.toString(text(remote, "Args"), ""));
    }
    return Objects.equals(spec.getSql(), text(remote, "SqlText"));
  }

  private static boolean matchesDependency(JsonElement remote, List<String> jars) {
    if (empty(remote)) {
      return jars.isEmpty();
    }
    JsonObject dependency = objectValue(remote);
    if (dependency == null) {
      return false;
    }
    JsonArray expected = new JsonArray();
    jars.forEach(expected::add);
    JsonElement jarList = null;
    for (Map.Entry<String, JsonElement> entry : dependency.entrySet()) {
      if ("jars".equalsIgnoreCase(entry.getKey())) {
        if (jarList != null) {
          return false;
        }
        jarList = entry.getValue();
      } else if (!empty(entry.getValue())) {
        return false;
      }
    }
    return jars.isEmpty() ? empty(jarList) : expected.equals(jarList);
  }

  static String revision(JsonObject remote) {
    if (remote == null) {
      return null;
    }
    JsonObject signature = new JsonObject();
    for (String field :
        new String[] {
          "JobType", "EngineVersion", "SqlText", "Jar", "MainClass", "Args", "ResourceVersion"
        }) {
      String value = text(remote, field);
      if (value != null && !value.isEmpty()) {
        signature.addProperty(field, value);
      }
    }
    JsonObject dependency = objectValue(remote.get("Dependency"));
    if (dependency != null) {
      JsonObject canonical = new JsonObject();
      Map<String, JsonElement> sortedDependencies = new TreeMap<>();
      dependency
          .entrySet()
          .forEach(
              entry ->
                  sortedDependencies.put(
                      entry.getKey().toLowerCase(Locale.ROOT), entry.getValue()));
      sortedDependencies.forEach(
          (key, value) -> {
            if (!empty(value)) {
              canonical.add(key, value);
            }
          });
      if (canonical.size() != 0) {
        signature.add("Dependency", canonical);
      }
    }
    JsonObject conf = objectValue(remote.get("Conf"));
    if (conf == null) {
      return null;
    }
    // Startup can change the displayed version label. The released configuration and release
    // marker are present even before the first start.
    Map<String, JsonElement> sorted = new TreeMap<>();
    for (Map.Entry<String, JsonElement> entry : conf.entrySet()) {
      if (!"snapshot.namespace".equals(entry.getKey())
          && !"checkpoint.restore-savepoint-path".equals(entry.getKey())) {
        sorted.put(entry.getKey(), entry.getValue());
      }
    }
    JsonObject stableConf = new JsonObject();
    sorted.forEach(stableConf::add);
    signature.add("Conf", stableConf);
    return ManagedFlinkStore.hash(signature.toString());
  }

  static JsonObject objectValue(JsonElement value) {
    if (value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isString()) {
      value = JsonParser.parseString(value.getAsString());
    }
    return value != null && value.isJsonObject() ? value.getAsJsonObject() : null;
  }

  private static boolean empty(JsonElement value) {
    if (value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isString()) {
      value = JsonParser.parseString(value.getAsString());
    }
    return value == null
        || value.isJsonNull()
        || (value.isJsonObject() && value.getAsJsonObject().size() == 0)
        || (value.isJsonArray() && value.getAsJsonArray().size() == 0);
  }

  private VolcengineExecutable() {}
}
