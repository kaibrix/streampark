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

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.text;

final class VolcengineExecutable {
  private static final String RELEASE_OPTION = "pipeline.name";

  static String providerType(JobType type) {
    return type == JobType.JAR ? "FLINK_STREAMING_JAR" : "FLINK_STREAMING_SQL";
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

  static String jarArgs(List<String> args) {
    return args.stream()
        .map(value -> value.matches("[^\\s'\"]+") ? value : "\"" + value + "\"")
        .collect(Collectors.joining(" "));
  }

  static boolean supportedConfiguration(Spec spec) {
    JsonObject permittedResources = resources();
    return spec.getConfiguration().entrySet().stream()
        .allMatch(
            entry -> {
              String key = entry.getKey();
              String value = entry.getValue();
              if (permittedResources.has(key)) {
                return permittedResources.get(key).getAsString().equals(value);
              }
              if (key.startsWith("execution.checkpointing.")) {
                return value.matches("[1-9][0-9]{0,5} (ms|s|min)");
              }
              return key.startsWith("containerized.");
            });
  }

  static boolean hasSupportedArgs(Spec spec) {
    return spec.getArgs().stream()
        .noneMatch(
            value ->
                value.indexOf('"') >= 0
                    || value.indexOf('\\') >= 0
                    || value.codePoints().anyMatch(Character::isISOControl));
  }

  static void writeDraft(JsonObject draft, OperationStep step) {
    Spec spec = step.getCommand().getSpec();
    String artifact = artifact(step);
    JsonArray jars = new JsonArray();
    if (spec.getUdfArtifactId() != null) {
      jars.add(artifact);
    }
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
      draft.addProperty("Args", jarArgs(spec.getArgs()));
      draft.add("MainArgs", new JsonObject());
    }
  }

  static boolean matches(JsonObject remote, OperationStep step, boolean draft) {
    if (remote == null || step.getCommand().getReleaseMarker() == null) {
      return false;
    }
    Spec spec = step.getCommand().getSpec();
    if (!Objects.equals(providerType(spec.getType()), text(remote, "JobType"))
        || !Objects.equals(step.getCommand().getEngineRef(), text(remote, "EngineVersion"))) {
      return false;
    }
    JsonObject config = objectValue(remote.get(draft ? "DynamicOptions" : "Conf"));
    if (config == null) {
      return false;
    }
    JsonObject settings = settings(step);
    for (Map.Entry<String, JsonElement> entry : settings.entrySet()) {
      if (!Objects.equals(entry.getValue(), config.get(entry.getKey()))) {
        return false;
      }
    }
    if (draft && !empty(remote.get("Options"))) {
      return false;
    }
    String artifact = artifact(step);
    if (spec.getType() == JobType.JAR) {
      return artifact != null
          && artifact.equals(text(remote, "Jar"))
          && Objects.equals(spec.getMainClass(), text(remote, "MainClass"))
          && jarArgs(spec.getArgs()).equals(Objects.toString(text(remote, "Args"), ""))
          && matchesDependency(remote.get("Dependency"), Collections.emptyList());
    }
    return Objects.equals(spec.getSql(), text(remote, "SqlText"))
        && (spec.getUdfArtifactId() == null
            ? matchesDependency(remote.get("Dependency"), Collections.emptyList())
            : artifact != null
                && matchesDependency(
                    remote.get("Dependency"), Collections.singletonList(artifact)));
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
