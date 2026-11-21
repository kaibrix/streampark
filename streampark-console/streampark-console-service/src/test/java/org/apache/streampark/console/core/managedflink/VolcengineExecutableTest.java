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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.FrozenCommand;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Spec;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class VolcengineExecutableTest {
  private static final String ARTIFACT = "tos://fixture-bucket/test/immutable.jar";

  @Test
  void flink116SqlDependenciesAndOptionsMatchPublishedManifest() {
    checkSqlManifest("FLINK_VERSION_1_16");
  }

  @Test
  void flink117SqlDependenciesAndOptionsMatchPublishedManifest() {
    checkSqlManifest("FLINK_VERSION_1_17");
  }

  @Test
  void flink120SqlDependenciesAndOptionsMatchPublishedManifest() {
    checkSqlManifest("FLINK_VERSION_1_20");
  }

  private void checkSqlManifest(String engine) {
    OperationStep step = step(engine, JobType.SQL);
    step.getCommand().getSpec().setUdfArtifactId("fixture-udf");
    step.getCommand()
        .getSpec()
        .getConfiguration()
        .put(
            "pipeline.global-job-parameters",
            "acceptance.run-label:sql-smoke,acceptance.tenant:租户 beta");
    assertThat(VolcengineExecutable.supportedConfiguration(step.getCommand().getSpec())).isTrue();
    JsonObject draft = new JsonObject();
    draft.addProperty("JobType", "FLINK_STREAMING_SQL");
    draft.addProperty("EngineVersion", engine);
    VolcengineExecutable.writeDraft(draft, step);

    assertThat(VolcengineExecutable.matches(draft, step, true)).isTrue();
    JsonObject published = draft.deepCopy();
    published.add("Conf", VolcengineExecutable.settings(step));
    JsonArray jars = new JsonArray();
    jars.add(ARTIFACT);
    JsonObject dependency = new JsonObject();
    dependency.add("Jars", jars);
    published.add("Dependency", dependency);

    assertThat(VolcengineExecutable.matches(published, step, false)).isTrue();
    JsonObject running = published.deepCopy();
    JsonObject conf = running.getAsJsonObject("Conf");
    conf.addProperty("snapshot.namespace", "fixture-namespace");
    conf.addProperty("checkpoint.restore-savepoint-path", "fixture-path");
    running.addProperty("Conf", conf.toString());
    running.addProperty("VersionName", "version assigned after the first start");

    assertThat(VolcengineExecutable.revision(running))
        .isEqualTo(VolcengineExecutable.revision(published));
    assertThat(VolcengineExecutable.matches(running, step, false)).isTrue();
    JsonObject configDrift = published.deepCopy();
    configDrift.getAsJsonObject("Conf").remove("pipeline.global-job-parameters");
    assertThat(VolcengineExecutable.matches(configDrift, step, false)).isFalse();
    JsonObject changedDraft = draft.deepCopy();
    JsonObject dynamic = VolcengineExecutable.objectValue(changedDraft.get("DynamicOptions"));
    dynamic.addProperty("containerized.taskmanager.env.OUT_OF_BAND", "unexpected");
    changedDraft.addProperty("DynamicOptions", dynamic.toString());
    assertThat(VolcengineExecutable.matches(changedDraft, step, true)).isFalse();
    jars.add("tos://fixture-bucket/test/external-jar.jar");
    assertThat(VolcengineExecutable.matches(published, step, false)).isFalse();
  }

  @Test
  void rejectsMalformedVolcengineSqlGlobalParameters() {
    Spec spec = step("FLINK_VERSION_1_17", JobType.SQL).getCommand().getSpec();
    for (String encoded : Arrays.asList("run", "run:", "run:one,run:two", "run:one,two", "a:b:c")) {
      spec.getConfiguration().put("pipeline.global-job-parameters", encoded);
      assertThat(VolcengineExecutable.supportedConfiguration(spec)).as(encoded).isFalse();
    }
  }

  @Test
  void jarParametersUseCloudArgvEncodingAndDetectConfigurationDrift() {
    OperationStep step = step("FLINK_VERSION_1_17", JobType.JAR);
    JsonObject draft = new JsonObject();
    draft.addProperty("JobType", "FLINK_STREAMING_JAR");
    draft.addProperty("EngineVersion", "FLINK_VERSION_1_17");
    assertThat(VolcengineExecutable.hasSupportedArgs(step.getCommand().getSpec())).isFalse();
    step.getCommand().getSpec().getConfiguration().put("args.base64.enabled", "true");
    assertThat(VolcengineExecutable.hasSupportedArgs(step.getCommand().getSpec())).isTrue();
    VolcengineExecutable.writeDraft(draft, step);
    assertThat(
            new String(
                    java.util.Base64.getDecoder().decode(draft.get("Args").getAsString()),
                    java.nio.charset.StandardCharsets.UTF_8)
                .split("\u0000", -1))
        .containsExactly("alpha beta", "中文", "--limit=7");
    assertThat(VolcengineExecutable.matches(draft, step, true)).isTrue();

    JsonObject published = draft.deepCopy();
    published.add("Conf", VolcengineExecutable.settings(step));
    JsonObject drifted = published.deepCopy();
    drifted.getAsJsonObject("Conf").addProperty("execution.checkpointing.interval", "8 s");
    assertThat(VolcengineExecutable.matches(drifted, step, false)).isFalse();
    assertThat(VolcengineExecutable.revision(drifted))
        .isNotEqualTo(VolcengineExecutable.revision(published));
  }

  private static OperationStep step(String engine, JobType type) {
    Spec spec = new Spec();
    spec.setType(type);
    spec.setSql(type == JobType.SQL ? "SELECT 1" : null);
    spec.setMainClass(type == JobType.JAR ? "fixture.Main" : null);
    if (type == JobType.JAR) {
      spec.setArgs(Arrays.asList("alpha beta", "中文", "--limit=7"));
    }
    spec.getConfiguration().put("execution.checkpointing.interval", "4 s");
    spec.getConfiguration()
        .put("containerized.taskmanager.env.STREAM_PARK_ACCEPTANCE_TM", "fixture");
    FrozenCommand command = new FrozenCommand();
    command.setReleaseMarker("fixture-release");
    command.setEngineRef(engine);
    command.setSpec(spec);
    return new OperationStep(
        "fixture-operation", 10001L, Action.UPDATE_DRAFT, command, null, null, ARTIFACT);
  }
}
