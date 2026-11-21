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

package org.apache.streampark.console.core.entity;

import org.apache.streampark.common.util.DeflaterUtils;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.service.impl.FlinkEnvServiceImpl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlinkEnvTest {
  @TempDir Path home;

  @Test
  void readsBothConfigFileNamesInNativeDetailAndWatcher() throws Exception {
    Path conf = Files.createDirectories(home.resolve("conf"));
    String yaml = "state.savepoints.dir: file:///e09-state/$jobName\n";
    Files.write(conf.resolve("flink-conf.yaml"), yaml.getBytes(StandardCharsets.UTF_8));
    Files.write(conf.resolve("config.yaml"), yaml.getBytes(StandardCharsets.UTF_8));
    for (String version : new String[] {"1.17.2", "1.20.0"}) {
      FlinkEnv env = new FlinkEnv();
      env.setFlinkHome(home.toString());
      env.setVersion(version);
      env.doSetFlinkConf();
      Application application = new Application();
      application.setJobName("fixture");
      assertThat(new FlinkEnvServiceImpl().getFlinkConfig(env, application))
          .containsEntry("state.savepoints.dir", "file:///e09-state/fixture");
      env.unzipFlinkConf();
      assertThat(env.getFlinkConf()).isEqualTo(yaml);
    }
  }

  @Test
  void preservesLegacyCompressedConfigRows() {
    FlinkEnv env = new FlinkEnv();
    String yaml = "parallelism.default: 1\n";
    env.setFlinkConf(DeflaterUtils.zipString(yaml));

    assertThat(env.convertFlinkYamlAsMap()).containsEntry("parallelism.default", "1");
    env.unzipFlinkConf();
    assertThat(env.getFlinkConf()).isEqualTo(yaml);
  }

  @Test
  void rejectsFilePathsOutsideTheFlinkConfigurationDirectory() {
    FlinkEnv env = new FlinkEnv();
    env.setFlinkHome(home.toString());
    env.setFlinkConf(home.resolve("outside.yaml").toString());

    assertThatThrownBy(env::convertFlinkYamlAsMap).isInstanceOf(ApiAlertException.class);
  }
}
