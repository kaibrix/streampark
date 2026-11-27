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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Credentials;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Target;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

class VolcengineProfileBindingTest {
  private static final String ENGINE = "FLINK_VERSION_1_17";

  @Test
  void teamCredentialWritesRequireOptInAndTheExactDeploymentScope() {
    VolcengineProfileBinding binding =
        new VolcengineProfileBinding(
            "",
            9L,
            "account",
            "project",
            "directory",
            "pool",
            "Default",
            ENGINE,
            true,
            true,
            false,
            true,
            "",
            "");
    binding.setAdditionalPools("pool-2=Secondary");
    Target target = new Target();
    target.setProjectId("project");
    target.setDirectoryId("directory");
    target.setResourcePoolId("pool-2");
    target.setResourcePoolName("Secondary");
    target.setEngineRef(ENGINE);
    Credentials credential = new Credentials("test-id", "test-secret");
    Context context = new Context(9L, "cn-beijing", "account", target, credential);

    assertThat(binding.permitsSqlWrites(context)).isFalse();
    ReflectionTestUtils.setField(binding, "teamCredentialLifecycle", true);
    assertThat(binding.permitsSqlWrites(context)).isTrue();
    assertThat(binding.permitsSnapshots(context)).isTrue();
    assertThat(binding.permitsMetricReads(context)).isFalse();
    assertThat(
            binding.permitsSqlWrites(new Context(8L, "cn-beijing", "account", target, credential)))
        .isFalse();
    target.setResourcePoolName("Changed name");
    assertThat(binding.permitsSqlWrites(context)).isFalse();
    target.setResourcePoolName("Secondary");
    target.setProjectId("other-project");
    assertThat(binding.permitsSqlWrites(context)).isFalse();
  }
}
