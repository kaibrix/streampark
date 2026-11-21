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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Context;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Outcome;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@ConditionalOnProperty(name = "streampark.managed-flink.mock-enabled", havingValue = "true")
class MockManagedFlinkProvider implements ManagedFlinkProvider {

  private final Map<String, Observation> applied = new ConcurrentHashMap<>();

  @Override
  public String id() {
    return "mock";
  }

  @Override
  public boolean requiresCredentials() {
    return false;
  }

  @Override
  public Map<String, String> capabilities(Context context) {
    Map<String, String> result = new LinkedHashMap<>();
    for (String feature :
        Arrays.asList(
            "IDENTITY",
            "PUBLISH_SQL",
            "PUBLISH_JAR",
            "START",
            "STOP",
            "SNAPSHOT",
            "SNAPSHOT_THEN_STOP",
            "RESTORE_FROM_SNAPSHOT",
            "RESTORE_FROM_LATEST")) {
      result.put(feature, "AVAILABLE");
    }
    result.put("LOG_SEARCH", "NEEDS_CONFIGURATION");
    result.put("METRICS", "NEEDS_CONFIGURATION");
    return result;
  }

  @Override
  public Observation dispatch(Context context, OperationStep step) {
    Observation recorded = createResult(step);
    if (applied.putIfAbsent(key(step), recorded) != null) {
      return Observation.outcome(Outcome.REJECTED, "DUPLICATE_PROVIDER_DISPATCH");
    }
    Observation ack = Observation.outcome(Outcome.WAITING, null);
    ack.setRequestId(recorded.getRequestId());
    return ack;
  }

  @Override
  public Observation observe(Context context, OperationStep step, Observation acknowledgement) {
    Observation observed = applied.get(key(step));
    return observed == null ? Observation.outcome(Outcome.UNKNOWN, "EVIDENCE_PENDING") : observed;
  }

  private Observation createResult(OperationStep step) {
    Observation result = Observation.outcome(Outcome.CONFIRMED, null);
    result.setRequestId("mock-" + step.getOperationId());
    String cmd = step.getOperationId();
    switch (step.getAction()) {
      case STAGE_ARTIFACT:
        result.setArtifactRef(step.getCommand().getArtifactRef());
        break;
      case CREATE_DRAFT:
        result.setDraftId("draft-" + step.getAppId());
        break;
      case UPDATE_DRAFT:
        result.setDraftId(step.getDraftId());
        result.setSpecHash(step.getCommand().getSpecHash());
        break;
      case PUBLISH:
        result.setJobId("job-" + step.getAppId());
        result.setVersion("version-" + cmd);
        result.setSpecHash(step.getCommand().getSpecHash());
        break;
      case START:
        result.setJobId(step.getProviderJobId());
        result.setInstanceId("instance-" + cmd);
        result.setRuntimeId("runtime-" + cmd);
        result.setVersion(step.getCommand().getProviderVersion());
        result.setRestoredFrom(step.getCommand().getRestoreRef());
        result.setRunState("RUNNING");
        break;
      case STOP:
        result.setJobId(step.getProviderJobId());
        result.setInstanceId(step.getCommand().getProviderInstance());
        result.setRunState("STOPPED");
        break;
      case SNAPSHOT:
        result.setJobId(step.getProviderJobId());
        result.setInstanceId(step.getCommand().getProviderInstance());
        result.setRecordId("record-" + cmd);
        result.setRestoreRef("restore-" + cmd);
        break;
      default:
        throw new IllegalStateException("Unsupported mock action");
    }
    return result;
  }

  private String key(OperationStep step) {
    return step.getOperationId() + ":" + step.getAction();
  }

  @Component
  @ConditionalOnProperty(name = "streampark.managed-flink.mock-enabled", havingValue = "true")
  static class AlibabaFixture extends MockManagedFlinkProvider {

    @Override
    public String id() {
      return "alibaba-fixture";
    }

    @Override
    public List<Action> publishSteps(JobType type, boolean hasDraft) {
      if (type == JobType.JAR) {
        return Arrays.asList(Action.STAGE_ARTIFACT, Action.PUBLISH);
      }
      return super.publishSteps(type, hasDraft);
    }
  }
}
