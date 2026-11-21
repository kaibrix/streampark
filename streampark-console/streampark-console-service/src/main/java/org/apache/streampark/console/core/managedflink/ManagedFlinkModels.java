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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import lombok.Value;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ManagedFlinkModels {

  private ManagedFlinkModels() {}

  public enum JobType {
    SQL,
    JAR
  }

  public enum CommandType {
    PUBLISH,
    ROLLBACK,
    START,
    STOP,
    SNAPSHOT
  }

  public enum Action {
    STAGE_ARTIFACT,
    CREATE_DRAFT,
    LOCK_DRAFT,
    UPDATE_DRAFT,
    PUBLISH,
    START,
    STOP,
    SNAPSHOT
  }

  public enum RestoreMode {
    FROM_NEW,
    FROM_LATEST,
    FROM_SNAPSHOT
  }

  public enum Outcome {
    CONFIRMED,
    WAITING,
    REJECTED,
    UNKNOWN
  }

  @Data
  public static class Credentials {
    private final String accessKey;
    private final String secretKey;

    @JsonCreator
    public Credentials(
        @JsonProperty("accessKey") String accessKey, @JsonProperty("secretKey") String secretKey) {
      this.accessKey = accessKey;
      this.secretKey = secretKey;
    }

    @Override
    public String toString() {
      return "Credentials[redacted]";
    }
  }

  @Data
  public static class ConnectionInput {
    private String name;
    private String provider;
    private String accountRef;
    @ToString.Exclude private String accessKey;
    @ToString.Exclude private String secretKey;
  }

  @Data
  public static class EnvironmentInput {
    private String name;
    private String connectionId;
    private String region;
    private Target target;
    private Boolean enabled;
    private Integer expectedConfigVersion;
  }

  @Data
  public static class Target {
    private String projectId;
    private String directoryId;
    private String resourcePoolId;
    private String resourcePoolName;
    private String engineRef;
    private String artifactBucket;
    private String artifactPrefix;
    private String tlsTopic;
    private String tlsRegion;
  }

  @Value
  public static class LogQuery {
    long actor;
    Long from;
    Long until;
    String component;
    String level;
    String keyword;
    String cursor;
  }

  @Data
  public static class JobInput {
    private String name;
    private String environmentId;
    private Long expectedDraftRevision;
    private Spec spec;
  }

  @Data
  public static class Spec {
    private JobType type;
    private String sql;
    private String artifactId;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String udfArtifactId;

    private String mainClass;
    private List<String> args = new ArrayList<>();
    private Map<String, String> configuration = new LinkedHashMap<>();
  }

  @Data
  public static class Restore {
    private RestoreMode mode;
    private String snapshotId;
    private boolean confirmedNew;
  }

  @Data
  public static class Command {
    private Long draftRevision;
    private Integer expectedEnvVersion;
    private String expectedReleaseId;
    private String sourceReleaseId;
    private String expectedRunId;
    private Restore restore;
    private String mode;
    private String description;
  }

  @Data
  public static class FrozenCommand {
    private Command request;
    private List<Action> actions;
    private Spec spec;
    private Target target;
    private String specHash;
    private String engineRef;
    private String jobName;
    private String providerVersion;
    private String providerInstance;
    private String restoreRef;
    private String artifactRef;
    private String artifactDigest;
    private Long artifactSize;
    private String releaseMarker;
  }

  @Data
  public static class Context {
    private final long teamId;
    private final String region;
    private final String accountRef;
    private final Target target;
    private final Credentials credentials;
    private final boolean serverCredential;

    public Context(String region, String accountRef, Target target, Credentials credentials) {
      this(-1, region, accountRef, target, credentials);
    }

    public Context(
        long teamId, String region, String accountRef, Target target, Credentials credentials) {
      this(teamId, region, accountRef, target, credentials, credentials == null);
    }

    public Context(
        long teamId,
        String region,
        String accountRef,
        Target target,
        Credentials credentials,
        boolean serverCredential) {
      this.teamId = teamId;
      this.region = region;
      this.accountRef = accountRef;
      this.target = target;
      this.credentials = credentials;
      this.serverCredential = serverCredential;
    }

    @Override
    public String toString() {
      return "ManagedFlinkContext[" + region + "]";
    }
  }

  @Data
  public static class OperationStep {
    private final String operationId;
    private final Long appId;
    private final Action action;
    private final FrozenCommand command;
    private final String draftId;
    private final String providerJobId;
    private final String stagedArtifact;
  }

  public enum ReadStatus {
    FOUND,
    MISSING,
    STALE,
    AUTH_ERROR,
    THROTTLED
  }

  @Value
  @AllArgsConstructor
  public static class CloudState {
    ReadStatus status;
    String jobId;
    String version;
    String instanceId;
    String runtimeId;
    String runState;
    long startedAt;
    long retryNotBefore;

    public CloudState(
        ReadStatus status,
        String jobId,
        String version,
        String instanceId,
        String runtimeId,
        String runState,
        long startedAt) {
      this(status, jobId, version, instanceId, runtimeId, runState, startedAt, 0);
    }
  }

  @Data
  public static class Observation {
    private Outcome outcome;
    private String requestId;
    private String code;
    private String draftId;
    private String jobId;
    private String version;
    private String remoteVersionId;
    private String specHash;
    private String instanceId;
    private String runtimeId;
    private String runState;
    private String recordId;
    private String restoreRef;
    private String artifactRef;
    private String restoredFrom;

    public static Observation outcome(Outcome outcome, String code) {
      Observation result = new Observation();
      result.setOutcome(outcome);
      result.setCode(code);
      return result;
    }
  }
}
