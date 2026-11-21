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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Target;

import com.volcengine.ApiException;
import com.volcengine.auth.CLIConfigCredentialProvider;
import com.volcengine.auth.CredentialProvider;
import com.volcengine.auth.CredentialValue;
import com.volcengine.sign.Credentials;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Component
final class VolcengineProfileBinding {
  private final CredentialProvider provider;
  private final long team;
  private final String account;
  private final String project;
  private final String directory;
  private final String pool;
  private final String poolName;
  private final List<String> engines;
  private final boolean sqlWrites;
  private final boolean snapshots;
  private final boolean artifacts;
  private final String artifactBucket;
  private final String artifactPrefix;

  VolcengineProfileBinding(
      @Value("${streampark.managed-flink.volcengine.profile:}") String profile,
      @Value("${streampark.managed-flink.volcengine.team-id:-1}") long team,
      @Value("${streampark.managed-flink.volcengine.account:}") String account,
      @Value("${streampark.managed-flink.volcengine.project-id:}") String project,
      @Value("${streampark.managed-flink.volcengine.directory-id:}") String directory,
      @Value("${streampark.managed-flink.volcengine.pool-id:}") String pool,
      @Value("${streampark.managed-flink.volcengine.pool-name:}") String poolName,
      @Value("${streampark.managed-flink.volcengine.engine:}") String engine,
      @Value("${streampark.managed-flink.volcengine.sql-lifecycle-enabled:false}")
          boolean sqlWrites,
      @Value("${streampark.managed-flink.volcengine.snapshot-lifecycle-enabled:false}")
          boolean snapshots,
      @Value("${streampark.managed-flink.volcengine.artifact-lifecycle-enabled:false}")
          boolean artifacts,
      @Value("${streampark.managed-flink.volcengine.artifact-bucket:}") String artifactBucket,
      @Value("${streampark.managed-flink.volcengine.artifact-prefix:}") String artifactPrefix) {
    provider =
        profile.isEmpty() ? null : new CredentialProvider(new CLIConfigCredentialProvider(profile));
    this.team = team;
    this.account = account;
    this.project = project;
    this.directory = directory;
    this.pool = pool;
    this.poolName = poolName;
    this.engines = Arrays.asList(engine.trim().split("\\s*,\\s*"));
    this.sqlWrites = sqlWrites;
    this.snapshots = snapshots;
    this.artifacts = artifacts;
    this.artifactBucket = artifactBucket;
    this.artifactPrefix = artifactPrefix;
  }

  boolean permitsConnection(long team, String account) {
    return provider != null
        && this.team > 0
        && this.team == team
        && !this.account.isEmpty()
        && this.account.equals(account)
        && !project.isEmpty()
        && !directory.isEmpty()
        && !pool.isEmpty()
        && engines.stream().anyMatch(value -> !value.isEmpty());
  }

  boolean permits(Context context) {
    Target t = context.getTarget();
    return permitsConnection(context.getTeamId(), context.getAccountRef())
        && "cn-beijing".equals(context.getRegion())
        && Objects.equals(project, t.getProjectId())
        && Objects.equals(directory, t.getDirectoryId())
        && Objects.equals(pool, t.getResourcePoolId())
        && !poolName.isEmpty()
        && poolName.equals(t.getResourcePoolName())
        && engines.contains(t.getEngineRef());
  }

  boolean permitsSqlWrites(Context context) {
    return sqlWrites
        && context.isServerCredential()
        && context.getCredentials() == null
        && permits(context);
  }

  boolean permitsSnapshots(Context context) {
    return snapshots && permitsSqlWrites(context);
  }

  boolean permitsArtifacts(Context context) {
    return artifacts
        && permitsSqlWrites(context)
        && artifactBucket.matches("[a-z0-9][a-z0-9-]{1,61}[a-z0-9]")
        && artifactPrefix.matches("[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*")
        && artifactBucket.equals(context.getTarget().getArtifactBucket())
        && artifactPrefix.equals(context.getTarget().getArtifactPrefix());
  }

  synchronized Credentials credentials(Context context) throws ApiException {
    if (context.getCredentials() != null) {
      return Credentials.getCredentials(
          context.getCredentials().getAccessKey(), context.getCredentials().getSecretKey());
    }
    if (!permits(context)) {
      return null;
    }
    CredentialValue value = provider.get();
    return Credentials.getCredentials(value.getAk(), value.getSk(), value.getSessionToken());
  }
}
