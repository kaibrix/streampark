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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Outcome;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Target;

import com.volcengine.ApiClient;
import com.volcengine.ApiException;
import com.volcengine.flink20250101.Flink20250101Api;
import com.volcengine.flink20250101.model.ListGMCSResourcesRequest;
import com.volcengine.flink20250101.model.ListGMCSResourcesResponse;
import com.volcengine.flink20250101.model.ListGWSApplicationRequest;
import com.volcengine.sign.Credentials;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Component
class VolcengineManagedFlinkProvider implements ManagedFlinkProvider {

  private static final String REGION = "cn-beijing";
  private static final String ENDPOINT = "open.volcengineapi.com";

  @Override
  public String id() {
    return "volcengine";
  }

  @Override
  public Map<String, String> capabilities(Context context) {
    Map<String, String> report = new LinkedHashMap<>();
    for (String feature :
        Arrays.asList(
            "IDENTITY",
            "DIRECTORY_READ",
            "RESOURCE_POOL_READ",
            "PROJECT_JOB_READ",
            "PUBLISH_SQL",
            "PUBLISH_JAR",
            "START",
            "STOP",
            "SNAPSHOT",
            "SNAPSHOT_THEN_STOP",
            "RESTORE_FROM_SNAPSHOT",
            "RESTORE_FROM_LATEST",
            "ARTIFACT_UPLOAD",
            "ARTIFACT_RUNTIME_READ",
            "LOG_SEARCH",
            "METRICS")) {
      report.put(feature, "UNVERIFIED");
    }
    if (context.getCredentials() == null) {
      return report;
    }
    if (!REGION.equals(context.getRegion())) {
      report.put("RESOURCE_POOL_READ", "UNSUPPORTED");
      report.put("PROJECT_JOB_READ", "UNSUPPORTED");
      return report;
    }
    Target target = context.getTarget();
    Flink20250101Api flink = new Flink20250101Api(client(context));
    try {
      ListGMCSResourcesResponse pools =
          flink.listGMCSResources(
              new ListGMCSResourcesRequest()
                  .projectId(target.getProjectId())
                  .name(target.getResourcePoolName())
                  .pageNum(1)
                  .pageSize(50));
      boolean matched =
          pools != null
              && pools.getDataList() != null
              && pools.getDataList().stream()
                  .anyMatch(
                      pool ->
                          target.getResourcePoolId().equals(pool.getId())
                              && target.getProjectId().equals(pool.getProjectId())
                              && "RUNNING".equals(pool.getStatus()));
      report.put("RESOURCE_POOL_READ", matched ? "AVAILABLE" : "NEEDS_CONFIGURATION");
    } catch (ApiException e) {
      recordError("ListGMCSResources", e);
      report.put("RESOURCE_POOL_READ", "NEEDS_CONFIGURATION");
    }
    try {
      flink.listGWSApplication(
          new ListGWSApplicationRequest()
              .projectId(target.getProjectId())
              .resourcePool(target.getResourcePoolName())
              .pageNum(1)
              .pageSize(1));
      report.put("PROJECT_JOB_READ", "AVAILABLE");
    } catch (ApiException e) {
      recordError("ListGWSApplication", e);
      report.put("PROJECT_JOB_READ", "NEEDS_CONFIGURATION");
    }
    return report;
  }

  @Override
  public Observation dispatch(Context context, OperationStep step) {
    return Observation.outcome(Outcome.REJECTED, "CAPABILITY_UNVERIFIED");
  }

  @Override
  public Observation observe(Context context, OperationStep step, Observation acknowledgement) {
    return Observation.outcome(Outcome.UNKNOWN, "NO_VERIFIED_OBSERVATION_CONTRACT");
  }

  private ApiClient client(Context context) {
    ApiClient client = new ApiClient();
    client.setEndpoint(ENDPOINT);
    client.setRegion(REGION);
    client.setCredentials(
        Credentials.getCredentials(
            context.getCredentials().getAccessKey(), context.getCredentials().getSecretKey()));
    client.setDebugging(false);
    client.setAutoRetry(false);
    client.setConnectTimeout(4000);
    client.setReadTimeout(8000);
    client.setWriteTimeout(8000);
    client
        .getHttpClient()
        .interceptors()
        .removeIf(
            interceptor ->
                interceptor instanceof com.volcengine.interceptor.HttpLoggingInterceptor);
    client.getHttpClient().setRetryOnConnectionFailure(false);
    client.getHttpClient().setFollowRedirects(false);
    client.getHttpClient().setFollowSslRedirects(false);
    return client;
  }

  private void recordError(String action, ApiException error) {
    String requestId =
        error.getResponseMetadata() == null ? null : error.getResponseMetadata().getRequestId();
    String suffix =
        requestId != null && requestId.matches("[a-zA-Z0-9_-]{6,128}")
            ? requestId.substring(requestId.length() - 6)
            : "unavailable";
    log.info(
        "Volcengine read {} failed with HTTP status {} (request suffix {})",
        action,
        error.getCode(),
        suffix);
  }
}
