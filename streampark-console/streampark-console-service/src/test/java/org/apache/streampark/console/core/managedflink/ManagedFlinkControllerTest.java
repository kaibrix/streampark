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

import org.apache.streampark.console.base.handler.GlobalExceptionHandler;
import org.apache.streampark.console.core.enums.UserType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkLifecycleTest.Fixture;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Command;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CommandType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.LogQuery;
import org.apache.streampark.console.system.entity.Member;
import org.apache.streampark.console.system.entity.User;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.server.ResponseStatusException;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ManagedFlinkControllerTest {

  @Test
  void commandReturns202AndBody200() throws Exception {
    ManagedFlinkJobs jobs = mock(ManagedFlinkJobs.class);
    ManagedFlinkStore store = mock(ManagedFlinkStore.class);
    when(jobs.enqueue(
            eq(100000L), eq(900432L), eq(CommandType.PUBLISH), eq("same"), any(Command.class)))
        .thenReturn("operation");
    when(store.require(anyString(), eq(100000L), eq(900432L), eq("operation")))
        .thenReturn(Collections.singletonMap("status", "PENDING"));
    mvc(jobs, store)
        .perform(
            post("/flink/managed/apps/900432/publish")
                .param("teamId", "100000")
                .header("Idempotency-Key", "same")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().isAccepted())
        .andExpect(jsonPath("$.code").value("200"))
        .andExpect(jsonPath("$.data.operationId").value("operation"))
        .andExpect(jsonPath("$.data.appId").value("900432"));
  }

  @Test
  void crossTeamAndPermissionStatusPreserveBody() throws Exception {
    ManagedFlinkJobs jobs = mock(ManagedFlinkJobs.class);
    when(jobs.detail(200001L, 900432L))
        .thenThrow(new ResponseStatusException(HttpStatus.NOT_FOUND, "Object not found"));
    when(jobs.detail(200002L, 900432L))
        .thenThrow(new ResponseStatusException(HttpStatus.FORBIDDEN, "Action forbidden"));
    MockMvc http = mvc(jobs, mock(ManagedFlinkStore.class));
    http.perform(get("/flink/managed/apps/900432").param("teamId", "200001"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.code").value("404"));
    http.perform(get("/flink/managed/apps/900432").param("teamId", "200002"))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.code").value("403"));
  }

  @Test
  void logReadsRequireAnAppOwnedRunAndTheSeparatePermission() throws Exception {
    MockManagedFlinkProvider provider = spy(new MockManagedFlinkProvider());
    Fixture fixture = new Fixture(provider);
    fixture.sql.update("update t_flink_managed_job set provider_job_id='fixture-job'");
    fixture.sql.update(
        "insert into t_flink_managed_run "
            + "(id,app_id,provider_instance_id,run_state,origin,started_at,observed_at) "
            + "values ('known-run',900401,'provider-run','RUNNING','STREAMPARK',1,1)");
    User viewer = new User();
    viewer.setUserId(100001L);
    viewer.setUserType(UserType.USER);
    when(fixture.login.getLoginUser()).thenReturn(viewer);
    when(fixture.members.findByUserId(100000L, 100001L)).thenReturn(new Member());
    when(fixture.members.findByUserId(200000L, 100001L)).thenReturn(new Member());
    when(fixture.users.getPermissions(100001L, 100000L)).thenReturn(ImmutableSet.of("app:detail"));
    when(fixture.users.getPermissions(100001L, 200000L))
        .thenReturn(ImmutableSet.of("app:detail", "app:log"));
    MockMvc http =
        MockMvcBuilders.standaloneSetup(
                new ManagedFlinkController(
                    fixture.catalog,
                    fixture.jobs,
                    mock(ManagedFlinkArtifacts.class),
                    fixture.store))
            .build();
    http.perform(get("/flink/managed/apps/900401").param("teamId", "100000"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.data.environmentName").value("review"))
        .andExpect(jsonPath("$.data.capabilities.START").value("AVAILABLE"))
        .andExpect(jsonPath("$.data.runtimeCapabilities.STOP").value("AVAILABLE"))
        .andExpect(jsonPath("$.data.credential").doesNotExist());
    http.perform(get("/flink/managed/environments/env").param("teamId", "100000"))
        .andExpect(status().isForbidden());
    String runs = "/flink/managed/apps/900401/runs/";
    http.perform(get(runs + "known-run/logs").param("teamId", "100000"))
        .andExpect(status().isForbidden());
    http.perform(get(runs + "known-run/logs").param("teamId", "200000"))
        .andExpect(status().isNotFound());
    when(fixture.users.getPermissions(100001L, 100000L))
        .thenReturn(ImmutableSet.of("app:detail", "app:log"));
    http.perform(get(runs + "provider-run/logs").param("teamId", "100000"))
        .andExpect(status().isNotFound());
    verify(provider, never()).logs(any(), any(), any(), any());
    http.perform(get(runs + "known-run/logs").param("teamId", "100000").param("keyword", "trace"))
        .andExpect(status().isOk())
        .andExpect(header().string("Cache-Control", "no-store"))
        .andExpect(jsonPath("$.data.status").value("UNVERIFIED"));
    verify(provider)
        .logs(
            any(),
            eq("fixture-job"),
            eq("provider-run"),
            eq(new LogQuery(100001L, null, null, null, null, "trace", null)));
  }

  @Test
  void missingKeyIsBadRequest() throws Exception {
    ManagedFlinkJobs jobs = mock(ManagedFlinkJobs.class);
    when(jobs.enqueue(eq(100000L), eq(900432L), eq(CommandType.PUBLISH), any(), any(Command.class)))
        .thenThrow(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Idempotency key required"));
    mvc(jobs, mock(ManagedFlinkStore.class))
        .perform(
            post("/flink/managed/apps/900432/publish")
                .param("teamId", "100000")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("400"));
  }

  private MockMvc mvc(ManagedFlinkJobs jobs, ManagedFlinkStore store) {
    return MockMvcBuilders.standaloneSetup(
            new ManagedFlinkController(
                mock(ManagedFlinkCatalog.class), jobs, mock(ManagedFlinkArtifacts.class), store))
        .setControllerAdvice(new GlobalExceptionHandler())
        .build();
  }
}
