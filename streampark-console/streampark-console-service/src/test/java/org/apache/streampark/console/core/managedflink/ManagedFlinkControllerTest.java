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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Command;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CommandType;

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
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
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
