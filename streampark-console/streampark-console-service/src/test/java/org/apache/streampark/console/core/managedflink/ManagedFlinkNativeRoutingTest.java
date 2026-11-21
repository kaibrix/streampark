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

import org.apache.streampark.common.enums.ExecutionMode;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.mapper.ApplicationMapper;
import org.apache.streampark.console.core.service.impl.ApplicationServiceImpl;
import org.apache.streampark.console.core.task.FlinkAppHttpWatcher;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ManagedFlinkNativeRoutingTest {

  @Test
  void managedJobRejectsNativeActions() {
    Application persist = app(900301L, ExecutionMode.MANAGED_FLINK);
    Application caller = app(persist.getId(), ExecutionMode.REMOTE);
    ApplicationServiceImpl service = spy(new ApplicationServiceImpl());
    ApplicationMapper mapper = mock(ApplicationMapper.class);
    ReflectionTestUtils.setField(service, "baseMapper", mapper);
    doReturn(persist).when(service).getById(persist.getId());

    assertThatThrownBy(() -> service.start(caller, false)).isInstanceOf(ApiAlertException.class);
    assertThatThrownBy(() -> service.cancel(caller)).isInstanceOf(ApiAlertException.class);
    assertThatThrownBy(() -> service.restart(caller)).isInstanceOf(ApiAlertException.class);
    assertThatThrownBy(() -> service.abort(caller)).isInstanceOf(ApiAlertException.class);
    assertThatThrownBy(() -> service.mapping(caller)).isInstanceOf(ApiAlertException.class);
    assertThatThrownBy(() -> service.buildApplication(persist.getId(), true))
        .isInstanceOf(ApiAlertException.class);
    assertThatThrownBy(() -> service.checkSavepointPath(caller))
        .isInstanceOf(ApiAlertException.class);
    verifyNoInteractions(mapper);
  }

  @Test
  void managedDetailAndWatcherNeedNoNativeState() {
    Application managed = app(900302L, ExecutionMode.MANAGED_FLINK);
    managed.setTracking(1);
    ApplicationServiceImpl service = new ApplicationServiceImpl();
    ApplicationMapper mapper = mock(ApplicationMapper.class);
    ReflectionTestUtils.setField(service, "baseMapper", mapper);
    when(mapper.getApp(managed)).thenReturn(managed);

    assertThat(service.getApp(managed)).isSameAs(managed);
    FlinkAppHttpWatcher.doWatching(managed);
    assertThat(FlinkAppHttpWatcher.getWatchingApps())
        .noneMatch(app -> app.getId().equals(managed.getId()));
  }

  @Test
  void nativeWatcherGetsUpdatedMapping() {
    Application before = app(900303L, ExecutionMode.REMOTE);
    before.setJobId("old");
    Application after = app(before.getId(), ExecutionMode.REMOTE);
    after.setJobId("new");
    ApplicationServiceImpl service = spy(new ApplicationServiceImpl());
    ApplicationMapper mapper = mock(ApplicationMapper.class);
    ReflectionTestUtils.setField(service, "baseMapper", mapper);
    doReturn(before, after).when(service).getById(before.getId());
    when(mapper.mapping(before)).thenReturn(true);

    try {
      assertThat(service.mapping(before)).isTrue();
      assertThat(FlinkAppHttpWatcher.getWatchingApps())
          .anyMatch(app -> app.getId().equals(after.getId()) && "new".equals(app.getJobId()));
    } finally {
      FlinkAppHttpWatcher.unWatching(after.getId());
    }
  }

  private Application app(long id, ExecutionMode mode) {
    Application app = new Application();
    app.setId(id);
    app.setExecutionMode(mode.getMode());
    return app;
  }
}
