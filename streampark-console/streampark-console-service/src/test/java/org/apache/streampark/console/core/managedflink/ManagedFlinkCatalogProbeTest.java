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

import org.apache.streampark.console.core.enums.UserType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.ConnectionInput;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Context;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.EnvironmentInput;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Target;
import org.apache.streampark.console.core.service.ServiceHelper;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.service.MemberService;
import org.apache.streampark.console.system.service.UserService;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.web.server.ResponseStatusException;

import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ManagedFlinkCatalogProbeTest {

  private static final long TEAM = 100000L;

  @Test
  void probeRetainsOnlyReadEvidence() {
    Fixture test = new Fixture();
    assertThat(test.catalog.capabilities(TEAM, test.environment))
        .containsEntry("RESOURCE_POOL_READ", "UNVERIFIED");
    assertThat(test.catalog.probe(TEAM, test.environment))
        .containsEntry("RESOURCE_POOL_READ", "AVAILABLE")
        .containsEntry("PUBLISH_SQL", "UNVERIFIED");
    assertThat(test.catalog.capabilities(TEAM, test.environment))
        .containsEntry("RESOURCE_POOL_READ", "AVAILABLE")
        .containsEntry("PUBLISH_SQL", "UNVERIFIED");
    assertThat(test.catalog.environments(TEAM, 0).get(0).get("capabilitiesObservedAt"))
        .isInstanceOf(Number.class);
  }

  @Test
  void probeEvidenceInvalidatesWhenScopeChanges() {
    Fixture test = new Fixture();
    test.catalog.probe(TEAM, test.environment);
    test.input.setExpectedConfigVersion(1);
    test.input.getTarget().setResourcePoolId("next-pool");
    test.catalog.saveEnvironment(TEAM, test.environment, test.input);
    assertThat(test.catalog.capabilities(TEAM, test.environment))
        .containsEntry("RESOURCE_POOL_READ", "UNVERIFIED");
    assertThat(test.catalog.environments(TEAM, 0).get(0).get("capabilitiesObservedAt")).isNull();
  }

  @Test
  void credentialChangesInvalidateAndFenceProbe() {
    Fixture test = new Fixture();
    test.catalog.probe(TEAM, test.environment);
    test.rotateVersion();
    assertThat(test.catalog.capabilities(TEAM, test.environment))
        .containsEntry("RESOURCE_POOL_READ", "UNVERIFIED");

    test.provider.duringProbe = test::rotateVersion;
    assertThatThrownBy(() -> test.catalog.probe(TEAM, test.environment))
        .isInstanceOfSatisfying(
            ResponseStatusException.class,
            error -> assertThat(error.getStatus().value()).isEqualTo(409));
    assertThat(test.catalog.capabilities(TEAM, test.environment))
        .containsEntry("RESOURCE_POOL_READ", "UNVERIFIED");
    assertThat(test.catalog.environments(TEAM, 0).get(0).get("capabilitiesObservedAt")).isNull();
  }

  private static final class Fixture {
    final JdbcTemplate sql;
    final ManagedFlinkCatalog catalog;
    final ProbeProvider provider = new ProbeProvider();
    final EnvironmentInput input = new EnvironmentInput();
    final String connection;
    final String environment;

    Fixture() {
      DriverManagerDataSource dataSource =
          new DriverManagerDataSource(
              "jdbc:h2:mem:"
                  + UUID.randomUUID()
                  + ";MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=true",
              "sa",
              "");
      new ResourceDatabasePopulator(new ClassPathResource("db/schema-h2.sql")).execute(dataSource);
      sql = new JdbcTemplate(dataSource);
      ManagedFlinkStore store = new ManagedFlinkStore(sql);
      ServiceHelper login = mock(ServiceHelper.class);
      User admin = new User();
      admin.setUserId(100001L);
      admin.setUserType(UserType.ADMIN);
      when(login.getLoginUser()).thenReturn(admin);
      String key = Base64.getEncoder().encodeToString(new byte[32]);
      catalog =
          new ManagedFlinkCatalog(
              store,
              login,
              mock(MemberService.class),
              mock(UserService.class),
              new ManagedFlinkCredentials(key, store),
              Collections.singletonList(provider));

      ConnectionInput access = new ConnectionInput();
      access.setName("catalog-fixture");
      access.setProvider(provider.id());
      access.setAccountRef("fixture-account");
      access.setAccessKey("fixture-access");
      access.setSecretKey("fixture-secret");
      connection = catalog.createConnection(TEAM, access);
      Target target = new Target();
      target.setProjectId("fixture-project");
      target.setDirectoryId("fixture-directory");
      target.setResourcePoolId("fixture-pool");
      target.setResourcePoolName("fixture-pool");
      target.setEngineRef("fixture-engine");
      input.setName("catalog-env");
      input.setConnectionId(connection);
      input.setRegion("cn-fixture");
      input.setTarget(target);
      environment = catalog.saveEnvironment(TEAM, null, input);
    }

    void rotateVersion() {
      sql.update(
          "update t_flink_cloud_connection set credential_version=credential_version+1 where id=?",
          connection);
    }
  }

  private static final class ProbeProvider extends MockManagedFlinkProvider {
    Runnable duringProbe = () -> {};

    @Override
    public String id() {
      return "catalog-fixture";
    }

    @Override
    public boolean requiresCredentials() {
      return true;
    }

    @Override
    public Map<String, String> capabilities(Context context) {
      boolean authenticated = context.getCredentials() != null;
      if (authenticated) {
        duringProbe.run();
      }
      Map<String, String> result = new LinkedHashMap<>();
      result.put("RESOURCE_POOL_READ", authenticated ? "AVAILABLE" : "UNVERIFIED");
      result.put("PUBLISH_SQL", authenticated ? "AVAILABLE" : "UNVERIFIED");
      return result;
    }
  }
}
