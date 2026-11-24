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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Credentials;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.EnvironmentInput;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Target;
import org.apache.streampark.console.core.service.ServiceHelper;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.service.MemberService;
import org.apache.streampark.console.system.service.UserService;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.check;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.hash;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.id;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.number;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.required;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.text;

@Service
public class ManagedFlinkCatalog {

  private static final List<String> PROBED_READ_CAPABILITIES =
      Arrays.asList(
          "IDENTITY",
          "DIRECTORY_READ",
          "RESOURCE_POOL_READ",
          "PROJECT_JOB_READ",
          "ARTIFACT_RUNTIME_READ",
          "LOG_SEARCH",
          "METRICS");

  private final ManagedFlinkStore store;
  private final ServiceHelper login;
  private final MemberService members;
  private final UserService users;
  private final ManagedFlinkCredentials credentials;
  private final Map<String, ManagedFlinkProvider> providers;

  ManagedFlinkCatalog(
      ManagedFlinkStore store,
      ServiceHelper login,
      MemberService members,
      UserService users,
      ManagedFlinkCredentials credentials,
      List<ManagedFlinkProvider> providers) {
    this.store = store;
    this.login = login;
    this.members = members;
    this.users = users;
    this.credentials = credentials;
    this.providers =
        providers.stream().collect(Collectors.toMap(ManagedFlinkProvider::id, Function.identity()));
  }

  public long authorize(long team, String... permissions) {
    User user = login.getLoginUser();
    check(user != null, HttpStatus.UNAUTHORIZED, "Sign in to StreamPark");
    long actor = user.getUserId();
    if (UserType.ADMIN.equals(user.getUserType())) {
      return actor;
    }
    check(
        members.findByUserId(team, actor) != null,
        HttpStatus.NOT_FOUND,
        "Managed Flink object not found");
    for (String permission : permissions) {
      check(
          users.getPermissions(actor, team).contains(permission),
          HttpStatus.FORBIDDEN,
          "Managed Flink action forbidden");
    }
    return actor;
  }

  public List<Map<String, Object>> connections(long team, int offset) {
    authorize(team, "team:update");
    return store
        .jdbc()
        .queryForList(
            "select id, name, provider, account_ref as \"accountRef\", "
                + "credential_version as \"credentialVersion\" from t_flink_cloud_connection "
                + "where team_id=? order by name, id limit 100 offset ?",
            team,
            offset);
  }

  public String createConnection(long team, ConnectionInput input) {
    long actor = authorize(team, "team:update");
    String provider = required(input.getProvider(), 32);
    provider(provider);
    String connection = id();
    String account = required(input.getAccountRef(), 128);
    String secret = seal(team, connection, account, provider, input);
    store
        .jdbc()
        .update(
            "insert into t_flink_cloud_connection "
                + "(id, team_id, name, provider, account_ref, credential, created_by, created_at) "
                + "values (?,?,?,?,?,?,?,?)",
            connection,
            team,
            required(input.getName(), 128),
            provider,
            account,
            secret,
            actor,
            System.currentTimeMillis());
    return connection;
  }

  @Transactional
  public void renameConnection(long team, String id, ConnectionInput input) {
    authorize(team, "team:update");
    connection(team, id);
    store
        .jdbc()
        .update(
            "update t_flink_cloud_connection set name=? where team_id=? and id=?",
            required(input.getName(), 128),
            team,
            id);
  }

  public Map<String, String> probe(long team, String env) {
    authorize(team, "team:update");
    Map<String, Object> environment = environment(team, env, false);
    Map<String, Object> connection = connection(team, text(environment, "connection_id"));
    Map<String, String> report =
        provider(text(connection, "provider")).probe(context(connection, environment, null));
    ReadReport reads = new ReadReport();
    for (String capability : PROBED_READ_CAPABILITIES) {
      if (report.containsKey(capability)) {
        reads.put(capability, report.get(capability));
      }
    }
    long credentialVersion = number(connection, "credential_version");
    int refreshed =
        store
            .jdbc()
            .update(
                "update t_flink_managed_env set capability_report=?,capability_observed_at=?,"
                    + "capability_credential_version=? where team_id=? and id=? and config_version=? "
                    + "and exists (select 1 from t_flink_cloud_connection where id=? and team_id=? "
                    + "and credential_version=?)",
                store.encode(reads),
                System.currentTimeMillis(),
                credentialVersion,
                team,
                env,
                number(environment, "config_version"),
                connection.get("id"),
                team,
                credentialVersion);
    check(refreshed == 1, HttpStatus.CONFLICT, "Environment or credentials changed during probe");
    Map<String, String> scopedReport = baseCapabilities(environment, connection);
    scopedReport.putAll(reads);
    return applyIdentityStatus(scopedReport);
  }

  public Map<String, String> rotateConnection(
      long team, String id, String environmentId, ConnectionInput input) {
    authorize(team, "team:update");
    Map<String, Object> connection = connection(team, id);
    Map<String, Object> env = environment(team, environmentId, false);
    check(
        id.equals(text(env, "connection_id")),
        HttpStatus.NOT_FOUND,
        "Managed Flink object not found");
    String provider = text(connection, "provider");
    String account = text(connection, "account_ref");
    check(
        Objects.equals(account, input.getAccountRef()),
        HttpStatus.CONFLICT,
        "Cloud account must match");
    Credentials candidate = supplied(input);
    Context context = new Context(team, text(env, "region"), account, target(env), candidate);
    Map<String, String> report = provider(provider).probe(context);
    check(
        "AVAILABLE".equals(report.get("IDENTITY")),
        HttpStatus.UNPROCESSABLE_ENTITY,
        "Cloud identity could not be confirmed");
    String sealed = credentials.seal(binding(team, id, account), candidate);
    int result =
        store
            .jdbc()
            .update(
                "update t_flink_cloud_connection set credential=?, "
                    + "credential_version=credential_version+1 where team_id=? and id=? and credential_version=?",
                sealed,
                team,
                id,
                number(connection, "credential_version"));
    check(result == 1, HttpStatus.CONFLICT, "Cloud credentials changed");
    return report;
  }

  Map<String, Object> cloudJobs(long team, String id, int page, String name) {
    authorize(team, "team:update");
    check(
        page > 0 && (name == null || name.length() <= 128),
        HttpStatus.BAD_REQUEST,
        "Invalid cloud job filter");
    Map<String, Object> env = environment(team, id, false);
    Map<String, Object> conn = connection(team, text(env, "connection_id"));
    return provider(text(conn, "provider")).cloudJobs(context(conn, env, null), page, name);
  }

  public List<Map<String, Object>> environments(long team, int offset) {
    authorize(team, "app:view");
    return store
        .jdbc()
        .queryForList(
            "select e.id,e.name,e.region,e.enabled,e.target,"
                + "e.connection_id as \"connectionId\", e.config_version as \"configVersion\", c.provider, "
                + "case when e.capability_credential_version=c.credential_version "
                + "then e.capability_observed_at end as \"capabilitiesObservedAt\" "
                + "from t_flink_managed_env e join t_flink_cloud_connection c on c.id=e.connection_id "
                + "and c.team_id=e.team_id where e.team_id=? order by e.name, e.id limit 100 offset ?",
            team,
            offset);
  }

  @Transactional
  public String saveEnvironment(long team, String environmentId, EnvironmentInput input) {
    authorize(team, "team:update");
    Map<String, Object> connection = connection(team, input.getConnectionId());
    Target target = input.getTarget();
    check(target != null, HttpStatus.BAD_REQUEST, "A resource target is required");
    required(target.getProjectId(), 128);
    required(target.getDirectoryId(), 128);
    required(target.getResourcePoolId(), 128);
    required(target.getResourcePoolName(), 128);
    required(target.getEngineRef(), 128);
    String region = required(input.getRegion(), 64);
    check(region.matches("[a-z0-9-]+"), HttpStatus.BAD_REQUEST, "Invalid region");
    String name = required(input.getName(), 128);
    boolean enabled = !Boolean.FALSE.equals(input.getEnabled());
    if (environmentId == null) {
      String created = id();
      store
          .jdbc()
          .update(
              "insert into t_flink_managed_env "
                  + "(id,team_id,connection_id,name,region,target,enabled) values (?,?,?,?,?,?,?)",
              created,
              team,
              connection.get("id"),
              name,
              region,
              store.encode(target),
              enabled);
      return created;
    }
    Map<String, Object> old = environment(team, environmentId, true);
    check(
        Objects.equals(input.getExpectedConfigVersion(), (int) number(old, "config_version")),
        HttpStatus.CONFLICT,
        "Environment configuration changed");
    check(
        Objects.equals(input.getConnectionId(), text(old, "connection_id"))
            && Objects.equals(region, text(old, "region"))
            && Objects.equals(target.getProjectId(), target(old).getProjectId()),
        HttpStatus.CONFLICT,
        "Create a new environment to change cloud scope");
    store
        .jdbc()
        .update(
            "update t_flink_managed_env set name=?,target=?,enabled=?,"
                + "config_version=config_version+1,capability_report=null,capability_observed_at=null,"
                + "capability_credential_version=null where team_id=? and id=?",
            name,
            store.encode(target),
            enabled,
            team,
            environmentId);
    return environmentId;
  }

  public Map<String, String> capabilities(long team, String environmentId) {
    authorize(team, "app:view");
    Map<String, Object> env = environment(team, environmentId, false);
    Map<String, Object> conn = connection(team, text(env, "connection_id"));
    Map<String, String> report = baseCapabilities(env, conn);
    String saved = text(env, "capability_report");
    if (saved != null
        && Objects.equals(
            text(env, "capability_credential_version"), text(conn, "credential_version"))) {
      ReadReport reads = store.decode(saved, ReadReport.class);
      for (String capability : PROBED_READ_CAPABILITIES) {
        if (reads.containsKey(capability)) {
          report.put(capability, reads.get(capability));
        }
      }
    }
    return applyIdentityStatus(report);
  }

  private Map<String, String> applyIdentityStatus(Map<String, String> report) {
    if ("NEEDS_CONFIGURATION".equals(report.get("IDENTITY"))) {
      report.replaceAll(
          (feature, value) -> "AVAILABLE".equals(value) ? "NEEDS_CONFIGURATION" : value);
    }
    return report;
  }

  private Map<String, String> baseCapabilities(Map<String, Object> env, Map<String, Object> conn) {
    return new LinkedHashMap<>(
        provider(text(conn, "provider"))
            .capabilities(
                new Context(
                    number(env, "team_id"),
                    text(env, "region"),
                    text(conn, "account_ref"),
                    target(env),
                    null,
                    text(conn, "credential") == null)));
  }

  Map<String, Object> connection(long team, String id) {
    return connection(team, id, false);
  }

  Map<String, Object> connection(long team, String id, boolean lock) {
    return store.require(
        "select * from t_flink_cloud_connection where team_id=? and id=?"
            + (lock ? " for update" : ""),
        team,
        id);
  }

  Map<String, Object> environment(long team, String id, boolean lock) {
    return store.require(
        "select * from t_flink_managed_env where team_id=? and id=?" + (lock ? " for update" : ""),
        team,
        id);
  }

  Target target(Map<String, Object> environment) {
    return store.decode(text(environment, "target"), Target.class);
  }

  Context context(Map<String, Object> connection, Map<String, Object> environment, Target frozen) {
    long team = number(connection, "team_id");
    String id = text(connection, "id");
    String account = text(connection, "account_ref");
    return new Context(
        team,
        text(environment, "region"),
        account,
        frozen == null ? target(environment) : frozen,
        credentials.open(binding(team, id, account), text(connection, "credential")));
  }

  ManagedFlinkProvider provider(String name) {
    ManagedFlinkProvider adapter = providers.get(name);
    check(adapter != null, HttpStatus.UNPROCESSABLE_ENTITY, "Managed Flink provider unavailable");
    return adapter;
  }

  Context jobContext(
      Map<String, Object> conn, Map<String, Object> env, Target frozen, Map<String, Object> job) {
    Context context = context(conn, env, frozen);
    String releaseId = text(job, "active_release_id");
    if (releaseId != null
        && text(job, "provider_draft_id") == null
        && text(job, "provider_job_id") != null
        && scope(conn, env).equals(text(job, "scope_key"))) {
      Map<String, Object> released =
          store.find(
              "select target from t_flink_managed_release where id=? and app_id=? and status='PUBLISHED'",
              releaseId,
              job.get("app_id"));
      if (released != null
          && sameExecutionScope(
              context.getTarget(), store.decode(text(released, "target"), Target.class))) {
        context.getTarget().setDirectJobId(text(job, "provider_job_id"));
        context.getTarget().setDirectReleaseId(releaseId);
      }
    }
    return context;
  }

  private boolean sameExecutionScope(Target current, Target released) {
    return released != null
        && Objects.equals(current.getProjectId(), released.getProjectId())
        && Objects.equals(current.getDirectoryId(), released.getDirectoryId())
        && Objects.equals(current.getResourcePoolId(), released.getResourcePoolId());
  }

  String scope(Map<String, Object> conn, Map<String, Object> env) {
    return hash(
        store.encode(
            java.util.Arrays.asList(
                text(conn, "provider"),
                text(conn, "account_ref"),
                text(env, "region"),
                target(env).getProjectId())));
  }

  private String seal(
      long team, String id, String account, String provider, ConnectionInput input) {
    ManagedFlinkProvider adapter = provider(provider);
    if (!adapter.requiresCredentials()
        || (input.getAccessKey() == null
            && input.getSecretKey() == null
            && adapter.permitsServerCredential(team, account))) {
      return null;
    }
    return credentials.seal(binding(team, id, account), supplied(input));
  }

  private Credentials supplied(ConnectionInput input) {
    return new Credentials(
        required(input.getAccessKey(), 512), required(input.getSecretKey(), 512));
  }

  private static class ReadReport extends LinkedHashMap<String, String> {
    private static final long serialVersionUID = 1L;
  }

  private String binding(long team, String id, String account) {
    return team + "/" + id + "/" + account;
  }
}
