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

import org.apache.streampark.console.core.managedflink.ManagedFlinkLifecycleTest.Fixture;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CloudState;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Context;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.ReadStatus;

import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.web.server.ResponseStatusException;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ManagedFlinkArchiveTest {

  private static final long TEAM = 100000L;
  private static final long APP = 900401L;

  @Test
  void unboundDraftAndConfirmedIdleJobCanArchive() {
    Fixture draft = new Fixture(new MockManagedFlinkProvider());
    draft.jobs.archive(TEAM, APP);
    assertArchived(draft, true);

    Fixture published = published(new MockManagedFlinkProvider());
    published.jobs.archive(TEAM, APP);
    assertArchived(published, true);

    MockManagedFlinkProvider provider = new MockManagedFlinkProvider();
    Fixture stopped = published(provider);
    provider.recordState(
        new CloudState(
            ReadStatus.FOUND,
            stopped.jobField("provider_job_id"),
            publishedVersion(stopped),
            "closed-run",
            "runtime",
            "STOPPED",
            1));
    stopped.jobs.archive(TEAM, APP);
    assertArchived(stopped, true);
  }

  @Test
  void externallyStartedJobCannotArchiveBeforeSync() {
    MockManagedFlinkProvider provider = new MockManagedFlinkProvider();
    Fixture test = published(provider);
    provider.recordState(
        new CloudState(
            ReadStatus.FOUND,
            test.jobField("provider_job_id"),
            publishedVersion(test),
            "external",
            "runtime",
            "RUNNING",
            1));
    assertThat(test.jobField("current_run_id")).isNull();
    assertRejected(test);
  }

  @Test
  void staleAndUnconfirmedCloudReadsBlockArchive() {
    MockManagedFlinkProvider provider = new MockManagedFlinkProvider();
    Fixture test = published(provider);
    String id = test.jobField("provider_job_id");
    for (ReadStatus status :
        Arrays.asList(ReadStatus.STALE, ReadStatus.AUTH_ERROR, ReadStatus.MISSING)) {
      provider.recordState(new CloudState(status, id, null, null, null, null, 0));
      assertRejected(test);
    }
    provider.recordState(new CloudState(ReadStatus.FOUND, id, "drifted", null, null, null, 0));
    assertRejected(test);
  }

  @Test
  void changeAfterReadCannotCreateTombstone() {
    InspectingProvider provider = new InspectingProvider();
    Fixture test = published(provider);
    provider.afterRead =
        () -> {
          assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
          test.sql.update(
              "update t_flink_cloud_connection set credential_version=credential_version+1 where id='conn'");
        };
    assertRejected(test);
  }

  private Fixture published(MockManagedFlinkProvider provider) {
    Fixture test = new Fixture(provider);
    test.complete(test.publish("publish", test.command()));
    return test;
  }

  private String publishedVersion(Fixture test) {
    return test.sql.queryForObject(
        "select provider_version from t_flink_managed_release where status='PUBLISHED'",
        String.class);
  }

  private void assertRejected(Fixture test) {
    assertThatThrownBy(() -> test.jobs.archive(TEAM, APP))
        .isInstanceOfSatisfying(
            ResponseStatusException.class,
            error -> assertThat(error.getStatus().value()).isEqualTo(409));
    assertArchived(test, false);
  }

  private void assertArchived(Fixture test, boolean expected) {
    assertThat(
            test.sql.queryForObject(
                "select archived from t_flink_managed_job where app_id=?", Boolean.class, APP))
        .isEqualTo(expected);
  }

  private static final class InspectingProvider extends MockManagedFlinkProvider {
    Runnable afterRead = () -> {};

    @Override
    public CloudState inspect(Context context, String jobId) {
      CloudState state = super.inspect(context, jobId);
      afterRead.run();
      return state;
    }
  }
}
