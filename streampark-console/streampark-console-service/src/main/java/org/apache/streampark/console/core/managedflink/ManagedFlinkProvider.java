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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.CloudState;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Context;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.LogQuery;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Outcome;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Spec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

public interface ManagedFlinkProvider {

  String id();

  default boolean requiresCredentials() {
    return true;
  }

  default boolean permitsServerCredential(long team, String account) {
    return false;
  }

  Map<String, String> capabilities(Context context);

  default Map<String, String> probe(Context context) {
    return capabilities(context);
  }

  default String specIssue(Spec spec) {
    return null;
  }

  default List<Action> publishSteps(JobType type, boolean hasDraft) {
    List<Action> steps = new ArrayList<>();
    if (type == JobType.JAR) {
      steps.add(Action.STAGE_ARTIFACT);
    }
    if (!hasDraft) {
      steps.add(Action.CREATE_DRAFT);
    }
    steps.addAll(Arrays.asList(Action.UPDATE_DRAFT, Action.PUBLISH));
    return steps;
  }

  /**
   * Reads the scoped version and latest instance. A found result without an instance confirms that
   * the job has no active instance. Returns null when an authoritative observation is unavailable.
   */
  default CloudState inspect(Context context, String jobId) {
    return null;
  }

  default Map<String, Object> diagnostics(
      Context context, String jobId, String instanceId, String source) {
    return Collections.singletonMap("status", "UNVERIFIED");
  }

  /** Optional selection bounds are epoch milliseconds, shared with the log query. */
  default Map<String, Object> diagnostics(
      Context context, String jobId, String instanceId, String source, Long from, Long until) {
    return diagnostics(context, jobId, instanceId, source);
  }

  default Map<String, Object> logs(
      Context context, String jobId, String instanceId, LogQuery query) {
    return Collections.singletonMap("status", "UNVERIFIED");
  }

  /** Dispatches at most once. Even a confirmed acknowledgement is followed by an observation. */
  Observation dispatch(Context context, OperationStep step);

  /**
   * Requests permission once before dispatch. Implementations separating reads from writes should
   * acquire it after the read checks, immediately before the write.
   */
  default Observation dispatch(Context context, OperationStep step, BooleanSupplier permit) {
    return permit.getAsBoolean()
        ? dispatch(context, step)
        : Observation.outcome(Outcome.REJECTED, "DISPATCH_NOT_AUTHORIZED");
  }

  /** Reads evidence for a specific step. An ambiguous observation must remain unresolved. */
  Observation observe(Context context, OperationStep step, Observation acknowledgement);
}
