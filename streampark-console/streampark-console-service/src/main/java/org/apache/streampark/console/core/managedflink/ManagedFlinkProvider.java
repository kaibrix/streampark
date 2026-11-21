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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Context;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.JobType;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface ManagedFlinkProvider {

  String id();

  default boolean requiresCredentials() {
    return true;
  }

  Map<String, String> capabilities(Context context);

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

  /** Dispatches at most once. Even a confirmed acknowledgement is followed by an observation. */
  Observation dispatch(Context context, OperationStep step);

  /** Reads evidence for a specific step. An ambiguous observation must remain unresolved. */
  Observation observe(Context context, OperationStep step, Observation acknowledgement);
}
