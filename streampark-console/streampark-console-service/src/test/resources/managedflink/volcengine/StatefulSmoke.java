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

package org.apache.streampark.acceptance;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public class StatefulSmoke {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(2000);
    String label = "jar-v1 " + Arrays.toString(programArgs(args))
        + " jm=" + System.getenv("STREAM_PARK_ACCEPTANCE_JM");
    env.addSource(new Sequence()).uid("streampark-smoke-source")
        .map(value -> label + " tm=" + System.getenv("STREAM_PARK_ACCEPTANCE_TM")
            + " offset=" + value).uid("streampark-smoke-marker")
        .print().uid("streampark-smoke-print");
    env.execute("streampark-managed-flink-smoke");
  }

  private static String[] programArgs(String[] args) {
    if (!Boolean.parseBoolean(System.getenv("STREAM_PARK_ACCEPTANCE_ARGS_BASE64"))) {
      return args;
    }
    if (args.length != 1) {
      throw new IllegalArgumentException("Expected one encoded argument vector");
    }
    return new String(Base64.getDecoder().decode(args[0]), StandardCharsets.UTF_8)
        .split("\u0000", -1);
  }

  private static class Sequence extends RichSourceFunction<Long>
      implements CheckpointedFunction {
    private volatile boolean active = true;
    private long offset;
    private transient ListState<Long> checkpoint;

    @Override
    public void run(SourceContext<Long> source) throws InterruptedException {
      while (active && offset < 10000) {
        synchronized (source.getCheckpointLock()) {
          source.collect(++offset);
        }
        Thread.sleep(1000);
      }
    }

    @Override
    public void cancel() {
      active = false;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      checkpoint = context.getOperatorStateStore().getListState(
          new ListStateDescriptor<>("streampark-smoke-offset", Long.class));
      if (context.isRestored()) {
        for (Long restored : checkpoint.get()) {
          offset = Math.max(offset, restored);
        }
      }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      checkpoint.clear();
      checkpoint.add(offset);
    }
  }
}
