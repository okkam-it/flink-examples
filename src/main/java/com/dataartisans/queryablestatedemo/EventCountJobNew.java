package com.dataartisans.queryablestatedemo;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;

public class EventCountJobNew {

  /**
   * Main class.
   * 
   * @param args the arguments
   * @throws Exception if any Exception occurs
   */
  public static void main(String[] args) throws Exception {

    // We use a mini cluster here for sake of simplicity, because I don't want
    // to require a Flink installation to run this demo. Everything should be
    // contained in this JAR.

    Configuration config = new Configuration();
    config.setInteger(JobManagerOptions.PORT, EventCountJobLegacy.RPC_PORT);
    config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
    config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, EventCountJobLegacy.PARALLELISM);

    try (LocalFlinkMiniCluster flinkCluster = new LocalFlinkMiniCluster(config, false)) {
      flinkCluster.start(true);

      StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
          "localhost", EventCountJobLegacy.RPC_PORT, EventCountJobLegacy.PARALLELISM);

      DataStream<BumpEvent> bumps = env.addSource(new BumpEventGeneratorSource(true));

      // DEPRECATED API
      // bumps.keyBy(BumpEvent::getItemId)//
      // .asQueryableState(EventCountJobLegacy.QUERY_STATE_NAME,
      // EventCountJobLegacy.getStateDesc());

      // TODO: how to migrate FoldingStateDescriptor to AggregatingStateDescriptor??
      AggregatingStateDescriptor<BumpEvent, Long, Long> countingStateDesc = //
          new AggregatingStateDescriptor<>("itemCounts", new SumAggr(), Long.class);
      bumps.keyBy(BumpEvent::getItemId)//
          .window(GlobalWindows.create())//
          .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))//
          .aggregate(new SumAggr());

      final JobGraph jobGraph = env.getStreamGraph().getJobGraph();

      System.out.println("[info] Job ID: " + jobGraph.getJobID());
      System.out.println();

      flinkCluster.submitJobAndWait(jobGraph, false);
    }
  }

  /**
   * Test {@link AggregateFunction} concatenating the already stored string with the long passed as
   * argument.
   */
  private static class SumAggr implements AggregateFunction<BumpEvent, Long, Long> {

    private static final long serialVersionUID = -6249227626701264599L;

    @Override
    public Long createAccumulator() {
      return 0L;
    }

    @Override
    public Long add(BumpEvent value, Long accumulator) {
      return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
      return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
      return a + b;
    }

  }

}