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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EventCountJobLegacy {

  public static final int RPC_PORT = 6124;
  public static final int PARALLELISM = 4;
  public static final String QUERY_STATE_NAME = "itemCounts";

  /**
   * Main class.
   * 
   * @param args the arguments
   * @throws Exception if any Exception occurs
   */
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.addSource(new BumpEventGeneratorSource(true))//
        .keyBy(BumpEvent::getItemId)//
        .asQueryableState(QUERY_STATE_NAME, getStateDesc());

    final JobExecutionResult jobGraph = env.execute("EventCountJob");

    System.out.println("[info] Job ID: " + jobGraph.getJobID());
    System.out.println();

  }

  /**
   * Return the FoldingStateDescriptor descriptor.
   * 
   * @return the FoldingStateDescriptor descriptor.
   */
  public static FoldingStateDescriptor<BumpEvent, Long> getStateDesc() {
    return new FoldingStateDescriptor<>("itemCounts", //
        0L, // Initial value is 0
        (acc, event) -> acc + 1L, // Increment for each event
        Long.class);
  }

}