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

import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.queryablestate.client.QueryableStateClient;

public class EventCountClient {

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 9069;// QueryableStateOptions.PROXY_PORT_RANGE.defaultValue()

  // TODO JOBID and KEY must be changed every time (runtime values)
  private static final String JOBID = "41c4e5cf8d5e985fd4be2937b419d62e";
  private static final String KEY = "slb";

  /**
   * Main.
   */
  public static void main(String[] args) throws Exception {
    final JobID jobId = JobID.fromHexString(JOBID);
    final QueryableStateClient client = new QueryableStateClient(HOST, PORT);
    ExecutionConfig config = new ExecutionConfig();
    config.enableGenericTypes();
    client.setExecutionConfig(config);

    CompletableFuture<FoldingState<BumpEvent, Long>> resultFuture = client.getKvState(jobId,
        EventCountJobLegacy.QUERY_STATE_NAME, //
        KEY, //
        BasicTypeInfo.STRING_TYPE_INFO, //
        EventCountJobLegacy.getStateDesc());

    System.out.println("get kv state return future, waiting......");
    FoldingState<BumpEvent, Long> res = resultFuture.join();
    System.out.println("query result: " + res);

    // resultFuture.thenAccept(response -> {
    // try {
    // Long res = response.get();
    // System.out.println("***" + res);
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // });

    client.shutdownAndWait();
  }
}
