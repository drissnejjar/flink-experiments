/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package proto.flink;

import org.apache.flink.api.java.utils.ParameterTool;

public class Runner {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        String zookeeper = params.get("zookeeper", Constants.zookeeperHost);
        String brokers = params.get("brokers", Constants.kafkaBrokerHosts);
        String sourceTopic = params.get("source", Constants.queueData);
        String queryTopic = params.get("query", Constants.queueQuery);
        String rtBufferTopic = params.get("query", Constants.queueRtBuffer);
        String sinkTopic = params.get("sink", Constants.queueResult);
        Long windowSize = params.getLong("window-size", 10_000);

        new RunnerBatch(zookeeper, brokers,
                rtBufferTopic, queryTopic,sinkTopic)
            .start();

        new RunnerStreaming(zookeeper, brokers,
                sourceTopic, rtBufferTopic, windowSize)
            .start();
    }
}
