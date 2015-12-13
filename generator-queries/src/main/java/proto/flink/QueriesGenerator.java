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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * This class emulates queries of an external application in the JSON format.
 *
 */
public class QueriesGenerator {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        String zookeeper = params.get("zookeeper", Constants.zookeeperHost);
        String brokers = params.get("brokers", Constants.kafkaBrokerHosts);
        String sinkTopic = params.get("query", Constants.queueQuery);
        Integer sleep = params.getInt("sleep", 1000);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        env.getConfig().enableObjectReuse();

        Properties props = new Properties();
        props.setProperty("zookeeper.connect", zookeeper);
        props.setProperty("bootstrap.servers", brokers);
        props.setProperty("group.id", "flink-lambda");
        props.setProperty("auto.commit.enable", "false");
        props.setProperty("auto.offset.reset", "largest");

        DataStream<String> result = env
                .addSource(new InfiniteQuerySource(sleep));

        result.addSink(new FlinkKafkaProducer<>(brokers, sinkTopic,
                new SimpleStringSchema()));

        // TODO: add results processing

        env.execute("Queries Generator");
    }

    public static class InfiniteQuerySource
            extends RichParallelSourceFunction<String> {

        private static final long serialVersionUID = -8227802175109704944L;

        private int sleepInterval;

        private volatile boolean running = true;

        private long remoteUserId = 0;

        public InfiniteQuerySource(int sleepInterval) {
            this.sleepInterval = sleepInterval;
        }

        @Override
        public void run(SourceContext<String> out) throws Exception {
            while (running) {
                Query query = new Query();
                query.setRemoteUserId(new Long(remoteUserId).toString());
                //query.timeline = query.timeline - ....

                out.collect(query.toString());
                Thread.sleep(sleepInterval);
                remoteUserId++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
