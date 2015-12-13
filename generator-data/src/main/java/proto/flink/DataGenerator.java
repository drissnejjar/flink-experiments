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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import proto.flink.twitter.Twitter4jSource;
import proto.flink.twitter.TwitterMessage;
import proto.flink.twitter.TwitterSerializationSchema;

import java.util.ArrayList;
import java.util.Properties;

/**
 * This class provides transmission of Twitter's messages to a kafka queue
 *
 */
public class DataGenerator {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        String zookeeper = params.get("zookeeper", Constants.zookeeperHost);
        String brokers = params.get("brokers", Constants.kafkaBrokerHosts);
        String sinkTopic = params.get("sink", Constants.queueData);
        String resultTopic = params.get("result", Constants.queueResult);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().enableObjectReuse();

        Properties props = new Properties();
        props.setProperty("zookeeper.connect", zookeeper);
        props.setProperty("bootstrap.servers", brokers);
        props.setProperty("group.id", "flink-lambda");
        props.setProperty("auto.commit.enable", "false");
        props.setProperty("auto.offset.reset", "largest");

        DataStream<TwitterMessage> result = env
                .addSource(new Twitter4jSource());

        result.addSink(new FlinkKafkaProducer<>(brokers, sinkTopic,
                new TwitterSerializationSchema()));

        @SuppressWarnings("serial")
        DataStream<Tuple2<Query, String>> inputStream = env.addSource(
                new FlinkKafkaConsumer<>(
                        new ArrayList<String>() {{add(resultTopic);}},
                        new ResultSerializationSchema(),
                        props,
                        FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER,
                        FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL));
        inputStream.print();

        env.execute("Data Generator");
    }
}
