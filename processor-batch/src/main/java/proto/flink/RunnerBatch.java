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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.log4j.Logger;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import proto.flink.twitter.TwitterMessageList;
import proto.flink.twitter.TwitterListSerializationSchema;
import proto.flink.twitter.TwitterMessage;
import proto.flink.twitter.TwitterSerializationSchema;

import java.io.IOException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings("unused")
public class RunnerBatch extends Thread {

    private String zookeeper;
    private String brokers;
    private String rtBufferTopic;

    private String queryTopic;
    private String sinkTopic;

    private ConsumerConnector consumerQueries;
    private ConsumerConnector consumerRtBuffer;

    private static final Logger LOG = Logger.getLogger(RunnerStreaming.class);

    public RunnerBatch(String zookeeper, String brokers, String rtBufferTopic,
            String queryTopic, String sinkTopic) {
        super();
        this.zookeeper = zookeeper;
        this.brokers = brokers;
        this.rtBufferTopic = rtBufferTopic;
        this.queryTopic = queryTopic;
        this.sinkTopic = sinkTopic;

        consumerQueries = kafka.consumer.Consumer
                .createJavaConsumerConnector(createConsumerConfig());

        consumerRtBuffer = kafka.consumer.Consumer
                .createJavaConsumerConnector(createConsumerConfig());
    }

    private ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", "flink-lambda");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(rtBufferTopic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                consumerQueries.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(rtBufferTopic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while (it.hasNext()) {
            byte[] in = it.next().message();

            new Runnable() {
                private byte[] in;

                @Override
                public void run() {
                    String message = new String(in);
                    LOG.debug(message);

                    Query query = Query.fromString(message);
                    TwitterMessageList rtBuffer = readRtBuffer();

                    final ExecutionEnvironment env =
                            ExecutionEnvironment.getExecutionEnvironment();

                    DataSet<TwitterMessage> rtDs = env.fromCollection(rtBuffer);

                    try {
                        DataProcessor.process("localhost", query, rtDs)
                                .output(new KafkaOutputFormat(sinkTopic));

                        env.execute();
                    } catch (Exception e) {
                        LOG.error(e.getMessage());
                    }
                }

                public void init(byte[] in) {
                    this.in = in;
                }
            }.init(in);
        }
    }

    private TwitterMessageList readRtBuffer() {
        TwitterMessageList result = null;
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(rtBufferTopic, new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                consumerRtBuffer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(rtBufferTopic)
                .get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        try {
            if (it.hasNext()) {
                byte[] in = it.next().message();
                result = new TwitterListSerializationSchema().deserialize(in);
            }
        } catch (IOException e) {
        } finally {
            if (result == null)
                result = new TwitterMessageList();
        }
        return result;
    }
}
