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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import org.apache.log4j.Logger;

import proto.flink.twitter.TwitterMessageList;
import proto.flink.twitter.TwitterListSerializationSchema;
import proto.flink.twitter.TwitterMessage;
import proto.flink.twitter.TwitterSerializationSchema;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class RunnerStreaming extends Thread {

    private String zookeeper;
    private String brokers;
    private String sourceTopic;
    private String rtBufferTopic;

    @SuppressWarnings("unused")
    private Long windowSize;

    private static final Logger LOG = Logger.getLogger(RunnerStreaming.class);

    public RunnerStreaming(String zookeeper, String brokers, String sourceTopic,
            String rtBufferTopic, Long windowSize) {
        super();
        this.zookeeper = zookeeper;
        this.brokers = brokers;
        this.sourceTopic = sourceTopic;
        this.rtBufferTopic = rtBufferTopic;
        this.windowSize = windowSize;
    }

    @Override
    public void run() {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("zookeeper.connect", zookeeper);
        props.setProperty("bootstrap.servers", brokers);
        props.setProperty("group.id", "flink-lambda");
        props.setProperty("auto.commit.enable", "false");
        props.setProperty("auto.offset.reset", "largest");

        @SuppressWarnings("serial")
        DataStream<TwitterMessage> inputStream = env
                .addSource(new FlinkKafkaConsumer<>(
                        new ArrayList<String>() {{ add(sourceTopic); }},
                        new TwitterSerializationSchema(),
                        props,
                        FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER,
                        FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL));

        @SuppressWarnings("serial")
        KeyedStream<TwitterMessage, Long> withOne = inputStream
                .keyBy(new KeySelector<TwitterMessage, Long>() {
                    @Override
                    public Long getKey(TwitterMessage value) throws Exception {
                        return value.getId();
                    }
                });

        WindowedStream<TwitterMessage, Long, TimeWindow> rtStream = withOne
                .timeWindow(Time.of(10, TimeUnit.SECONDS),
                        Time.of(200, TimeUnit.MILLISECONDS));

        @SuppressWarnings("serial")
        DataStream<TwitterMessageList> outStream = rtStream.fold(
                new TwitterMessageList(),
                new FoldFunction<TwitterMessage, TwitterMessageList>() {
                    @Override
                    public TwitterMessageList fold(TwitterMessageList arg0,
                            TwitterMessage arg1) throws Exception {
                        arg0.add(arg1);
                        return arg0;
                    }
                });

        outStream
                .addSink(new FlinkKafkaProducer<TwitterMessageList>(
                        brokers,
                        rtBufferTopic,
                        new TwitterListSerializationSchema()))
                .disableChaining();

        try {
            env.execute("Query Window Example");
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        super.run();
    }

}
