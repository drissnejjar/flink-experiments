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

import proto.flink.twitter.TwitterMessageList;
import proto.flink.twitter.TwitterMessage;
import proto.flink.twitter.TwitterSerializationSchema;

import java.util.ArrayList;
import java.util.Properties;

public class Runner {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        String zookeeper = params.get("zookeeper", Constants.zookeeperHost);
        String brokers = params.get("brokers", Constants.kafkaBrokerHosts);
        String sourceTopic = params.get("source", Constants.queueData);
        String queryTopic = params.get("query", Constants.queueQuery);
        String sinkTopic = params.get("sink", Constants.queueResult);
        Long windowSize = params.getLong("window-size", 10_000);
        Long checkpointInterval = params.getLong("checkpoint", 1000);

        
        String statePath = params.get("state-path", Constants.statePath);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        env.enableCheckpointing(checkpointInterval);
        env.setStateBackend(new FsStateBackend(statePath));

        Properties props = new Properties();
        props.setProperty("zookeeper.connect", zookeeper);
        props.setProperty("bootstrap.servers", brokers);
        props.setProperty("group.id", "flink-lambda");
        props.setProperty("auto.commit.enable", "false");
        props.setProperty("auto.offset.reset", "largest");

        @SuppressWarnings("serial")
        DataStream<TwitterMessage> inputStream = env.addSource(
                new FlinkKafkaConsumer<>(
                        new ArrayList<String>() {{add(sourceTopic);}},
                        new TwitterSerializationSchema(),
                        props,
                        FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER,
                        FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL));

        @SuppressWarnings("serial")
        KeyedStream<Query, String> queryStream = env.addSource(
                new FlinkKafkaConsumer<>(
                        new ArrayList<String>() {{add(queryTopic);}},
                        new SimpleStringSchema(),
                        props,
                        FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER,
                        FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL))
                .map(new MapFunction<String, Query>() {
                    @Override
                    public Query map(String json) throws Exception {
                        return Query.fromString(json);
                    }
                    
                })
                .keyBy(new KeySelector<Query, String>() {
                    @Override
                    public String getKey(Query query) throws Exception {
                        return query.getRemoteUserId();
                    }

                });

        @SuppressWarnings("serial")
        KeyedStream<TwitterMessage, Long> withOne = inputStream
                .keyBy(new KeySelector<TwitterMessage, Long>() {
                    @Override
                    public Long getKey(TwitterMessage value)
                            throws Exception {
                        return value.getId();
                    }
                });

//        TypeInformation<ResultSerializationSchema> resultType = 
//                (TypeInformation) TypeInfoParser.parse(ResultSerializationSchema.class.getSimpleName());
//        WindowResultType<Query, ListOfMessages> resultType =
//                new WindowResultType<>(
//                        TypeInfoParser.parse(ListOfMessages.class.getSimpleName()),
//                        TypeInfoParser.parse(Query.class.getSimpleName())
//                        );
                
        DataStream<Tuple2<Query, String>> result = withOne
                .connect(queryStream).transform("Query Window",
                        new WindowResultType<>(
                                TypeInfoParser.parse(TwitterMessageList.class.getCanonicalName()),
                                TypeInfoParser.parse(Query.class.getCanonicalName())),
                        new QueryableWindowOperator(windowSize))
                .map(new DataProcessor());

        result.addSink(new FlinkKafkaProducer<>(brokers, sinkTopic,
                new ResultSerializationSchema())).disableChaining();

        env.execute("Query Window Example");
    }

}
