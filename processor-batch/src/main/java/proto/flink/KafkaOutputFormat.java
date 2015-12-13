package proto.flink;

import java.util.Properties;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaOutputFormat
        extends RichOutputFormat<Tuple2<Query, String>> {

    private static final long serialVersionUID = -4394608185949685411L;

    private kafka.javaapi.producer.Producer<Integer, Tuple2<Query, String>> producer;
    private String topic;
    private final Properties props = new Properties();

    public KafkaOutputFormat(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public void configure(Configuration parameters) {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "localhost:9092");
        topic = parameters.getString("sink", "window-result");

        // Use random partitioner. Don't need the key type. Just set it to
        // Integer.
        // The message is of type String.
        producer = new kafka.javaapi.producer.Producer<Integer, Tuple2<Query, String>>(
                new ProducerConfig(props));
    }

    @Override
    public void open(int a, int b) {
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public void writeRecord(Tuple2<Query, String> record) {
        producer.send(
                new KeyedMessage<Integer, Tuple2<Query, String>>(topic, record));
    }
}