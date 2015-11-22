package proto.flink.batch;

import java.util.Properties;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaOutputFormat
        extends RichOutputFormat<Tuple2<String, Integer>> {

    private static final long serialVersionUID = -4394608185949685411L;

    private kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic = KafkaProperties.topic2;
    private final Properties props = new Properties();

    @Override
    public void configure(Configuration parameters) {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "localhost:9092");
        // Use random partitioner. Don't need the key type. Just set it to
        // Integer.
        // The message is of type String.
        producer = new kafka.javaapi.producer.Producer<Integer, String>(
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
    public void writeRecord(Tuple2<String, Integer> record) {
        // output.add(record + getRuntimeContext().getIndexOfThisSubtask() + ""
        // + getRuntimeContext().getNumberOfParallelSubtasks());
        producer.send(
                new KeyedMessage<Integer, String>(topic, record.toString()));
    }
}