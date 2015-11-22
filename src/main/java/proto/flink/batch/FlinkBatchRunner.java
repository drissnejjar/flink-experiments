package proto.flink.batch;

public class FlinkBatchRunner {
    public static void main(String[] args) {
        Consumer consumerThread = new Consumer(KafkaProperties.topic);
        consumerThread.start();
    }
}
