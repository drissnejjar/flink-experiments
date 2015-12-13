package proto.flink;

public class Constants {
    final public static String zookeeperHost = "localhost:2181";
    final public static String kafkaBrokerHosts = "localhost:9092";
    final public static String statePath = "hdfs://localhost:9000/flink-checkpoints";

    final public static String queueData = "regular-input";
    final public static String queueQuery = "query-input";
    final public static String queueResult = "window-result";
    final public static String queueRtBuffer = "rt-buffer";
}
