package proto.flink.twitter;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

public class RunnerTwitterGatherer {
    private static boolean fileOutput = false;// true;
    private static String outputPath = "output";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        // DataStream<String> text = env.fromElements("Who's there?",
        // "I think I hear them. Stand, ho! Who's there?");

        //DataStream<String> text = env.socketTextStream("localhost", 2015, '\n');

        //URL resource = RunnerTwitterGatherer.class.getResource("twitter.properties");
        // resource.toURI()

        DataStream<TwitterMessage> text = env.addSource(new Twitter4jSource());

        DataStream<Tuple2<String, Integer>> wordCounts =
                text
                .flatMap(new LineSplitter())
                .keyBy(0)
                .sum(1);


        // DataStream<Tuple2<String, Integer>> wordCounts = text
        // .map(line -> line.toLowerCase().split("\\W+"))
        // // convert splitted line in pairs (2-tuples) containing:
        // // (word,1)
        // .flatMap((String[] tokens,
        // Collector<Tuple2<String, Integer>> out) -> {
        // // emit the pairs with non-zero-length words
        // Arrays.stream(tokens).filter(t -> t.length() > 0)
        // .forEach(t -> out.collect(new Tuple2<>(t, 1)));
        // })
        // // group by the tuple field "0" and sum up tuple field "1"
        // .keyBy(0)
        // .sum(1);

        // emit result
        if (fileOutput) {
            wordCounts.writeAsCsv(outputPath);
            // execute program
        } else {
            wordCounts.print();
        }
        env.execute("WordCount Example");
    }

    public static class LineSplitter implements
            FlatMapFunction<TwitterMessage, Tuple2<String, Integer>> {
        /**
         * 
         */
        private static final long serialVersionUID = -7440828001060308528L;

        @Override
        public void flatMap(TwitterMessage sentence,
                Collector<Tuple2<String, Integer>> out) throws Exception {
            System.out.println(sentence);
            for (String word : sentence.getText().split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
