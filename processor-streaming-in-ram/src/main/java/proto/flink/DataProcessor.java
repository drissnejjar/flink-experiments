package proto.flink;

import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import proto.flink.twitter.TwitterMessageList;

public class DataProcessor
        extends RichMapFunction<WindowResult<Query, TwitterMessageList>,
                                Tuple2<Query, String>> {

    private static final long serialVersionUID = 2153860089769293870L;

    @Override
    public void open(Configuration parameters)
            throws Exception {
        super.open(parameters);
        //TODO: add persistence storage connection
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public Tuple2<Query, String> map(
            WindowResult<Query, TwitterMessageList> input)
                    throws Exception {

        final Query query = input.query;
        final String hashtag = query.getHashtag();

        String str = input.result.parallelStream()
        .filter(item -> {
            if (hashtag.isEmpty())
                return true;
            else
                return item.getHashtag().equals(hashtag);
        })
        .map(item -> item.getText())
        .collect(Collectors.joining("\n"));

        Tuple2<Query, String> result = new Tuple2<Query, String>(query, str);
        return result;
    }
}
