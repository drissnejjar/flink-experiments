package proto.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

import proto.flink.twitter.TwitterMessage;

public class DataProcessor {

    @SuppressWarnings("serial")
    static public DataSet<Tuple2<Query, String>> process(
            String dbParams,
            Query query,
            DataSet<TwitterMessage> rtBuffer)
                    throws Exception {

        final String hashtag = query.getHashtag();

        //TODO: connect to persistent storage and create DataSet

        
        DataSet<Tuple2<Query, String>> result = rtBuffer
        .filter(item -> {
            if (hashtag.isEmpty())
                return true;
            else
                return item.getHashtag().equals(hashtag);
        })
        .map(item -> item.getText())
        .reduce(new ReduceFunction<String>() {

            @Override
            public String reduce(String arg0, String arg1) throws Exception {
                // TODO Fix inefficient strings join
                return arg0.concat(arg1);
            }
        })
        .map(new MapFunction<String, Tuple2<Query, String>>(){

            @Override
            public Tuple2<Query, String> map(String arg0) throws Exception {
                return new Tuple2<Query, String>(query, arg0);
            }
            
        });
        return result;
    }
}
