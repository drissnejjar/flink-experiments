package proto.flink.twitter;

import org.apache.flink.api.java.tuple.Tuple5;

// id hashtag text user.name JSON
public class TwitterMessage extends Tuple5<Long, String, String, String, String>{

    private static final long serialVersionUID = 1059742035247753711L;

    public TwitterMessage() {
        super();
    }

    public TwitterMessage(long id, String hashtag, String text, String name,
            String rawJSON) {
        super(id, hashtag, text, name, rawJSON);
    }

    public Long getId() {
        return f0;
    }

    public String getHashtag() {
        return f1;
    }

    public String getText() {
        return f2;
    }

}
