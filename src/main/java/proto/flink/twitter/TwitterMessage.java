package proto.flink.twitter;

import org.apache.flink.api.java.tuple.Tuple5;

// id hashtag text user.name JSON
public class TwitterMessage extends Tuple5<Long, String, String, String, String>{

    public TwitterMessage() {
        super();
    }
    public TwitterMessage(long id, String string, String text, String name,
            String rawJSON) {
        super(id, string, text, name, rawJSON);
    }

    public String getText() {
        return f3;
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1059742035247753711L;

}
