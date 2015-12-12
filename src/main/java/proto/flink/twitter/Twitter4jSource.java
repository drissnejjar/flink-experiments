package proto.flink.twitter;

import java.util.List;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.log4j.Logger;

import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStreamFactory;

public class Twitter4jSource implements SourceFunction<TwitterMessage> {

    private static final long serialVersionUID = -6932434186650974859L;

    private static final Logger log = Logger.getLogger(Twitter4jSource.class);

    private volatile boolean isRunning = true;

    private volatile boolean isInfinite = false;

    @Override
    public void run(
            SourceContext<TwitterMessage> ctx)
            throws Exception {
        Twitter twitter = TwitterFactory.getSingleton();

        while (isRunning) {
            List<Status> statuses = twitter.getHomeTimeline();
            for (Status status : statuses) {
                TwitterMessage message = new TwitterMessage(status.getId(),
                        status.getHashtagEntities().toString(),
                        status.getText(), status.getUser().getName(),
                        TwitterObjectFactory.getRawJSON(status));
                log.debug(status.getUser().getName() + ":"
                        + status.getText());
                ctx.collect(message);
            }
            if (isInfinite) {
                Thread.sleep(1000);
            } else {
                break;
            }
        }
    }

    //@Override
    public void run2(
            org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<TwitterMessage> ctx)
            throws Exception {

        TwitterStreamFactory
                .getSingleton()
                .onStatus(
                        status -> {
                            TwitterMessage message =
                                    new TwitterMessage(status
                                    .getId(), status.getHashtagEntities()
                                    .toString(), status.getText(), status
                                    .getUser().getName(), TwitterObjectFactory
                                    .getRawJSON(status));
                            log.debug(status.getUser().getName() + ":"
                                    + status.getText());
                            ctx.collect(message);
                        }).onException(e -> e.printStackTrace());
        // .filter("twitter4j", "#twitter4j");
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}