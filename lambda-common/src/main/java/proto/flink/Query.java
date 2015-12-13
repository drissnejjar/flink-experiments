package proto.flink;

import java.io.Serializable;
import java.util.Date;

import com.google.gson.Gson;

public class Query implements Serializable {

    private static final long serialVersionUID = -805638447473718038L;

    private String hashtag = "";
    private Date timeline = new Date();
    private String userId = "";

    private String remoteUserId = "";

    public String getHashtag() {
        return hashtag;
    }

    public void setHashtag(String hashtag) {
        this.hashtag = hashtag;
    }

    public Date getTimeline() {
        return timeline;
    }

    public void setTimeline(Date timeline) {
        this.timeline = timeline;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getRemoteUserId() {
        return remoteUserId;
    }

    public void setRemoteUserId(String remoteUserId) {
        this.remoteUserId = remoteUserId;
    }

    public Query() {
    }

    static Query fromString(String json) {
        Gson gson = new Gson();
        return gson.fromJson(json, Query.class);
    }

    public Query(String hashtag, Date timeline, String userId,
            String remoteUserId) {
        super();
        this.hashtag = hashtag;
        this.timeline = timeline;
        this.userId = userId;
        this.remoteUserId = remoteUserId;
    }

    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}