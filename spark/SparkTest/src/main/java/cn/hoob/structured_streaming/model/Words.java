package cn.hoob.structured_streaming.model;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * @author zhuqinhe
 */
public class Words implements Serializable {
    private Timestamp  timestamp;
    private  String value;

    public Words(){};

    public Words(Timestamp timestamp, String value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
