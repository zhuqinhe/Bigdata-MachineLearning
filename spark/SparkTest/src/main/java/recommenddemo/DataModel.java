package recommenddemo;



import org.apache.spark.sql.sources.In;

import java.io.Serializable;

public class DataModel implements Comparable<DataModel>, Serializable {
    private Integer uid;
    private Integer series;
    private String userid;
    private String time;
    private String contentid;
    private Integer count;

    public DataModel() { }

    public DataModel(Integer uid, Integer series, Integer count) {
        this.uid = uid;
        this.series = series;
        this.count = count;
    }

    public Integer getUid() {
        return uid;
    }

    public void setUid(Integer uid) {
        this.uid = uid;
    }

    public Integer getSeries() {
        return series;
    }

    public void setSeries(Integer series) {
        this.series = series;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getContentid() {
        return contentid;
    }

    public void setContentid(String contentid) {
        this.contentid = contentid;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public int compareTo(DataModel o) {
        return this.getCount().compareTo(o.getCount());
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DataModel other = (DataModel) obj;
        if (userid == null) {
            if (other.userid != null)
                return false;
        } else if (!userid.equals(other.userid))
            return false;
        if (contentid == null) {
            if (other.contentid != null)
                return false;
        } else if (!contentid.equals(other.contentid)) {
            return false;
        }
        return true;


    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((contentid == null) ? 0 : contentid.hashCode());
        result = prime * result +((userid == null) ? 0 : userid.hashCode());
        return result;
    }
    @Override
    public String toString() {
        return "DataModel{" +
                "userid='" + userid + '\'' +
                ", time='" + time + '\'' +
                ", contentid='" + contentid + '\'' +
                ", count=" + count +
                '}';
    }

}
