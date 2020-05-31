package cn.hoob.recommenddemo;



import java.io.Serializable;

public class SimilarityDataModel implements  Serializable {
    private Long uid;
    private Long sidX;
    private Long sidY;
    private  Double count;

    public SimilarityDataModel() { }

    public SimilarityDataModel(Long sidX, Long sidY, Double count) {
        this.sidY = sidY;
        this.sidY = sidY;
        this.count = count;
    }

    public Long getUid() {
        return uid;
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }

    public Long getSidX() {
        return sidX;
    }

    public void setSidX(Long sidX) {
        this.sidX = sidX;
    }

    public Long getSidY() {
        return sidY;
    }

    public void setSidY(Long sidY) {
        this.sidY = sidY;
    }

    public Double getCount() {
        return count;
    }

    public void setCount(Double count) {
        this.count = count;
    }
}
