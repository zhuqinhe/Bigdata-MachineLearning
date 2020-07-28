package cn.hoob.recommenddemo.similarity.eg2;

import org.apache.spark.ml.linalg.SparseVector;

import java.io.Serializable;

public class MiniTrainningData implements Serializable {
    private Long id;
    private String contentId;
    private String kind;
    private SparseVector features;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getContentId() {
        return contentId;
    }

    public void setContentId(String contentId) {
        this.contentId = contentId;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public SparseVector getFeatures() {
        return features;
    }

    public void setFeatures(SparseVector features) {
        this.features = features;
    }

    @Override
    public String toString() {
        return "MiniTrainningData{" +
                "id=" + id +
                ", contentId='" + contentId + '\'' +
                ", kind='" + kind + '\'' +
                ", features=" + features +
                '}';
    }
}
