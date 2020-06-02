package cn.hoob.recommenddemo.similarity;

import java.io.Serializable;

public class SimilartyData implements Serializable {
    private Long id;
    private Long eleId;
    private String contentId;
    private  Double similarty;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getEleId() {
        return eleId;
    }

    public void setEleId(Long eleId) {
        this.eleId = eleId;
    }

    public String getContentId() {
        return contentId;
    }

    public void setContentId(String contentId) {
        this.contentId = contentId;
    }

    public Double getSimilarty() {
        return similarty;
    }

    public void setSimilarty(Double similarty) {
        this.similarty = similarty;
    }

    @Override
    public String toString() {
        return "SimilartyData{" +
                "id=" + id +
                ", eleId=" + eleId +
                ", contentId='" + contentId + '\'' +
                ", similarty=" + similarty +
                '}';
    }
}
