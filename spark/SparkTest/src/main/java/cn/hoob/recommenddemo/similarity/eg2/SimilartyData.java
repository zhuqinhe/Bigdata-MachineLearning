package cn.hoob.recommenddemo.similarity.eg2;

import java.io.Serializable;

public class SimilartyData implements  Comparable,Serializable {
    private Long id;
    private Long eleId;
    private String contentId;
    private String  eleContentId;
    private Double similarty;

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

    public String getEleContentId() {
        return eleContentId;
    }

    public void setEleContentId(String eleContentId) {
        this.eleContentId = eleContentId;
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

    @Override
    public int compareTo(Object o) {
        if (o instanceof SimilartyData) {
            SimilartyData goods = (SimilartyData) o;
            if (this.similarty > goods.getSimilarty()) {
                return 1;
            } else if (this.similarty < goods.getSimilarty()) {
                return -1;
            } else {
                return 0;

            }
        }
        return 0;
    }

}
