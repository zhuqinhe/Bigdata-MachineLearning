package cn.hoob.recommenddemo.similarity.eg2;

import java.io.Serializable;
import java.util.List;

public class SimilartyDatas implements Serializable {
    private   Long id;
    private   List<SimilartyData> similartyDatas;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public List<SimilartyData> getSimilartyDatas() {
        return similartyDatas;
    }

    public void setSimilartyDatas(List<SimilartyData> similartyDatas) {
        this.similartyDatas = similartyDatas;
    }

    @Override
    public String toString() {
        return "SimilartyDatas{" +
                "id=" + id +
                ", similartyDatas=" + similartyDatas +
                '}';
    }
}
