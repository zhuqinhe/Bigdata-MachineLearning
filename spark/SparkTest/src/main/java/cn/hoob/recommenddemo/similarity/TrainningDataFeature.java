package cn.hoob.recommenddemo.similarity;

import java.io.Serializable;

/****
 * 分词后合集的向量模型
 * ******/
public class TrainningDataFeature implements Serializable {
    private Long id;
    private String name;
    private String contentId;
    private String kind;
    private String catgoryid;
    private String programType;
    private String vectorIndices;
    private String vectorValues;
    private Integer vectorSize;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public String getCatgoryid() {
        return catgoryid;
    }

    public void setCatgoryid(String catgoryid) {
        this.catgoryid = catgoryid;
    }

    public String getProgramType() {
        return programType;
    }

    public void setProgramType(String programType) {
        this.programType = programType;
    }

    public String getVectorIndices() {
        return vectorIndices;
    }

    public void setVectorIndices(String vectorIndicesv) {
        this.vectorIndices = vectorIndicesv;
    }

    public String getVectorValues() {
        return vectorValues;
    }

    public void setVectorValues(String vectorValues) {
        this.vectorValues = vectorValues;
    }

    public Integer getVectorSize() {
        return vectorSize;
    }

    public void setVectorSize(Integer vectorSize) {
        this.vectorSize = vectorSize;
    }

    @Override
    public String toString() {
        return "TrainningDataFeature{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", contentId='" + contentId + '\'' +
                ", kind='" + kind + '\'' +
                ", catgoryid='" + catgoryid + '\'' +
                ", programType='" + programType + '\'' +
                ", vectorIndices='" + vectorIndices + '\'' +
                ", vectorValues='" + vectorValues + '\'' +
                ", vectorSize=" + vectorSize +
                '}';
    }
}
