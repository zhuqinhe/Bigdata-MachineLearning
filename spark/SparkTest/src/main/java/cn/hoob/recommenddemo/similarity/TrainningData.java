package cn.hoob.recommenddemo.similarity;

import java.io.Serializable;

/******
 * 合集相似性元数据模型
 * ******/
public class TrainningData implements Serializable {
    private Long id;
    private String name;
    private String contentId;
    private String kind;
    private String catgoryid;
    private String programType;

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

    @Override
    public String toString() {
        return "TrainningData{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", contentId='" + contentId + '\'' +
                ", kind='" + kind + '\'' +
                ", catgoryid='" + catgoryid + '\'' +
                ", programType='" + programType + '\'' +
                '}';
    }
}
