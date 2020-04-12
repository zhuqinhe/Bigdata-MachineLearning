package cn.hoob.jdbcrdd;

import java.io.Serializable;

public class UserScore implements Comparable<UserScore>, Serializable {
    private Long id;
    private String name;
    private Double mathScore;
    private Double chineseScore;

    public UserScore(){}
    public UserScore(String name, Double mathScore, Double chineseScore){
        this.name=name;
        this.mathScore=mathScore;
        this.chineseScore=chineseScore;
    }

    public Long getId() {
        return id;
    }
    public UserScore(Long id,String name, Double mathScore, Double chineseScore){
        this.name=name;
        this.mathScore=mathScore;
        this.chineseScore=chineseScore;
        this.id=id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Double getMathScore() {
        return mathScore;
    }

    public void setMathScore(Double mathScore) {
        this.mathScore = mathScore;
    }

    public Double getChineseScore() {
        return chineseScore;
    }

    public void setChineseScore(Double chineseScore) {
        this.chineseScore = chineseScore;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int compareTo(UserScore o) {
        if(this.mathScore!=o.getMathScore()){
            return -(this.mathScore.intValue()-o.getMathScore().intValue());
        }else if(this.chineseScore!=o.getChineseScore()){
            return -(this.chineseScore.intValue()-o.getChineseScore().intValue());
        }
        return 0;
    }

    @Override
    public String toString() {
        return "UserScore{" +
                "name='" + name + '\'' +
                ", mathScore=" + mathScore +
                ", chineseScore=" + chineseScore +
                '}';
    }
}
