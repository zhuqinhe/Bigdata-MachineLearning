package cn.hoob.aceessprocess;

import java.io.Serializable;

public class IpRules implements Serializable {
    private Long minNum;
    private Long maxNum;
    private String province;

    public IpRules(Long minNum, Long maxNum, String province) {
        this.minNum = minNum;
        this.maxNum = maxNum;
        this.province = province;
    }

    public Long getMinNum() {
        return minNum;
    }

    public void setMinNum(Long minNum) {
        this.minNum = minNum;
    }

    public Long getMaxNum() {
        return maxNum;
    }

    public void setMaxNum(Long maxNum) {
        this.maxNum = maxNum;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    @Override
    public String toString() {
        return "IpRules{" +
                "minNum=" + minNum +
                ", maxNum=" + maxNum +
                ", province='" + province + '\'' +
                '}';
    }
}
