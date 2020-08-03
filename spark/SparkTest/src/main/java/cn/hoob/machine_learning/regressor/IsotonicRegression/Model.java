package cn.hoob.machine_learning.regressor.IsotonicRegression;

import java.io.Serializable;

public class Model implements Serializable {
    private Double label;
    private Double features;
    private Double weight;

    public Model(Double label, Double feature, Double weight) {
        this.label = label;
        this.features = feature;
        this.weight = weight;
    }
    public Model() { };
    public Double getLabel() {
        return label;
    }

    public void setLabel(Double label) {
        this.label = label;
    }

    public Double getFeatures() {
        return features;
    }

    public void setFeatures(Double feature) {
        this.features = feature;
    }

    public Double getWeight() {
        return weight;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }
}
