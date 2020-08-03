package cn.hoob.machine_learning.classify.OneVsRest;

import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 One-vs-Rest
 算法介绍：
 OneVsRest将一个给定的二分类算法有效地扩展到多分类问题应用中，也叫做“One-vs-All.”算法。
 OneVsRest是一个Estimator。它采用一个基础的Classifier然后对于k个类别分别创建二分类问题。
 类别i的二分类分类器用来预测类别为i还是不为i，即将i类和其他类别区分开来。
 最后，通过依次对k个二分类分类器进行评估，取置信最高的分类器的标签作为i类别的标签。
 参数：
 featuresCol:类型：字符串型。含义：特征列名。
 labelCol:类型：字符串型。含义：标签列名。
 predictionCol:类型：字符串型。含义：预测结果列名。
 classifier:类型：分类器型。含义：基础二分类分类器
 **/
public class OneVsRestModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("MultilayerPerceptronClassifierModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        // Load training data
        Dataset<Row> dataFrame = sparkSession.read().format("libsvm").
                load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest" +
                        "\\src\\main\\data\\mllib\\sample_multiclass_classification_data.txt");

        // generate the train/test split.
        Dataset<Row>[] tmp = dataFrame.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> train = tmp[0];
        Dataset<Row> test = tmp[1];

// configure the base classifier.
        LogisticRegression classifier = new LogisticRegression()
                .setMaxIter(10)
                .setTol(1E-6)
                .setFitIntercept(true);

// instantiate the One Vs Rest Classifier.
        OneVsRest ovr = new OneVsRest().setClassifier(classifier);

// train the multiclass model.
        OneVsRestModel ovrModel = ovr.fit(train);

// score the model on test data.
        Dataset<Row> predictions = ovrModel.transform(test)
                .select("prediction", "label");

// obtain evaluator.
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setMetricName("accuracy");

// compute the classification error on test data.
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test Error : " + (1 - accuracy));
    }
}
