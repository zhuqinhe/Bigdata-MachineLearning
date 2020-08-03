package cn.hoob.machine_learning.classify.NaiveBayes;

import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 朴素贝叶斯
 算法介绍：
 朴素贝叶斯法是基于贝叶斯定理与特征条件独立假设的分类方法。
 朴素贝叶斯的思想基础是这样的：对于给出的待分类项，求解在此项出现的条件下各个类别出现的概率，
 在没有其它可用信息下，我们会选择条件概率最大的类别作为此待分类项应属的类别。
 朴素贝叶斯分类的正式定义如下：
 ...
 spark.ml现在支持多项朴素贝叶斯和伯努利朴素贝叶斯。
 参数：
 featuresCol:类型：字符串型。含义：特征列名。
 labelCol:类型：字符串型。含义：标签列名。
 modelType:类型：字符串型。含义：模型类型（区分大小写）。
 predictionCol:类型：字符串型.含义：预测结果列名。
 probabilityCol:类型：字符串型。含义：用以预测类别条件概率的列名。
 rawPredictionCol:类型：字符串型。含义：原始预测。
 smoothing:类型：双精度型。含义：平滑参数。
 thresholds:类型：双精度数组型。含义：多分类预测的阀值，以调整预测结果在各个类别的概率。
 **/
public class NaiveBayesModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("NaiveBayesModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        // Load training data
        Dataset<Row> dataFrame = sparkSession.read().format("libsvm").
                load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest" +
                        "\\src\\main\\data\\mllib\\sample_libsvm_data.txt");

        ///Split the data into train and test
        Dataset<Row>[] splits = dataFrame.randomSplit(new double[]{0.6, 0.4}, 1234L);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];

        // create the trainer and set its parameters
        NaiveBayes nb = new NaiveBayes();
          // train the model
        NaiveBayesModel model = nb.fit(train);
        // compute accuracy on the test set
        Dataset<Row> result = model.transform(test);
        Dataset<Row> predictionAndLabels = result.select("prediction", "label");
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setMetricName("accuracy");
        System.out.println("Accuracy = " + evaluator.evaluate(predictionAndLabels));
    }
}
