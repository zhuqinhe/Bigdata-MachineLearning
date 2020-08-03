package cn.hoob.machine_learning.classify.MLP;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 多层感知机
 算法简介：
 多层感知机是基于反向人工神经网络（feedforwardartificial neural network）。
 多层感知机含有多层节点，每层节点与网络的下一层节点完全连接。输入层的节点代表输入数据，
 其他层的节点通过将输入数据与层上节点的权重w以及偏差b线性组合且应用一个激活函数，得到该层输出。
 多层感知机通过方向传播来学习模型，其中我们使用逻辑损失函数以及L-BFGS。K＋1层多层感知机分类器可以写成矩阵形式如下：
 参数：
 featuresCol:类型：字符串型。含义：特征列名。
 labelCol:类型：字符串型。含义：标签列名。
 layers:类型：整数数组型。含义：层规模，包括输入规模以及输出规模。
 maxIter:类型：整数型。含义：迭代次数（>=0）。
 predictionCol:类型：字符串型。含义：预测结果列名。
 seed:类型：长整型。含义：随机种子。
 stepSize:类型：双精度型。含义：每次迭代优化步长。
 tol:类型：双精度型。含义：迭代算法的收敛性。
 **/
public class MultilayerPerceptronClassifierModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("MultilayerPerceptronClassifierModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        // Load training data
        Dataset<Row> dataFrame = sparkSession.read().format("libsvm").
                load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest" +
                        "\\src\\main\\data\\mllib\\sample_multiclass_classification_data.txt");

        // Split the data into train and test
        Dataset<Row>[] splits = dataFrame.randomSplit(new double[]{0.6, 0.4}, 1234L);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];
        // specify layers for the neural network:
        // input layer of size 4 (features), two intermediate of size 5 and 4
        // and output of size 3 (classes)
        int[] layers = new int[] {4, 5, 4, 3};
        // create the trainer and set its parameters
        MultilayerPerceptronClassifier trainer = new MultilayerPerceptronClassifier()
                .setLayers(layers)
                .setBlockSize(128)
                .setSeed(1234L)
                .setMaxIter(100);
        // train the model
        MultilayerPerceptronClassificationModel model = trainer.fit(train);
        // compute accuracy on the test set
        Dataset<Row> result = model.transform(test);
        Dataset<Row> predictionAndLabels = result.select("prediction", "label");
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setMetricName("accuracy");
        System.out.println("Accuracy = " + evaluator.evaluate(predictionAndLabels));
    }
}
