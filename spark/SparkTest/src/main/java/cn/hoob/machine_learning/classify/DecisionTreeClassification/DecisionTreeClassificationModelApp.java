package cn.hoob.machine_learning.classify.DecisionTreeClassification;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 决策树
 算法介绍：
 决策树以及其集成算法是机器学习分类和回归问题中非常流行的算法。因其易解释性、
 可处理类别特征、易扩展到多分类问题、不需特征缩放等性质被广泛使用。
 树集成算法如随机森林以及boosting算法几乎是解决分类和回归问题中表现最优的算法。
 决策树是一个贪心算法递归地将特征空间划分为两个部分，
 在同一个叶子节点的数据最后会拥有同样的标签。每次划分通过贪心的以获得最大信息增益为目的，
 从可选择的分裂方式中选择最佳的分裂节点。节点不纯度有节点所含类别的同质性来衡量。工具提供为分类提供两种不纯度衡量（基尼不纯度和熵），
 为回归提供一种不纯度衡量（方差）。
 spark.ml支持二分类、多分类以及回归的决策树算法，适用于连续特征以及类别特征。另外，对于分类问题，
 工具可以返回属于每种类别的概率（类别条件概率），对于回归问题工具可以返回预测在偏置样本上的方差。
 参数：
 checkpointInterval:类型：整数型。含义：设置检查点间隔（>=1），或不设置检查点（-1）。
 featuresCol:类型：字符串型。含义：特征列名。
 impurity:类型：字符串型。含义：计算信息增益的准则（不区分大小写）。
 labelCol:类型：字符串型。含义：标签列名。
 maxBins:类型：整数型。含义：连续特征离散化的最大数量，以及选择每个节点分裂特征的方式。
 maxDepth:类型：整数型。含义：树的最大深度（>=0）。
 minInfoGain:类型：双精度型。含义：分裂节点时所需最小信息增益。
 minInstancesPerNode:类型：整数型。含义：分裂后自节点最少包含的实例数量。
 predictionCol:类型：字符串型。含义：预测结果列名。
 probabilityCol:类型：字符串型。含义：类别条件概率预测结果列名。
 rawPredictionCol:类型：字符串型。含义：原始预测。
 seed:类型：长整型。含义：随机种子。
 thresholds:类型：双精度数组型。 含义：多分类预测的阀值，以调整预测结果在各个类别的概率
 **/
public class DecisionTreeClassificationModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("DecisionTreeClassificationModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        // Load training data
        Dataset<Row> data = sparkSession.read().format("libsvm").
                load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main\\data\\mllib\\sample_libsvm_data.txt");

        // Index labels, adding metadata to the label column.
        // Fit on whole dataset to include all labels in index.
        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(data);

        // Automatically identify categorical features, and index them.
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
                .fit(data);

        // Split the data into training and test sets (30% held out for testing).
        Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        // Train a DecisionTree model.
        DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures");

        // Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

        // Chain indexers and tree in a Pipeline.
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, dt, labelConverter});

        // Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(trainingData);

        // Make predictions.
        Dataset<Row> predictions = model.transform(testData);

        // Select example rows to display.
        predictions.select("predictedLabel", "label", "features").show(5);

        // Select (prediction, true label) and compute test error.
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test Error = " + (1.0 - accuracy));

        DecisionTreeClassificationModel treeModel =
                (DecisionTreeClassificationModel) (model.stages()[2]);
        System.out.println("Learned classification tree model:\n" + treeModel.toDebugString());
    }
}
