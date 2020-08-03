package cn.hoob.machine_learning.classify.GBTClassification;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * 梯度迭代树
 * 算法简介：
 * 梯度提升树是一种决策树的集成算法。它通过反复迭代训练决策树来最小化损失函数。决策树类似，梯度提升树具有可处理类别特征、
 * 易扩展到多分类问题、不需特征缩放等性质。Spark.ml通过使用现有decision tree工具来实现。
 * 梯度提升树依次迭代训练一系列的决策树。在一次迭代中，算法使用现有的集成来对每个训练实例的类别进行预测，
 * 然后将预测结果与真实的标签值进行比较。通过重新标记，来赋予预测结果不好的实例更高的权重。所以，在下次迭代中，决策树会对先前的错误进行修正。
 * 对实例标签进行重新标记的机制由损失函数来指定。每次迭代过程中，梯度迭代树在训练数据上进一步减少损失函数的值。
 * spark.ml为分类问题提供一种损失函数（Log Loss），为回归问题提供两种损失函数（平方误差与绝对误差）。
 * Spark.ml支持二分类以及回归的随机森林算法，适用于连续特征以及类别特征。
 * ＊注意梯度提升树目前不支持多分类问题。
 * 参数：
 * checkpointInterval:类型：整数型。含义：设置检查点间隔（>=1），或不设置检查点（-1）。
 * featuresCol:类型：字符串型。含义：特征列名。
 * impurity:类型：字符串型。含义：计算信息增益的准则（不区分大小写）。
 * labelCol:类型：字符串型。含义：标签列名。
 * lossType:类型：字符串型。含义：损失函数类型。
 * maxBins:类型：整数型。含义：连续特征离散化的最大数量，以及选择每个节点分裂特征的方式。
 * maxDepth:类型：整数型。含义：树的最大深度（>=0）。
 * maxIter:类型：整数型。含义：迭代次数（>=0）。
 * minInfoGain:类型：双精度型。含义：分裂节点时所需最小信息增益。
 * minInstancesPerNode:类型：整数型。含义：分裂后自节点最少包含的实例数量。
 * predictionCol:类型：字符串型。含义：预测结果列名。
 * rawPredictionCol:类型：字符串型。含义：原始预测。
 * seed:类型：长整型。含义：随机种子。
 * subsamplingRate:类型：双精度型。含义：学习一棵决策树使用的训练数据比例，范围[0,1]。
 * stepSize:类型：双精度型。含义：每次迭代优化步长。
 **/
public class GBTClassificationModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("GBTClassificationModelApp").
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
// Set maxCategories so features with > 4 distinct values are treated as continuous.
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4)
                .fit(data);

// Split the data into training and test sets (30% held out for testing)
        Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

// Train a GBT model.
        GBTClassifier gbt = new GBTClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures")
                .setMaxIter(10);

// Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

// Chain indexers and GBT in a Pipeline.
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, gbt, labelConverter});

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

        GBTClassificationModel gbtModel = (GBTClassificationModel) (model.stages()[2]);
        System.out.println("Learned classification GBT model:\n" + gbtModel.toDebugString());
    }
}
