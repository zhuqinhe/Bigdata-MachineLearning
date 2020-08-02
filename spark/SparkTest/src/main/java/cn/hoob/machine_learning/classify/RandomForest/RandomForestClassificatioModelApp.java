package cn.hoob.machine_learning.classify.RandomForest;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 随机森林分类器：

 算法简介：
 随机森林是决策树的集成算法。随机森林包含多个决策树来降低过拟合的风险。随机森林同样具有易解释性、可处理类别特征、
 易扩展到多分类问题、不需特征缩放等性质。
 随机森林分别训练一系列的决策树，所以训练过程是并行的。因算法中加入随机过程，所以每个决策树又有少量区别。
 通过合并每个树的预测结果来减少预测的方差，提高在测试集上的性能表现。
 随机性体现
 1.每次迭代时，对原始数据进行二次抽样来获得不同的训练数据。
 2.对于每个树节点，考虑不同的随机特征子集来进行分裂。
 除此之外，决策时的训练过程和单独决策树训练过程相同。
 对新实例进行预测时，随机森林需要整合其各个决策树的预测结果。回归和分类问题的整合的方式略有不同。分类问题采取投票制，每个决策树投票给一个类别，获得最多投票的类别为最终结果。回归问题每个树得到的预测结果为实数，最终的预测结果为各个树预测结果的平均值。
 spark.ml支持二分类、多分类以及回归的随机森林算法，适用于连续特征以及类别特征。
 参数：
 checkpointInterval:类型：整数型。含义：设置检查点间隔（>=1），或不设置检查点（-1）。
 featureSubsetStrategy:类型：字符串型。含义：每次分裂候选特征数量。
 featuresCol:类型：字符串型。含义：特征列名。
 impurity:类型：字符串型。含义：计算信息增益的准则（不区分大小写）。
 labelCol:类型：字符串型。含义：标签列名。
 maxBins:类型：整数型。含义：连续特征离散化的最大数量，以及选择每个节点分裂特征的方式。
 maxDepth:类型：整数型。含义：树的最大深度（>=0）。
 minInfoGain:类型：双精度型。含义：分裂节点时所需最小信息增益。
 minInstancesPerNode:类型：整数型。含义：分裂后自节点最少包含的实例数量。
 numTrees:类型：整数型。含义：训练的树的数量。
 predictionCol:类型：字符串型。含义：预测结果列名。
 probabilityCol:类型：字符串型。含义：类别条件概率预测结果列名。
 rawPredictionCol:类型：字符串型。含义：原始预测。
 seed:类型：长整型。含义：随机种子
 subsamplingRate:类型：双精度型。含义：学习一棵决策树使用的训练数据比例，范围[0,1]。
 thresholds:类型：双精度数组型。含义：多分类预测的阀值，以调整预测结果在各个类别的概率。
 **/
public class RandomForestClassificatioModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("RandomForestClassificatioModelApp").
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
        Dataset<Row>[] splits = data.randomSplit(new double[] {0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

// Train a RandomForest model.
        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures");

// Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

// Chain indexers and forest in a Pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {labelIndexer, featureIndexer, rf, labelConverter});

// Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(trainingData);

// Make predictions.
        Dataset<Row> predictions = model.transform(testData);

// Select example rows to display.
        predictions.select("predictedLabel", "label", "features").show(5);

// Select (prediction, true label) and compute test error
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test Error = " + (1.0 - accuracy));

        RandomForestClassificationModel rfModel = (RandomForestClassificationModel)(model.stages()[2]);
        System.out.println("Learned classification forest model:\n" + rfModel.toDebugString());
    }
}
