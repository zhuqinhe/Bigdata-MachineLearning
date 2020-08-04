package cn.hoob.machine_learning.model_selection_and_debugging;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.tuning.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Arrays;

/**
 训练验证分裂
 除了交叉验证以外，Spark还提供训练验证分裂用以超参数调整。和交叉验证评估K次不同，训练验证分裂只对每组参数评估一次。
 因此它计算代价更低，但当训练数据集不是足够大时，其结果可靠性不高。
 与交叉验证不同，训练验证分裂仅需要一个训练数据与验证数据对。使用训练比率参数将原始数据划分为两个部分。
 如当训练比率为0.75时，训练验证分裂使用75%数据以训练，25%数据以验证。
 与交叉验证相同，确定最佳参数表后，训练验证分裂最后使用最佳参数表基于全部数据来重新拟合估计器。
 **/
public class TrainValidationSplitModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("BinarizerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();
        Dataset<Row> data = sparkSession.read().format("libsvm").
        load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main\\data\\mllib\\input\\mllibFromSpark\\sample_linear_regression_data.txt");


// Prepare training and test data.
        Dataset<Row>[] splits = data.randomSplit(new double[] {0.9, 0.1}, 12345);
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];

        LinearRegression lr = new LinearRegression();

// We use a ParamGridBuilder to construct a grid of parameters to search over.
// TrainValidationSplit will try all combinations of values and determine best model using
// the evaluator.
        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(lr.regParam(), new double[] {0.1, 0.01})
                .addGrid(lr.fitIntercept())
                .addGrid(lr.elasticNetParam(), new double[] {0.0, 0.5, 1.0})
                .build();

// In this case the estimator is simply the linear regression.
// A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(lr)
                .setEvaluator(new RegressionEvaluator())
                .setEstimatorParamMaps(paramGrid)
                .setTrainRatio(0.8);  // 80% for training and the remaining 20% for validation

// Run train validation split, and choose the best set of parameters.
        TrainValidationSplitModel model = trainValidationSplit.fit(training);

// Make predictions on test data. model is the model with combination of parameters
// that performed best.
        model.transform(test)
                .select("features", "label", "prediction")
                .show();
    }
}
