package cn.hoob.machine_learning.model_selection_and_debugging;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Arrays;

/**
 交叉验证
 方法思想：
 CrossValidator将数据集划分为若干子集分别地进行训练和测试。
 如当k＝3时，CrossValidator产生3个训练数据与测试数据对，每个数据对使用2/3的数据来训练，1/3的数据来测试。
 对于一组特定的参数表，CrossValidator计算基于三组不同训练数据与测试数据对训练得到的模型的评估准则的平均值。
 确定最佳参数表后，CrossValidator最后使用最佳参数表基于全部数据来重新拟合估计器。
 示例：
 注意对参数网格进行交叉验证的成本是很高的。如下面例子中，参数网格hashingTF.numFeatures有3个值，
 lr.regParam有2个值，CrossValidator使用2折交叉验证。这样就会产生(3*2)*2=12中不同的模型需要进行训练。
 在实际的设置中，通常有更多的参数需要设置，且我们可能会使用更多的交叉验证折数（3折或者10折都是经使用的）。
 所以CrossValidator的成本是很高的，尽管如此，比起启发式的手工验证，交叉验证仍然是目前存在的参数选择方法中非常有用的一种。
 **/
public class CrossValidatorModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("BinarizerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();
        // Prepare training documents, which are labeled.
        Dataset<Row> training = sparkSession.createDataFrame(Arrays.asList(
                new JavaLabeledDocument(0L, "a b c d e spark", 1.0),
                new JavaLabeledDocument(1L, "b d", 0.0),
                new JavaLabeledDocument(2L,"spark f g h", 1.0),
                new JavaLabeledDocument(3L, "hadoop mapreduce", 0.0),
                new JavaLabeledDocument(4L, "b spark who", 1.0),
                new JavaLabeledDocument(5L, "g d a y", 0.0),
                new JavaLabeledDocument(6L, "spark fly", 1.0),
                new JavaLabeledDocument(7L, "was mapreduce", 0.0),
                new JavaLabeledDocument(8L, "e spark program", 1.0),
                new JavaLabeledDocument(9L, "a e c l", 0.0),
                new JavaLabeledDocument(10L, "spark compile", 1.0),
                new JavaLabeledDocument(11L, "hadoop software", 0.0)
        ), JavaLabeledDocument.class);

// Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words");
        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(1000)
                .setInputCol(tokenizer.getOutputCol())
                .setOutputCol("features");
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.01);
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {tokenizer, hashingTF, lr});


        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(hashingTF.numFeatures(), new int[] {10, 100, 1000})
                .addGrid(lr.regParam(), new double[] {0.1, 0.01})
                .build();


        CrossValidator cv = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(new BinaryClassificationEvaluator())
                .setEstimatorParamMaps(paramGrid).setNumFolds(2);  // Use 3+ in practice

       // Run cross-validation, and choose the best set of parameters.
        CrossValidatorModel cvModel = cv.fit(training);

       // Prepare test documents, which are unlabeled.
        Dataset<Row> test = sparkSession.createDataFrame(Arrays.asList(
                new JavaDocument(4L, "spark i j k"),
                new JavaDocument(5L, "l m n"),
                new JavaDocument(6L, "mapreduce spark"),
                new JavaDocument(7L, "apache hadoop")
        ), JavaDocument.class);

        // Make predictions on test documents. cvModel uses the best model found (lrModel).
        Dataset<Row> predictions = cvModel.transform(test);
        for (Row r : predictions.select("id", "text", "probability", "prediction").collectAsList()) {
            System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> prob=" + r.get(2)
                    + ", prediction=" + r.get(3));
        }
    }
}
