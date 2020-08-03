package cn.hoob.machine_learning.regressor.IsotonicRegression;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.regression.IsotonicRegression;
import org.apache.spark.ml.regression.IsotonicRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;

import java.io.IOException;
import java.util.List;

/**
 保序回归
 算法介绍：
 保序回归是回归算法的一种。保序回归给定一个有限的实数集合 代表观察到的响应，以及 代表未知的响应值，训练一个模型来最小化下列方程：
 其中 ， 为权重是正值。其结果方程称为保序回归，而且其解是唯一的。它可以被视为有顺序约束下的最小二乘法问题。实
 际上保序回归在拟合原始数据点时是一个单调函数。我们实现池旁者算法，它使用并行保序回归。训练数据是DataFrame格式，包含标签、特征值以及权重三列。另外保序算法还有一个参数名为isotonic，其默认值为真，它指定保序回归为保序（单调递增）或者反序（单调递减）。
 训练返回一个保序回归模型，可以被用于来预测已知或者未知特征值的标签。保序回归的结果是分段线性函数，预测规则如下：
 1.如果预测输入与训练中的特征值完全匹配，则返回相应标签。如果一个特征值对应多个预测标签值，则返回其中一个，具体是哪一个未指定。
 2.如果预测输入比训练中的特征值都高（或者都低），则相应返回最高特征值或者最低特征值对应标签。
 如果一个特征值对应多个预测标签值，则相应返回最高值或者最低值。
 3.如果预测输入落入两个特征值之间，则预测将会是一个分段线性函数，其值由两个最近的特征值的预测值计算得到。
 如果一个特征值对应多个预测标签值，则使用上述两种情况中的处理方式解决。
 参数：
 featuresIndex:类型：整数型。含义：当特征列维向量时提供索引值，否则不进行处理。
 featuresCol:类型：字符串型。含义：特征列名。
 isotonic:类型：布尔型。含义：输出序列为保序/增序（真）或者反序/降序（假）。
 labelCol:类型：字符串型。含义：标签列名。
 predictionCol:类型：字符串型。含义：预测结果列名。
 weightCol:类型：字符串型。含义：列权重。
 **/
public class IsotonicRegressionModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("RandomForestRegressionModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "1").enableHiveSupport().getOrCreate();

        // Load training data   libsvm
        Dataset<Row> dataset = sparkSession.read().format("libsvm").
                load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main" +
                       "\\data\\mllib\\input\\classification\\dt.txt");
        //dataset.show();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> stringRdd = jsc.textFile("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main\\data\\mllib\\input\\classification\\sample_isotonic_regression_data.txt");
        // Create label, feature, weight tuples from input data with weight set to default value 1.0.
        JavaRDD<Model>dRdd=stringRdd.map(line->{
            String[] ss=line.split(",");
            return new Model(Double.parseDouble(ss[0]),Double.parseDouble(ss[1]),1.0);
        });
        dataset=sparkSession.createDataFrame(dRdd, Model.class);
        IsotonicRegression ir = new IsotonicRegression();
        IsotonicRegressionModel model = ir.fit(dataset);

        System.out.println("Boundaries in increasing order: " + model.boundaries());
        System.out.println("Predictions associated with the boundaries: " + model.predictions());

         // Makes predictions.
        model.transform(dataset).show();
    }
}
