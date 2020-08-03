package cn.hoob.machine_learning.regressor.AFTSurvivalRegression;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.AFTSurvivalRegression;
import org.apache.spark.ml.regression.AFTSurvivalRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 生存回归（加速失效时间模型）
 算法介绍：
 在spark.ml中，我们实施加速失效时间模型（Acceleratedfailure time），对于截尾数据它是一个参数化生存回归的模型。
 它描述了一个有对数生存时间的模型，所以它也常被称为生存分析的对数线性模型。与比例危险模型不同，因AFT模型中每个实例对目标函数的贡献是独立的，
 其更容易并行化。
 最常用的AFT模型基于韦伯分布的生存时间，生存时间的韦伯分布对应于生存时间对数的极值分布
 可以证明AFT模型是一个凸优化问题，即是说找到凸函数的最小值取决于系数向量以及尺度参数的对数。在工具中实施的优化算法为L-BFGS。
 当使用无拦截的连续非零列训练AFTSurvivalRegressionModel时，Spark MLlib为连续非零列输出零系数。这种处理与R中的生存函数survreg不同。
 参数：
 censorCol:类型：字符串型。含义：检查器列名。
 featuresCol:类型：字符串型。含义：特征列名。
 fitIntercept:类型：布尔型。含义：是否训练拦截对象。
 labelCol:类型：字符串型。含义：标签列名。
 maxIter:类型：整数型。含义：迭代次数（>=0）。
 quantileProbabilities:类型：双精度数组型。含义：分位数概率数组。
 quantilesCol:类型：字符串型。含义：分位数列名。
 stepSize:类型：双精度型。含义：每次迭代优化步长。
 tol:类型：双精度型。含义：迭代算法的收敛性。
 **/
public class AFTSurvivalRegressionModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("AFTSurvivalRegressionModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(1.218, 1.0, Vectors.dense(1.560, -0.605)),
                RowFactory.create(2.949, 0.0, Vectors.dense(0.346, 2.158)),
                RowFactory.create(3.627, 0.0, Vectors.dense(1.380, 0.231)),
                RowFactory.create(0.273, 1.0, Vectors.dense(0.520, 1.151)),
                RowFactory.create(4.199, 0.0, Vectors.dense(0.795, -0.226))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("censor", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> training = sparkSession.createDataFrame(data, schema);
        double[] quantileProbabilities = new double[]{0.3, 0.6};
        AFTSurvivalRegression aft = new AFTSurvivalRegression()
                .setQuantileProbabilities(quantileProbabilities)
                .setQuantilesCol("quantiles");

        AFTSurvivalRegressionModel model = aft.fit(training);

        // Print the coefficients, intercept and scale parameter for AFT survival regression
        System.out.println("Coefficients: " + model.coefficients() + " Intercept: "
                + model.intercept() + " Scale: " + model.scale());
        model.transform(training).show(false);
    }
}
