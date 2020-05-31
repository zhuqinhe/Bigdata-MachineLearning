package recommenddemo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/***
 * 显示
 numBlocks 是用于并行化计算的用户和商品的分块个数 (默认为10)。
 rank 是模型中隐语义因子的个数（默认为10）。
 maxIter 是迭代的次数（默认为10）。
 regParam 是ALS的正则化参数（默认为1.0）。
 implicitPrefs 决定了是用显性反馈ALS的版本还是用适用隐性反馈数据集的版本（默认是false，即用显性反馈）。
 alpha 是一个针对于隐性反馈 ALS 版本的参数，这个参数决定了偏好行为强度的基准（默认为1.0）。
 nonnegative 决定是否对最小二乘法使用非负的限制（默认为false）
 **/
public class ALSFitModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("ALSFitModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();
        //模型训练数据
        Dataset<Row> traningDateSet = sparkSession.sql("select * from logs_all");
        // 训练集，转为Rating格式
        JavaRDD<DataModel> ratingsRDD = traningDateSet.toJavaRDD().map(row -> {
            return new DataModel(row.getInt(0), row.getInt(1), row.getInt(2));
        });

        Dataset<Row> ratings = sparkSession.createDataFrame(ratingsRDD, DataModel.class);
        Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];
        // 构建模型,不断调试参数
        ALS als = new ALS()
                .setMaxIter(10)
                .setRegParam(0.01)
                .setImplicitPrefs(true)//  false  为显式评分模式，true 为隐式评分模式
                .setUserCol("uid")
                .setItemCol("series")
                .setRatingCol("count");
        ALSModel model = als.fit(training);

        Dataset<Row> predictions = model.transform(test);

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("count")
                .setPredictionCol("prediction");
        //取最优结果时，存下模型
        Double RMSE = evaluator.evaluate(predictions);
        model.save("hdfs://node1:9000/recommend_processed_bestModel/" + RMSE);

        System.out.println("Root-mean-square error = " + RMSE);
    }
}
