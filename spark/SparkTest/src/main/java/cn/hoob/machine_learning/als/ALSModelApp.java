package cn.hoob.machine_learning.als;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 协同过滤

 算法介绍：

 协同过滤常被用于推荐系统。这类技术目标在于填充“用户－商品”联系矩阵中的缺失项。
 Spark.ml目前支持基于模型的协同过滤，其中用户和商品以少量的潜在因子来描述，用以预测缺失项。
 Spark.ml使用交替最小二乘（ALS）算法来学习这些潜在因子。

 ＊注意基于DataFrame的ALS接口目前仅支持整数型的用户和商品编号。

 显式与隐式反馈

 基于矩阵分解的协同过滤的标准方法中，“用户－商品”矩阵中的条目是用户给予商品的显式偏好，
 例如，用户给电影评级。然而在现实世界中使用时，我们常常只能访问隐式反馈（如意见、点击、购买、喜欢以及分享等），
 在spark.ml中我们使用“隐式反馈数据集的协同过滤“来处理这类数据。本质上来说它不是直接对评分矩阵进行建模，
 而是将数据当作数值来看待，这些数值代表用户行为的观察值（如点击次数，用户观看一部电影的持续时间）。
 这些数值被用来衡量用户偏好观察值的置信水平，而不是显式地给商品一个评分。然后，模型用来寻找可以用来预测用户对商品预期偏好的潜在因子。

 正则化参数
 我们调整正则化参数regParam来解决用户在更新用户因子时产生新评分或者商品更新商品因子时收到的新评分带来的最小二乘问题。
 这个方法叫做“ALS-WR”它降低regParam对数据集规模的依赖，所以我们可以将从部分子集中学习到的最佳参数应用到整个数据集中时获得同样的性能。
 参数：
 alpha:类型：双精度型。含义：隐式偏好中的alpha参数（非负）。
 checkpointInterval:类型：整数型。含义：设置检查点间隔（>=1），或不设置检查点（-1）。
 implicitPrefs:类型：布尔型。含义：特征列名。
 itemCol:类型：字符串型。含义：商品编号列名。
 maxIter:类型：整数型。含义：迭代次数（>=0）。
 nonnegative:类型：布尔型。含义：是否需要非负约束。
 numItemBlocks:类型：整数型。含义：商品数目（正数）。
 numUserBlocks:类型：整数型。含义：用户数目（正数）。
 predictionCol:类型：字符串型。含义：预测结果列名。
 rank:类型：整数型。含义：分解矩阵的排名（正数）。
 ratingCol:类型：字符串型。含义：评分列名。
 regParam:类型：双精度型。含义：正则化参数（>=0）。
 seed:类型：长整型。含义：随机种子。
 userCol:类型：字符串型。含义：用户列名。
 **/
public class ALSModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("KMeansModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();
        JavaRDD<Rating> ratingsRDD = sparkSession
                .read().textFile("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main\\data\\mllib\\input\\mllibFromSpark\\als\\sample_movielens_ratings.txt").javaRDD()
                .map(new Function<String, Rating>() {
                    public Rating call(String str) {
                        return Rating.parseRating(str);
                    }
                });
        Dataset<Row> ratings = sparkSession.createDataFrame(ratingsRDD, Rating.class);
        Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];

        // Build the recommendation model using ALS on the training data
        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("userId")
                .setItemCol("movieId")
                .setRatingCol("rating");
        ALSModel model = als.fit(training);

        // Evaluate the model by computing the RMSE on the test data
        Dataset<Row> predictions = model.transform(test);

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rating")
                .setPredictionCol("prediction");
        Double rmse = evaluator.evaluate(predictions);
        System.out.println("Root-mean-square error = " + rmse);
    }
}
