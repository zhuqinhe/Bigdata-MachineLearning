package cn.hoob.recommenddemo.BasicStistics;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/***
 *  Basic Statistics
 *  分为：
 *  1、Summery statistic(汇总统计)
 *
 * 2、Correlations(相关系数)
 *
 * 3、Stratified sampling（分层抽样）
 *
 * 4、Hypothesis Testing(假设检验)
 *
 * 5、Random data generation(随机数生成)
 *
 * 6、Kernel density estimation（核密度估计）
 *
 *    这里的是相关系数统计
 *
 *    Correlations，相关度量，目前Spark支持两种相关性系数：皮尔逊相关系数（pearson）和斯皮尔曼等级相关系数（spearman）。
 *    相关系数是用以反映变量之间相关关系密切程度的统计指标。简单的来说就是相关系数绝对值越大（值越接近1或者-1）,当取值为0表示不相关，
 *    取值为(0~-1]表示负相关，取值为(0, 1]表示正相关。则表示数据越可进行线性拟合
 *
 **/
public class CorrelationsStatistics {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("CorrelationsStatistics").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();
        //sparksession to javaSparkContext
        SparkContext sc=sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);

        JavaDoubleRDD seriesX = jsc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 3.0, 5.0));

        JavaDoubleRDD seriesY = jsc.parallelizeDoubles(Arrays.asList(11.0, 22.0, 33.0, 33.0, 555.0));

        Double correlation = Statistics.corr(seriesX.srdd(), seriesY.srdd(), "pearson");

        System.out.println("Correlation pearson is: " + correlation);
        correlation = Statistics.corr(seriesX.srdd(), seriesY.srdd(), "spearman");

        System.out.println("Correlation spearman is: " + correlation);

        JavaRDD<Vector> data = jsc.parallelize(
                Arrays.asList(
                        Vectors.dense(1.0, 10.0, 100.0),
                        Vectors.dense(2.0, 20.0, 200.0),
                        Vectors.dense(5.0, 33.0, 366.0)
                )
        );

        Matrix correlMatrix = Statistics.corr(data.rdd(), "pearson");
        System.out.println("pearson Matrix is:"+correlMatrix.toString());

        correlMatrix = Statistics.corr(data.rdd(), "spearman");

        System.out.println("spearman Matrix is:"+correlMatrix.toString());

    }
}
