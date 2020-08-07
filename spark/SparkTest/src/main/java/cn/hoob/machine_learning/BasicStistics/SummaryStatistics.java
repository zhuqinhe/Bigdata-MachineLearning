package cn.hoob.machine_learning.BasicStistics;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
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
 * 这里的是汇总统计
 **/
public class SummaryStatistics {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("SummaryStatistics").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();
        //sparksession to javaSparkContext
        SparkContext sc=sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);

        List<Vector>vectors=new ArrayList<>();
        //稠密向量
        vectors.add(Vectors.dense(4.0, 5.0, 0.0, 3.0));
        vectors.add(Vectors.dense(6.0, 7.0, 0.0, 8.0));
        //稀疏向量
        vectors.add(Vectors.sparse(4, new int[] {0, 1}, new double[] {1.0,-2.0}));
        vectors.add(Vectors.sparse(4, new int[] {0, 3}, new double[] {9.0, 1.0}));
        JavaRDD<Vector> mat=jsc.parallelize(vectors);
        MultivariateStatisticalSummary summary = Statistics.colStats(mat.rdd());
        System.out.println(summary.mean());
        System.out.println(summary.variance());
        System.out.println(summary.numNonzeros());
    }
}
