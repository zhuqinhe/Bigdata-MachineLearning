package cn.hoob.recommenddemo.BasicStistics;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.random.RandomRDDs;
import org.apache.spark.mllib.stat.KernelDensity;
import org.apache.spark.sql.SparkSession;
import scala.Array;

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
 * 这里的是核密度估计
 **/
public class KernelDensityEstimation {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("KernelDensityEstimation").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();
        //sparksession to javaSparkContext
        SparkContext sc=sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);

        JavaRDD<Double> data = jsc.parallelize(Arrays.asList(11.0, 22.0, 33.0, 33.0, 555.0));
        KernelDensity kd = new KernelDensity().setSample(data).setBandwidth(3.0);

        double[] densities = kd.estimate(new double[]{11.0, 222.0, 331.0, 33.0, 55.0});
        for(double t:densities){
            System.out.println(t);
        }
    }
}
