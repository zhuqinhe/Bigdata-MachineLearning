package cn.hoob.recommenddemo.BasicStistics;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.SparkSession;
import org.spark_project.guava.collect.ImmutableMap;
import scala.Tuple2;

import java.io.IOException;
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
 * 这里的是分层抽样
 *
 * 先将总体的单位按某种特征分为若干次级总体（层），然后再从每一层内进行单纯随机抽样，组成一个样本。分层可以提高总体指标估计值的精确度，它可以将一个内部变异很大的总体分成一些内部变异较小的层（次总体）。
 *
 * 每一层内个体变异越小越好，层间变异则越大越好。
 *
 * 分层抽样比单纯随机抽样所得到的结果准确性更高，组织管理更方便，而且它能保证总体中每一层都有个体被抽到。这样除了能估计总体的参数值，还可以分别估计各个层内的情况，因此分层抽样技术常被采用
 **/
public class StratifiedSampling {

    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("CorrelationsStatistics").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();
        //sparksession to javaSparkContext
        SparkContext sc=sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);

        List<Tuple2<Integer, Character>> list = Arrays.asList(
                new Tuple2<>(1, 'a'),
                new Tuple2<>(1, 'b'),
                new Tuple2<>(2, 'c'),
                new Tuple2<>(2, 'd'),
                new Tuple2<>(2, 'e'),
                new Tuple2<>(3, 'f')
        );

        JavaPairRDD<Integer, Character> data = jsc.parallelizePairs(list);

        ImmutableMap<Integer, Double> fractions = ImmutableMap.of(1, 0.1, 2, 0.6, 3, 0.3);

        JavaPairRDD<Integer, Character> approxSample = data.sampleByKey(false, fractions);
        JavaPairRDD<Integer, Character> exactSample = data.sampleByKeyExact(false, fractions);

        System.out.println(approxSample.collect());
        System.out.println(exactSample.collect());
    }

}
