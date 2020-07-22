package cn.hoob.recommenddemo.BasicStistics;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Arrays;
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
 *    这里的是假设检验
 *    MLlib当前支持用于判断拟合度或者独立性的Pearson卡方(chi-squared ( χ2) )检验。
 *    不同的输入类型决定了是做拟合度检验还是独立性检验。
 *    拟合度检验要求输入为Vector,
 *    独立性检验要求输入是Matrix
 *
 **/
public class HypothesisTesting {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("HypothesisTesting").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();
        //sparksession to javaSparkContext
        SparkContext sc=sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);


        Vector vec = Vectors.dense(0.1, 0.15, 0.2, 0.3, 0.25);

        ChiSqTestResult goodnessOfFitTestResult = Statistics.chiSqTest(vec);
        System.out.println(goodnessOfFitTestResult + "\n");

        Matrix mat = Matrices.dense(3, 2, new double[]{1.0, 3.0, 5.0, 2.0, 4.0, 6.0});

        ChiSqTestResult independenceTestResult = Statistics.chiSqTest(mat);
        System.out.println(independenceTestResult + "\n");


        JavaRDD<LabeledPoint> obs = jsc.parallelize(
                Arrays.asList(
                        new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0)),
                        new LabeledPoint(1.0, Vectors.dense(1.0, 2.0, 0.0)),
                        new LabeledPoint(-1.0, Vectors.dense(-1.0, 0.0, -0.5))
                )
        );

        ChiSqTestResult[] featureTestResults = Statistics.chiSqTest(obs.rdd());
        int i = 1;
        for (ChiSqTestResult result : featureTestResults) {
            System.out.println("Column " + i + ":");
            System.out.println(result + "\n");  // summary of the test
            i++;
        }
    }
}
