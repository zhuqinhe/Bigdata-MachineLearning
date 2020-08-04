package cn.hoob.machine_learning.clustering.BisectingKMeans;

import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 二分K均值算法
 算法介绍：
 二分K均值算法是一种层次聚类算法，使用自顶向下的逼近：所有的观察值开始是一个簇，递归地向下一个层级分裂。
 分裂依据为选择能最大程度降低聚类代价函数（也就是误差平方和）的簇划分为两个簇。以此进行下去，直到簇的数目等于用户给定的数目k为止。
 二分K均值常常比传统K均值算法有更快的计算速度，但产生的簇群与传统K均值算法往往也是不同的。
 BisectingKMeans是一个Estimator，在基础模型上训练得到BisectingKMeansModel。
 参数：
 featuresCol:类型：字符串型。含义：特征列名。
 k:类型：整数型。含义：聚类簇数。
 maxIter:类型：整数型。含义：迭代次数（>=0）。
 predictionCol:类型：字符串型。含义：预测结果列名。
 seed:类型：长整型。含义：随机种子。
 **/
public class BisectingKMeansModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("KMeansModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

         //Load training data
         Dataset<Row> dataset = sparkSession.read().format("libsvm").
                load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main\\data\\mllib\\input\\mllibFromSpark\\kmeans_libsvm_data.txt");
        dataset.show();
        // Trains a bisecting k-means model.
        BisectingKMeans bkm = new BisectingKMeans().setK(2).setSeed(1);
        BisectingKMeansModel model = bkm.fit(dataset);

        // Evaluate clustering.
        double cost = model.computeCost(dataset);
        System.out.println("Within Set Sum of Squared Errors = " + cost);

        // Shows the result.
        System.out.println("Cluster Centers: ");
        Vector[] centers = model.clusterCenters();
        for (Vector center : centers) {
            System.out.println(center);
        }
    }
}
