package cn.hoob.machine_learning.clustering.KMeans;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;

/**
 K均值（K-means）算法
 算法介绍：
 K-means是一个常用的聚类算法来将数据点按预定的簇数进行聚集。
 K-means算法的基本思想是：以空间中k个点为中心进行聚类，对最靠近他们的对象归类。
 通过迭代的方法，逐次更新各聚类中心的值，直至得到最好的聚类结果。
 假设要把样本集分为c个类别，算法描述如下：
 （1）适当选择c个类的初始中心；
 （2）在第k次迭代中，对任意一个样本，求其到c个中心的距离，将该样本归到距离最短的中心所在的类；
 （3）利用均值等方法更新该类的中心值；
 （4）对于所有的c个聚类中心，如果利用（2）（3）的迭代法更新后，值保持不变，则迭代结束，否则继续迭代。
 MLlib工具包含并行的K-means++算法，称为kmeans||。Kmeans是一个Estimator，它在基础模型之上产生一个KMeansModel。
 参数：
 featuresCol:类型：字符串型。含义：特征列名。
 k:类型：整数型。含义：聚类簇数。
 maxIter:类型：整数型。含义：迭代次数（>=0）。
 predictionCol:类型：字符串型。含义：预测结果列名。
 seed:类型：长整型。含义：随机种子。
 tol:类型：双精度型。含义：迭代算法的收敛性。
 **/
public class KMeansModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("KMeansModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> stringRdd = jsc.textFile("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main\\data\\mllib\\input\\mllibFromSpark\\kmeans_data.txt");

        JavaRDD<Vector> dRdd=stringRdd.map(line-> {
            String[] arr = line.split(" ");
            ArrayList<Double> list = new ArrayList<Double>();
            double[] d = new double[3];
            int i = 0;
            for (String num : arr) {
                d[i] = new Double(num);
                i++;
            }
            return Vectors.dense(d);
        });
        int numClusters = 2;
        int numIterations = 20;
        KMeansModel model = KMeans.train(dRdd.rdd(), numClusters, numIterations);

        // Evaluate clustering by computing Within Set Sum of Squared Errors.
        double WSSSE = model.computeCost(dRdd.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

       // Shows the result.
        Vector[] centers = model.clusterCenters();
        System.out.println("Cluster Centers: ");
        for (Vector center: centers) {
            System.out.println(center);
        }
    }
}
