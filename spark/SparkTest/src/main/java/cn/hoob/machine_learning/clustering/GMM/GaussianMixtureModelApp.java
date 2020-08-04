package cn.hoob.machine_learning.clustering.GMM;

import org.apache.spark.ml.clustering.GaussianMixture;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 高斯混合模型
 算法原理：
 混合高斯模型描述数据点以一定的概率服从k种高斯子分布的一种混合分布。Spark.ml使用EM算法给出一组样本的极大似然模型。
 参数：
 featuresCol:类型：字符串型。含义：特征列名。
 k:类型：整数型。含义：混合模型中独立的高斯数目。
 maxIter:类型：整数型。含义：迭代次数（>=0）。
 predictionCol:类型：字符串型。含义：预测结果列名。
 probabilityCol:类型：字符串型。含义：用以预测类别条件概率的列名。
 seed:类型：长整型。含义：随机种子。
 tol:类型：双精度型。含义：迭代算法的收敛性。
 **/
public class GaussianMixtureModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("KMeansModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

         //Load training data
         Dataset<Row> dataset = sparkSession.read().format("libsvm").
                load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main\\data\\mllib\\input\\mllibFromSpark\\kmeans_libsvm_data.txt");

        // Trains a GaussianMixture model
        GaussianMixture gmm = new GaussianMixture().setK(2);
        GaussianMixtureModel model = gmm.fit(dataset);

        // Output the parameters of the mixture model
        for (int i = 0; i < model.getK(); i++) {
            System.out.printf("weight=%f\nmu=%s\nsigma=\n%s\n",
                    model.weights()[i], model.gaussians()[i].mean(), model.gaussians()[i].cov());
        }
    }
}
