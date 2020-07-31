package cn.hoob.machine_learning.feature_transformation;


import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 StandardScaler
 算法介绍：
 StandardScaler处理Vector数据，标准化每个特征使得其有统一的标准差以及（或者）均值为零。它需要如下参数：
 1. withStd：默认值为真，使用统一标准差方式。
 2. withMean：默认为假。此种方法将产出一个稠密输出，所以不适用于稀疏输入。
 StandardScaler是一个Estimator，它可以fit数据集产生一个StandardScalerModel，用来计算汇总统计。
 然后产生的模可以用来转换向量至统一的标准差以及（或者）零均值特征。注意如果特征的标准差为零，则该特征在向量中返回的默认值为0.0。
 **/
public class StandardScalerDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("StandardScalerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        Dataset<Row> dataFrame = sparkSession.read().format("libsvm").
                load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main\\data\\mllib\\sample_libsvm_data.txt");

        StandardScaler scaler = new StandardScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures")
                .setWithStd(true)
                .setWithMean(false);

        StandardScalerModel scalerModel = scaler.fit(dataFrame);

        Dataset<Row> scaledData = scalerModel.transform(dataFrame);
        scaledData.show();
    }
}
