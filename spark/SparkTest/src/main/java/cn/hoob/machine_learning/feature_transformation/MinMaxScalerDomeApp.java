package cn.hoob.machine_learning.feature_transformation;


import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 MinMaxScaler
 算法介绍：
 MinMaxScaler通过重新调节大小将Vector形式的列转换到指定的范围内，通常为[0,1]，它的参数有：
 1. min：默认为0.0，为转换后所有特征的下边界。
 2. max：默认为1.0，为转换后所有特征的下边界。
 MinMaxScaler计算数据集的汇总统计量，并产生一个MinMaxScalerModel。该模型可以将独立的特征的值转换到指定的范围内。
 注意因为零值转换后可能变为非零值，所以即便为稀疏输入，输出也可能为稠密向量
 **/
public class MinMaxScalerDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("MinMaxScalerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        Dataset<Row> dataFrame = sparkSession.read().format("libsvm").
                load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main\\data\\mllib\\sample_libsvm_data.txt");

        MinMaxScaler scaler = new MinMaxScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures");

        // Compute summary statistics and generate MinMaxScalerModel
        MinMaxScalerModel scalerModel = scaler.fit(dataFrame);

        // rescale each feature to range [min, max].
        Dataset<Row> scaledData = scalerModel.transform(dataFrame);
        scaledData.show();
    }
}
