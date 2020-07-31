package cn.hoob.machine_learning.feature_transformation;


import org.apache.spark.ml.feature.MaxAbsScaler;
import org.apache.spark.ml.feature.MaxAbsScalerModel;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 MaxAbsScaler
 算法介绍：
 MaxAbsScaler使用每个特征的最大值的绝对值将输入向量的特征值转换到[-1,1]之间。
 因为它不会转移／集中数据，所以不会破坏数据的稀疏性。
 下面的示例展示如果读入一个libsvm形式的数据以及调整其特征值到[-1,1]之间
 **/
public class MaxAbsScalerDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("MinMaxScalerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        Dataset<Row> dataFrame = sparkSession.read().format("libsvm").
                load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main\\data\\mllib\\sample_libsvm_data.txt");

        MaxAbsScaler scaler = new MaxAbsScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures");

        MaxAbsScalerModel scalerModel = scaler.fit(dataFrame);

        Dataset<Row> scaledData = scalerModel.transform(dataFrame);
        scaledData.show();
    }
}
