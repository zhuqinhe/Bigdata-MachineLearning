package cn.hoob.machine_learning.feature_transformation;


import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 VectorIndexer
 算法介绍：
 VectorIndexer解决数据集中的类别特征Vector。它可以自动识别哪些特征是类别型的，并且将原始值转换为类别指标。它的处理流程如下：
 1.获得一个向量类型的输入以及maxCategories参数。
 2.基于原始数值识别哪些特征需要被类别化，其中最多maxCategories需要被类别化。
 3.对于每一个类别特征计算0-based类别指标。
 4.对类别特征进行索引然后将原始值转换为指标。
 索引后的类别特征可以帮助决策树等算法处理类别型特征，并得到较好结果
 **/
public class VectorIndexerDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("IndexToStringDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        Dataset<Row> data = sparkSession.read().format("libsvm").load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main\\data\\mllib\\sample_libsvm_data.txt");

        VectorIndexer indexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexed")
                .setMaxCategories(10);
        VectorIndexerModel indexerModel = indexer.fit(data);

        Map<Integer, Map<Double, Integer>> categoryMaps = indexerModel.javaCategoryMaps();
        System.out.print("Chose " + categoryMaps.size() + " categorical features:");

        for (Integer feature : categoryMaps.keySet()) {
            System.out.print(" " + feature);
        }
        System.out.println();
        Dataset<Row> indexedData = indexerModel.transform(data);
        indexedData.show();
    }
}
