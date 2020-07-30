package cn.hoob.machine_learning.feature_transformation;

import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 PCA
 算法介绍：
 主成分分析是一种统计学方法，它使用正交转换从一系列可能相关的变量中提取线性无关变量集，
 提取出的变量集中的元素称为主成分。
 使用PCA方法可以对变量集合进行降维。下面的示例将会展示如何将5维特征向量转换为3维主成分向量
 **/
public class PCADomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("BinarizerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(Vectors.sparse(5, new int[]{1, 3}, new double[]{1.0, 7.0})),
                RowFactory.create(Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0)),
                RowFactory.create(Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });

        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        PCAModel pca = new PCA()
                .setInputCol("features")
                .setOutputCol("pcaFeatures")
                .setK(3)
                .fit(df);
        Dataset<Row> result = pca.transform(df);
        result.show();
    }
}
