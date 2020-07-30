package cn.hoob.machine_learning.feature_transformation;

import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.feature.PolynomialExpansion;
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
 算法介绍：
 多项式扩展通过产生n维组合将原始特征将特征扩展到多项式空间。
 下面的示例会介绍如何将你的特征集拓展到3维多项式空间
 **/
public class PolynomialExpansionDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("PolynomialExpansionDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        PolynomialExpansion polyExpansion = new PolynomialExpansion()
                .setInputCol("features")
                .setOutputCol("polyFeatures")
                .setDegree(3);

        List<Row> data = Arrays.asList(
                RowFactory.create(Vectors.dense(-2.0, 2.3)),
                RowFactory.create(Vectors.dense(0.0, 0.0)),
                RowFactory.create(Vectors.dense(0.6, -1.1))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });

        Dataset<Row> df = sparkSession.createDataFrame(data, schema);
        Dataset<Row> polyDF = polyExpansion.transform(df);

        List<Row> rows = polyDF.select("polyFeatures").takeAsList(3);
        for (Row r : rows) {
            System.out.println(r.get(0));
        }
    }
}
