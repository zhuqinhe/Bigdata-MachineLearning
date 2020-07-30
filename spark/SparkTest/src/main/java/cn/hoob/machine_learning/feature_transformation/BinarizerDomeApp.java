package cn.hoob.machine_learning.feature_transformation;

import org.apache.spark.ml.feature.Binarizer;
import org.apache.spark.ml.feature.NGram;
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

/**
 Binarizer
 算法介绍：
 二值化是根据阀值将连续数值特征转换为0-1特征的过程。
 Binarizer参数有输入、输出以及阀值。特征值大于阀值将映射为1.0，特征值小于等于阀值将映射为0.0
 **/
public class BinarizerDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("BinarizerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();


        List<Row> data = Arrays.asList(
                RowFactory.create(0, 0.1),
                RowFactory.create(1, 0.8),
                RowFactory.create(2, 0.2)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("feature", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> continuousDataFrame = sparkSession.createDataFrame(data, schema);
        Binarizer binarizer = new Binarizer()
                .setInputCol("feature")
                .setOutputCol("binarized_feature")
                .setThreshold(0.5);
        Dataset<Row> binarizedDataFrame = binarizer.transform(continuousDataFrame);
        binarizedDataFrame.show();
        Dataset<Row> binarizedFeatures = binarizedDataFrame.select("binarized_feature");
        for (Row r : binarizedFeatures.collectAsList()) {
            Double binarized_value = r.getDouble(0);
            System.out.println(binarized_value);
        }
    }
}
