package cn.hoob.machine_learning.feature_transformation;


import org.apache.spark.ml.feature.Bucketizer;
import org.apache.spark.ml.feature.MaxAbsScaler;
import org.apache.spark.ml.feature.MaxAbsScalerModel;
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
 Bucketizer
 算法介绍：
 Bucketizer将一列连续的特征转换为特征区间，区间由用户指定。参数如下：
 1. splits：分裂数为n+1时，将产生n个区间。除了最后一个区间外，每个区间范围［x,y］由分裂的x，y决定。分裂必须是严格递增的。
 在分裂指定外的值将被归为错误。两个分裂的例子为Array(Double.NegativeInfinity,0.0, 1.0, Double.PositiveInfinity)以及Array(0.0, 1.0, 2.0)。
 注意，当不确定分裂的上下边界时，应当添加Double.NegativeInfinity和Double.PositiveInfinity以免越界
 **/
public class BucketizerDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("BucketizerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        double[] splits = {Double.NEGATIVE_INFINITY, -0.5, 0.0, 0.5, Double.POSITIVE_INFINITY};

        List<Row> data = Arrays.asList(
                RowFactory.create(-0.5),
                RowFactory.create(-0.3),
                RowFactory.create(0.0),
                RowFactory.create(0.2)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("features", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> dataFrame = sparkSession.createDataFrame(data, schema);

        Bucketizer bucketizer = new Bucketizer()
                .setInputCol("features")
                .setOutputCol("bucketedFeatures")
                .setSplits(splits);

        Dataset<Row> bucketedData = bucketizer.transform(dataFrame);
        bucketedData.show();
    }
}
