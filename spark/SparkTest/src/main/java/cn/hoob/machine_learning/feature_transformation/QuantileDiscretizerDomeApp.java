package cn.hoob.machine_learning.feature_transformation;


import org.apache.spark.ml.feature.QuantileDiscretizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
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

import static org.apache.spark.sql.types.DataTypes.*;

/**
 QuantileDiscretizer
 算法介绍：
 QuantileDiscretizer讲连续型特征转换为分级类别特征。分级的数量由numBuckets参数决定。
 分级的范围有渐进算法决定。渐进的精度由relativeError参数决定。
 当relativeError设置为0时，将会计算精确的分位点（计算代价较高）。分级的上下边界为负无穷到正无穷，覆盖所有的实数值
 **/
public class QuantileDiscretizerDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("QuantileDiscretizerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(0, 18.0),
                RowFactory.create(1, 19.0),
                RowFactory.create(2, 8.0),
                RowFactory.create(3, 5.0),
                RowFactory.create(4, 2.2)
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("hour", DataTypes.DoubleType, false, Metadata.empty())
        });

        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        QuantileDiscretizer discretizer = new QuantileDiscretizer()
                .setInputCol("hour")
                .setOutputCol("result")
                .setNumBuckets(3);

        Dataset<Row> result = discretizer.fit(df).transform(df);
        result.show();

    }
}
