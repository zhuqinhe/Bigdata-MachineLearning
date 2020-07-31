package cn.hoob.machine_learning.feature_transformation;


import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
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
 OneHotEncoder
 算法介绍：
 独热编码将标签指标映射为二值向量，其中最多一个单值。
 这种编码被用于将种类特征使用到需要连续特征的算法，如逻辑回归等
 **/
public class OneHotEncoderDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("IndexToStringDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(0, "a"),
                RowFactory.create(1, "b"),
                RowFactory.create(2, "c"),
                RowFactory.create(3, "a"),
                RowFactory.create(4, "a"),
                RowFactory.create(5, "c")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("category", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("category")
                .setOutputCol("categoryIndex")
                .fit(df);
        Dataset<Row> indexed = indexer.transform(df);

        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCol("categoryIndex")
                .setOutputCol("categoryVec");
        Dataset<Row> encoded = encoder.transform(indexed);
        encoded.select("id", "categoryVec").show();
    }
}
