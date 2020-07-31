package cn.hoob.machine_learning.feature_transformation;


import org.apache.spark.ml.feature.IndexToString;
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

import static org.apache.spark.sql.types.DataTypes.*;

/**
 IndexToString
 算法介绍：
 与StringIndexer对应，IndexToString将指标标签映射回原始字符串标签。
 一个常用的场景是先通过StringIndexer产生指标标签，然后使用指标标签进行训练，
 最后再对预测结果使用IndexToString来获取其原始的标签字符串
 **/
public class IndexToStringDomeApp {
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

        IndexToString converter = new IndexToString()
                .setInputCol("categoryIndex")
                .setOutputCol("originalCategory");
        Dataset<Row> converted = converter.transform(indexed);
        converted.select("id", "originalCategory").show();
    }
}
