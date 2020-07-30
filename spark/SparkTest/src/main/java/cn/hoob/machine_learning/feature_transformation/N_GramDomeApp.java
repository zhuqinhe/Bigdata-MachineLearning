package cn.hoob.machine_learning.feature_transformation;

import org.apache.spark.ml.feature.NGram;
import org.apache.spark.ml.feature.StopWordsRemover;
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
 n-gram
 算法介绍：
 一个n-gram是一个长度为整数n的字序列。NGram可以用来将输入转换为n-gram。
 NGram的输入为一系列字符串（如分词器输出）。参数n决定每个n-gram包含的对象个数。
 结果包含一系列n-gram，其中每个n-gram代表一个空格分割的n个连续字符。
 如果输入少于n个字符串，将没有输出结果
 **/
public class N_GramDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("N_GramDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, Arrays.asList("Hi", "I", "heard", "about", "Spark")),
                RowFactory.create(1.0, Arrays.asList("I", "wish", "Java", "could", "use", "case", "classes")),
                RowFactory.create(2.0, Arrays.asList("Logistic", "regression", "models", "are", "neat"))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField(
                        "words", DataTypes.createArrayType(DataTypes.StringType),
                        false, Metadata.empty())
        });

        Dataset<Row> wordDataFrame = sparkSession.createDataFrame(data, schema);

        NGram ngramTransformer = new NGram().setInputCol("words").setOutputCol("ngrams");

        Dataset<Row> ngramDataFrame = ngramTransformer.transform(wordDataFrame);

        ngramDataFrame.show();
    }
}
