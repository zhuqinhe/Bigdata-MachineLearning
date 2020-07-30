package cn.hoob.machine_learning.feature_transformation;

import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
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
 StopWordsRemover
 算法介绍：
 停用词为在文档中频繁出现，但未承载太多意义的词语，他们不应该被包含在算法输入中。
 StopWordsRemover的输入为一系列字符串（如分词器输出），输出中删除了所有停用词。
 停用词表由stopWords参数提供。一些语言的默认停用词表可以通过StopWordsRemover.loadDefaultStopWords(language)调用。
 布尔参数caseSensitive指明是否区分大小写（默认为否）
 **/
public class StopWordsRemoverDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("StopWordsRemoverDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("raw")
                .setOutputCol("filtered");

        List<Row> data = Arrays.asList(
                RowFactory.create(Arrays.asList("I", "saw", "the", "red", "baloon")),
                RowFactory.create(Arrays.asList("Mary", "had", "a", "little", "lamb"))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField(
                        "raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
        });

        Dataset<Row> dataset = sparkSession.createDataFrame(data, schema);
        remover.transform(dataset).show();
    }
}
