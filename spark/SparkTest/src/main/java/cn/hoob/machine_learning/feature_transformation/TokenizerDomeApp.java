package cn.hoob.machine_learning.feature_transformation;

import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 Tokenizer（分词器）
 算法介绍：
 Tokenization将文本划分为独立个体（通常为单词）。下面的例子展示了如何把句子划分为单词。
 RegexTokenizer基于正则表达式提供更多的划分选项。默认情况下，参数“pattern”为划分文本的分隔符。
 或者，用户可以指定参数“gaps”来指明正则“patten”表示“tokens”而不是分隔符，这样来为分词结果找到所有可能匹配的情况
 **/
public class TokenizerDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("TokenizerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(0, "Hi I heard about Spark"),
                RowFactory.create(1, "I wish Java could use case classes"),
                RowFactory.create(2, "Logistic,regression,models,are,neat")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> sentenceDataFrame = sparkSession.createDataFrame(data, schema);

        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");

        Dataset<Row> wordsDataFrame = tokenizer.transform(sentenceDataFrame);
        for (Row r : wordsDataFrame.select("words", "label").takeAsList(3)) {
            java.util.List<String> words = r.getList(0);
            for (String word : words)
                System.out.println(word + " ");
        }
        //高级正则分词
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol("sentence")
                .setOutputCol("words")
                .setPattern("\\W");  // alternatively .setPattern("\\w+").setGaps(false);
        wordsDataFrame=regexTokenizer.transform(sentenceDataFrame);
        wordsDataFrame.show();
    }
}
