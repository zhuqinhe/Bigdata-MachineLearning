package cn.hoob.machine_learning.feature_extraction;

import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Countvectorizer
 * 算法介绍：
 * Countvectorizer和Countvectorizermodel旨在通过计数来将一个文档转换为向量。
 * 当不存在先验字典时，Countvectorizer可作为Estimator来提取词汇，并生成一个Countvectorizermodel。
 * 该模型产生文档关于词语的稀疏表示，其表示可以传递给其他算法如LDA。
 * 在fitting过程中，countvectorizer将根据语料库中的词频排序选出前vocabsize个词。
 * 一个可选的参数minDF也影响fitting过程中，它指定词汇表中的词语在文档中最少出现的次数。
 * 另一个可选的二值参数控制输出向量，如果设置为真那么所有非零的计数为1。这对于二值型离散概率模型非常有用
 **/
public class CountvectorizerDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("CountvectorizerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        // Input data: Each row is a bag of words from a sentence or document.
        List<Row> data = Arrays.asList(
                RowFactory.create(Arrays.asList("a", "b", "c")),
                RowFactory.create(Arrays.asList("a", "b", "b", "c", "a"))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, true),
                        false, Metadata.empty())
        });
        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        // fit a CountVectorizerModel from the corpus
        CountVectorizerModel cvModel = new CountVectorizer()
                .setInputCol("text")
                .setOutputCol("feature")
                .setVocabSize(3)
                .setMinDF(2).fit(df);

       // alternatively, define CountVectorizerModel with a-priori vocabulary
        CountVectorizerModel cvm = new CountVectorizerModel(new String[]{"a", "b", "c"})
                .setInputCol("text")
                .setOutputCol("feature");

        cvModel.transform(df).show();
    }
}
