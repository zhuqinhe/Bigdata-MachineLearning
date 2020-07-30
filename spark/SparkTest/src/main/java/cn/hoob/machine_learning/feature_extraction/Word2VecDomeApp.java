package cn.hoob.machine_learning.feature_extraction;

import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 算法介绍：
 Word2vec是一个Estimator，它采用一系列代表文档的词语来训练word2vecmodel。该模型将每个词语映射到一个固定大小的向量。
 word2vecmodel使用文档中每个词语的平均数来将文档转换为向量，然后这个向量可以作为预测的特征，来计算文档相似度计算等等。
 在下面的代码段中，我们首先用一组文档，其中每一个文档代表一个词语序列。对于每一个文档，我们将其转换为一个特征向量
 * **/
public class Word2VecDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("Word2VecDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
                RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
                RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" ")))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, true),
                        false, Metadata.empty())
        });
        Dataset<Row> documentDF = sparkSession.createDataFrame(data, schema);

        // Learn a mapping from words to Vectors.
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("text")
                .setOutputCol("result")
                .setVectorSize(3)
                .setMinCount(0);
        Word2VecModel model = word2Vec.fit(documentDF);
        Dataset<Row> result = model.transform(documentDF);
        for (Row r : result.select("result").takeAsList(3)) {
            System.out.println(r);
        }
    }
}
