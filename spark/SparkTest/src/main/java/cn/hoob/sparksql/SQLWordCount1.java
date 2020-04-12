package cn.hoob.sparksql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * Created by zx on 2017/10/13.
 */
public class SQLWordCount1 {
    public static void main(String[] args) throws Exception {

        //创建SparkSession
        SparkSession sparkSession= SparkSession.builder().appName("SQLWordCount1").master("local[*]").getOrCreate();

        //(指定以后从哪里)读数据，是lazy
        //Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
        //dataset只有一列，默认这列叫value
        Dataset<String> dataset= sparkSession.read().textFile("hdfs://node1:9000/wordcount/input");
        //由于Dataset接口没有提供Iterator，无法实现相关逻辑，这里换成rdd来实现
        JavaRDD<String> wordsrdd= dataset.toJavaRDD().flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        //JavaRDD 转化成dataset
        //基础类型直接转化，自定义
       //Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<String> wordsDateset = sparkSession.createDataset(wordsrdd.rdd(),Encoders.STRING());
        //使用DataSet的API（DSL）
        Dataset<Row> rowDataset=wordsDateset.groupBy(wordsDateset.col("value").alias("word")).count().alias("count");

        Dataset<Row> sortDataset=rowDataset.orderBy(rowDataset.apply("count").desc());

        sortDataset.show();
        sparkSession.stop();

    }
}
