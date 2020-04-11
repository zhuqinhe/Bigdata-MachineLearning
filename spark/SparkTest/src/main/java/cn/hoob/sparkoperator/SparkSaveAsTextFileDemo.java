package cn.hoob.sparkoperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
/**
 * saveAsTextFile用于将RDD以文本文件的格式存储到文件系统中
 * **/
public class SparkSaveAsTextFileDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkSaveAsTextFileDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(5,3,7,7,8,9,1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data,3);
        javaRDD.saveAsTextFile("/hoob/test");
    }
}
