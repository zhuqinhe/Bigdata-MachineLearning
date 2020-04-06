package cn.hoob.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
/**
 * saveAsObjectFile用于将RDD中的元素序列化成对象，存储到文件中
 * **/
public class SparkSaveAsObjectFileDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkSortByDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(5,3,7,7,8,9,1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data,3);
        javaRDD.saveAsObjectFile("/hoob/test1");
    }
}
