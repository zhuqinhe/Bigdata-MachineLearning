package cn.hoob.sparkoperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
/**
 *foreach用于遍历RDD,将函数f应用于每一个元素
 * ***/
public class SparkForeachDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkForeachDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<String> data = Arrays.asList("7","5", "1","4","3", "1","6", "2", "2","8");
        JavaRDD<String> javaRDD = jsc.parallelize(data,5);
        javaRDD.foreach(tp->System.out.println(tp));
    }
}
