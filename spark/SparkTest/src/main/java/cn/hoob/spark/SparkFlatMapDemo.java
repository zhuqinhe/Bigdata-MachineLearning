package cn.hoob.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
/**
 * 将函数应用于RDD中的每个元素，将返回的迭代器的所有内容构成新的RDD，
 * 通常用来切分单词。与map的区别是：这个函数返回的值是list的一个，去除原有的格式
 * ***/
public class SparkFlatMapDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkFlatMapDemo").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> lines = context.parallelize(Arrays.asList("hoob ha", "hello ha"));
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        System.out.println(words.collect());
        System.out.println(words.first());
    }
}
