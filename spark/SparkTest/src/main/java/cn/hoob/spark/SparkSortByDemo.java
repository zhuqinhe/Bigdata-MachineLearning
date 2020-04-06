package cn.hoob.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
/**
 * sortBy根据给定的f函数将RDD中的元素进行排序
 * */
public class SparkSortByDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkSortByDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(8,9,7,6,6,9,5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 3);
        Random random = new Random(50);
        //对RDD进行转换，每个元素有两部分组成
        JavaRDD<String> javaRDD1 = javaRDD.map(tp->tp+"#"+random.nextInt(50));
        System.out.println(javaRDD1.collect());
        //按RDD中每个元素的第二部分进行排序
        JavaRDD<String> resultRDD = javaRDD1.sortBy(tp->tp.split("#")[0],false,3);
        System.out.println(resultRDD.collect());
    }

}
