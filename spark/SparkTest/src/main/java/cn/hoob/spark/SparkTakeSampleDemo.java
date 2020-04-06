package cn.hoob.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 *takeSample函数返回一个数组，在数据集中随机采样 num 个元素组成
 * **/
public class SparkTakeSampleDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkTakeOrderedDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(9,8,7,6,5, 1, 0, 4, 4, 2, 2,1);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 3);
        System.out.println("takeSample" + javaRDD.takeSample(true,2));
        System.out.println("takeSample" + javaRDD.takeSample(true,2,100));
        //返回20个元素
        System.out.println("takeSample" + javaRDD.takeSample(true,20,100));
        //返回5个元素
        System.out.println("takeSample" + javaRDD.takeSample(false,5,100));
    }
}
