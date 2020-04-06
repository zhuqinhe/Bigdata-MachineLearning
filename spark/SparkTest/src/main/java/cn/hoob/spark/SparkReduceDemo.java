package cn.hoob.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 根据映射函数f，对RDD中的元素进行二元计算（满足交换律和结合律），返回计算结果。
 * 从源码中可以看出，reduce函数相当于对RDD中的元素进行reduceLeft函数操作，reduceLeft函数是从列表的左边往右边应用reduce函数；
 * 之后，在driver端对结果进行合并处理，因此，如果分区数量过多或者自定义函数过于复杂，对driver端的负载比较重
 * */
public class SparkReduceDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkReducedemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data,3);

        Integer reduceRDD = javaRDD.reduce((v1,v2)->v1+v2);
         System.out.println(reduceRDD);
    }
}
