package cn.hoob.sparkoperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/***
 * 第一个函数是基于第二函数实现的，只是numPartitions默认为partitions.length，partitions为parent RDD的分区。
 *
 * distinct() 功能是 deduplicate RDD 中的所有的重复数据。由于重复数据可能分散在不同的 partition 里面，
 * 因此需要 shuffle 来进行 aggregate 后再去重。然而，shuffle 要求数据类型是 <K, V> 。
 * 如果原始数据只有 Key（比如例子中 record 只有一个整数），那么需要补充成 <K, null> 。
 * 这个补充过程由 map() 操作完成，生成 MappedRDD。然后调用上面的 reduceByKey() 来进行 shuffle，
 * 在 map 端进行 combine，然后 reduce 进一步去重，生成 MapPartitionsRDD。最后，将 <K, null> 还原成 K，仍然由 map() 完成，生成 MappedRDD
 * */
public class SparkDistinctDemo {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 1, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        JavaRDD<Integer> distinctRDD1 = javaRDD.distinct();
        System.out.println(distinctRDD1.collect());
        JavaRDD<Integer> distinctRDD2 = javaRDD.distinct(2);
        System.out.println(distinctRDD2.collect());
    }
}
