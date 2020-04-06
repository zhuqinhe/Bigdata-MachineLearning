package cn.hoob.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
/**
 * 从源码中可以看出，先是进行map操作转化为(key,1)键值对，再进行reduce聚合操作，最后利用collect函数将数据加载到driver，并转化为map类型。
 * 注意，从上述分析可以看出，countByKey操作将数据全部加载到driver端的内存，如果数据量比较大，可能出现OOM。因此，如果key数量比较多，
 * 建议进行rdd.mapValues(_ => 1L).reduceByKey(_ + _)，返回RDD[T, Long]
 * **/
public class SparkCountByKeyDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkCountByKeyDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<String> data = Arrays.asList("9","5", "1","7", "1", "3", "6", "2","4", "2","8");
        JavaRDD<String> javaRDD = jsc.parallelize(data,2);

        JavaRDD<String> partitionRDD = javaRDD.mapPartitionsWithIndex(
                (index,tp)->{
                    LinkedList<String> linkedList = new LinkedList<String>();
                    while(tp.hasNext()){
                        linkedList.add(index + "=" + tp.next());
                    }
                    return linkedList.iterator();
                },false);
        System.out.println(partitionRDD.collect());
        //一维rdd转化成二维rdd
        JavaPairRDD<String,String> javaPairRDD = javaRDD.mapToPair(tp->new Tuple2<>(tp,tp));
        System.out.println(javaPairRDD.countByKey());
    }
}
