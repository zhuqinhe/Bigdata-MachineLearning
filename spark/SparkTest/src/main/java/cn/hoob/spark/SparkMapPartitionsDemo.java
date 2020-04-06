package cn.hoob.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple1;
import scala.Tuple2;

import java.util.*;
/**
 * mapPartitions函数会对每个分区依次调用分区函数处理，然后将处理的结果(若干个Iterator)生成新的RDDs。
 * mapPartitions与map类似，但是如果在映射的过程中需要频繁创建额外的对象，使用mapPartitions要比map高效的过。
 * 比如，将RDD中的所有数据通过JDBC连接写入数据库，如果使用map函数，可能要为每一个元素都创建一个connection，这样开销很大，
 * 如果使用mapPartitions，那么只需要针对每一个分区建立一个connection。
 * 两者的主要区别是调用的粒度不一样：map的输入变换函数是应用于RDD中每个元素，而mapPartitions的输入函数是应用于每个分区。
 *
 *假设一个rdd有10个元素，分成3个分区。如果使用map方法，map中的输入函数会被调用10次；而使用mapPartitions方法的话，其输入函数会只会被调用3次，每个分区调用1次
 * **/
public class SparkMapPartitionsDemo {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("SparkMapPartitionsDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7,8,9);
        //RDD有两个分区
        JavaRDD<Integer> javaRDD = jsc.parallelize(data,2);
        //计算每个分区的合计
        JavaRDD<Integer> mapPartitionsRDD = javaRDD.mapPartitions(numbers->{
          Integer total=0;
          LinkedList<Integer> list=new LinkedList<Integer>();
          while(numbers.hasNext()){
                total+=numbers.next();
               // System.out.println(total);
            }
            list.add(total);
          return  list.iterator();
        });

        System.out.println(mapPartitionsRDD.collect());
    }
}
