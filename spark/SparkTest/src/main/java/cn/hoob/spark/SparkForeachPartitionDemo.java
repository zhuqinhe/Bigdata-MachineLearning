package cn.hoob.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 *foreachPartition和foreach类似，只不过是对每一个分区使用
 * **/
public class SparkForeachPartitionDemo {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkForeachPartitionDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<String> data = Arrays.asList("7","5", "1","4","3", "1","6", "2", "2","8");
        JavaRDD<String> javaRDD = jsc.parallelize(data,5);
        //把同一个分区的数据重组
        JavaRDD<String> javaPairRDD = javaRDD.mapPartitionsWithIndex((index,tp)->{
            LinkedList<String> linkedList = new LinkedList<String>();
            while (tp.hasNext()){
                linkedList.add("index:"+index+"&value:"+tp.next());
            }
            return linkedList.iterator();
        },false);
        System.out.println(javaPairRDD.collect());
        javaRDD.foreachPartition(tp->{
            while(tp.hasNext()){
                System.out.println(tp.next());
            }
        });
    }

}
