package cn.hoob.sparkoperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 可理解为更复杂的多阶aggregate。
 *
 * 从源码中可以看出，treeAggregate函数先是对每个分区利用scala的aggregate函数进行局部聚合的操作；同时，依据depth参数计算scale，
 * 如果当分区数量过多时，则按i%curNumPartitions进行key值计算，再按key进行重新分区合并计算；
 * 最后，在进行reduce聚合操作。这样可以通过调解深度来减少reduce的开销
 * ***/
public class SparkTreeAggregateDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkTakeOrderedDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data,3);
        //转化操作
        JavaRDD<String> javaRDD1 = javaRDD.map(tp->Integer.toString(tp));

        String result = javaRDD1.treeAggregate("0",(v1,v2)->{
           //return  "";
            return v1 + v2;
        } ,(v1,v2)->{
            return v1 + "<=comb=>" + v2;
        });
        System.out.println(result);
    }
}
