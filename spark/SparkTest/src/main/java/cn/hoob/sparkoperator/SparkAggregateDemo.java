package cn.hoob.sparkoperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/***
 *aggregate函数将每个分区里面的元素进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
 * 这个函数最终返回U的类型不需要和RDD的T中元素类型一致。 这样，我们需要一个函数将T中元素合并到U中，另一个函数将两个U进行合并。
 * 其中，参数1是初值元素；参数2是seq函数是与初值进行比较；参数3是comb函数是进行合并 。
 * 注意：如果没有指定分区，aggregate是计算每个分区的，空值则用初始值替换
 * **/
public class SparkAggregateDemo {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("SparkAggregateDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(5,4,3,2,1);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data,3);
        System.out.println(javaRDD.collect());
        Integer aggregateValue = javaRDD.aggregate(3,(v1,v2)->Math.max(v1,v2),(v1,v2)->v1+v2);
        System.out.println(aggregateValue);
    }
}
