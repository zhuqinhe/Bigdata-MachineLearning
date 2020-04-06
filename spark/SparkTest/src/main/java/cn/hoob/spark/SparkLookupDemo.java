package cn.hoob.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
/**
 * okup用于(K,V)类型的RDD,指定K值，返回RDD中该K对应的所有V值。
 * 如果partitioner不为空，计算key得到对应的partition，在从该partition中获得key对应的所有value；
 * 如果partitioner为空，则通过filter过滤掉其他不等于key的值，然后将其value输出
 * ****/
public class SparkLookupDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkLookupDemo").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data);
        JavaPairRDD<Integer,Integer> javaPairRDD = rdd.mapToPair(tp->new Tuple2<>(tp,tp*tp));
        System.out.println(javaPairRDD.collect());
        System.out.println(javaPairRDD.lookup(4));
    }
}
