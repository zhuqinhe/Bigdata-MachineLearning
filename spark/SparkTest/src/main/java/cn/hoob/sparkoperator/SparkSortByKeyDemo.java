package cn.hoob.sparkoperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
/**
 * 根据kek 排序
 * ***/
public class SparkSortByKeyDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkSortByKeyDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<String> data = Arrays.asList("a","b","c","d","e");
        JavaRDD<String> javaRDD = jsc.parallelize(data, 3);
        Random random = new Random(50);
        //对RDD进行转换，每个元素有两部分组成
        JavaPairRDD<String,Integer> javaRDD1 = javaRDD.mapToPair(tp->new Tuple2<>(tp,random.nextInt(50)));
        System.out.println(javaRDD1.collect());
        //按RDD中key  排序
        JavaPairRDD<String,Integer> resultRDD = javaRDD1.sortByKey(false);
        System.out.println(resultRDD.collect());
        //自定义比较器重排
        Comparator<String> comp = new StringComparator();
        JavaPairRDD<String,Integer> resultRDD1 = javaRDD1.sortByKey(comp);
        System.out.println(resultRDD1.collect());
    }
     /**自定义比较器**/
    public static class StringComparator implements Serializable, Comparator<String> {
        @Override
        public int compare(String o1, String o2) {
            return -o1.compareTo(o2);
        }
    }
}
