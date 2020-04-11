package cn.hoob.sparkoperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
/**
 * takeOrdered函数用于从RDD中，按照默认（升序）或指定排序规则，返回前num个元素。
 * ***/
public class SparkTakeOrderedDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkTakeOrderedDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1,1,2,2,4,3,2,1,6,7,9,6,0);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 3);
        //默认比较器
        System.out.println( javaRDD.takeOrdered(2));
        //自定义比较器
        List<Integer> list = javaRDD.takeOrdered(2, new TakeOrderedComparator());
        System.out.println("TakeOrderedComparator" + list);
    }
    public static class TakeOrderedComparator implements Serializable, Comparator<Integer> {
        @Override
        public int compare(Integer o1, Integer o2) {
            return -o1.compareTo(o2);
        }
    }

}

