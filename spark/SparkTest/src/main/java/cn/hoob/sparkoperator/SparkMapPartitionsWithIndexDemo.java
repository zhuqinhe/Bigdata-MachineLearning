package cn.hoob.sparkoperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
/**
 * mapPartitionsWithIndex与mapPartitions基本相同，只是在处理函数的参数是一个二元元组，
 * 元组的第一个元素是当前处理的分区的index，元组的第二个元素是当前处理的分区元素组成的Iterator
 * **/
public class SparkMapPartitionsWithIndexDemo {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("SparkMapPartitionsWithIndexDemo").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7,8,9,10);
        //RDD有两个分区
        JavaRDD<Integer> javaRDD = jsc.parallelize(data,2);
        //分区index、元素值、元素编号输出
        JavaRDD<String> mapPartitionsWithIndexRDD = javaRDD.mapPartitionsWithIndex((index,tp)->{
            StringBuilder  str=new StringBuilder("index:"+index+"|");

            while(tp.hasNext()){
                str.append(tp.next()+",");
            }
            return  new ArrayList<String>(Collections.singleton(str.toString())).iterator();

        },false);

        System.out.println(mapPartitionsWithIndexRDD.collect());
    }
}
