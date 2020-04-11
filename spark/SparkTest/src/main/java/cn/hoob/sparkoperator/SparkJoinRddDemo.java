/**
 * 
 */
package cn.hoob.sparkoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 将一组数据转化为RDD后，分别创造出两个PairRDD，然后再对两个PairRDD进行归约（即合并相同Key对应的Value）
 */
public class SparkJoinRddDemo {
	  public static void main(String[] args){ 
	        SparkConf conf = new SparkConf().setAppName("SparkJoinRddDemo").setMaster("local");
	        JavaSparkContext sc = new JavaSparkContext(conf); 
	   
	        List<Integer> data = Arrays.asList(1,2,3,4,5); 
	        JavaRDD<Integer> rdd = sc.parallelize(data); 
	   
	        //FirstRDD 构造第一个二维rdd 
	        JavaPairRDD<Integer, Integer> firstRDD = rdd.mapToPair(number->new Tuple2<>(number,number));
	        //SecondRDD  构造第二个二维rdd
	        JavaPairRDD<Integer, String> secondRDD = rdd.mapToPair(number->new Tuple2<>(number,number+"p")); 
	        //构建joinrdd
	        //将一组数据转化为RDD后，分别创造出两个PairRDD，然后再对两个PairRDD进行归约（即合并相同Key对应的Value）
	        JavaPairRDD<Integer, Tuple2<Integer, String>> joinRDD = firstRDD.join(secondRDD); 
	        //输出join后的字符串结果
	        JavaRDD<String> res = joinRDD.map(cp->cp._1+":"+cp._2._1+","+cp._2._2); 
	   
	        List<String> resList = res.collect(); 
	        for(String str : resList) 
	            System.out.println(str); 
	   
	        sc.stop(); 
	    }  
}
