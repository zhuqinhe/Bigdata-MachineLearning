/**
 * 
 */
package cn.hoob.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @Description 
 * @author hoob
 * @date 2020年4月3日上午11:37:07
 */
public class SparkJoinRddDeme {
	  public static void main(String[] args){ 
	        SparkConf conf = new SparkConf().setAppName("JoinRddDeme").setMaster("local"); 
	        JavaSparkContext sc = new JavaSparkContext(conf); 
	   
	        List<Integer> data = Arrays.asList(1,2,3,4,5); 
	        JavaRDD<Integer> rdd = sc.parallelize(data); 
	   
	        //FirstRDD 构造第一个二维rdd 
	        JavaPairRDD<Integer, Integer> firstRDD = rdd.mapToPair(number->new Tuple2<>(number,number));
	   
	        //SecondRDD  构造第二个二维rdd
	        JavaPairRDD<Integer, String> secondRDD = rdd.mapToPair(number->new Tuple2<>(number,number+"p")); 
	        
	        //构建joinrdd
	        JavaPairRDD<Integer, Tuple2<Integer, String>> joinRDD = firstRDD.join(secondRDD); 
	   
	        JavaRDD<String> res = joinRDD.map(new Function<Tuple2<Integer, Tuple2<Integer, String>>, String>() { 
	            @Override 
	            public String call(Tuple2<Integer, Tuple2<Integer, String>> integerTuple2Tuple2) throws Exception { 
	                int key = integerTuple2Tuple2._1(); 
	                int value1 = integerTuple2Tuple2._2()._1(); 
	                String value2 = integerTuple2Tuple2._2()._2(); 
	                return "<" + key + ",<" + value1 + "," + value2 + ">>"; 
	            } 
	        }); 
	   
	        List<String> resList = res.collect(); 
	        for(String str : resList) 
	            System.out.println(str); 
	   
	        sc.stop(); 
	    } 
	} 
}
