/**
 * 
 */
package cn.hoob.sparkoperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 有两个元组Tuple的集合A与B,先对A组集合中key相同的value进行聚合,
 然后对B组集合中key相同的value进行聚合,之后对A组与B组进行"join"操作
 */
public class SparkCogroupRddDemo {
	public static void main(String[] args) {  
		SparkConf conf=new SparkConf().setAppName("SparkCogroupRddDemo").setMaster("local");  
		JavaSparkContext sContext=new JavaSparkContext(conf);  
		List<Tuple2<Integer,String>> namesList=new ArrayList();  
		namesList.add(new Tuple2<Integer, String>(1,"Spark"));
		namesList.add(new Tuple2<Integer, String>(3,"Tachyon"));
		namesList.add(new Tuple2<Integer, String>(4,"Sqoop"));
		namesList.add(new Tuple2<Integer, String>(2,"Hadoop"));
		namesList.add(new Tuple2<Integer, String>(2,"Hadoop2") );
		 
		List<Tuple2<Integer,Integer>> scoresList=Arrays.asList(  
				new Tuple2<Integer, Integer>(1,100),  
				new Tuple2<Integer, Integer>(3,70),  
				new Tuple2<Integer, Integer>(3,77),  
				new Tuple2<Integer, Integer>(2,90),  
				new Tuple2<Integer, Integer>(2,80)  
				);  
		//转化成rdd
		JavaPairRDD<Integer, String> names=sContext.parallelizePairs(namesList);  
		JavaPairRDD<Integer, Integer> scores=sContext.parallelizePairs(scoresList);  
	    //有两个元组Tuple的集合A与B,先对A组集合中key相同的value进行聚合,然后对B组集合中key相同的value进行聚合,之后对A组与B组进行"join"操作;
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> nameScores=names.cogroup(scores);           
        //打印结果
		nameScores.foreach(tp->System.out.println("key="+tp._1+",name="+tp._2._1+",value="+tp._2._2));


		sContext.close();  
	}  
}
