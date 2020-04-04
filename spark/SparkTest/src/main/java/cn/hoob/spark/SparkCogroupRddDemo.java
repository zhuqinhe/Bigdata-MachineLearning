/**
 * 
 */
package cn.hoob.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @Description 
 * @author hoob
 * @date 2020年4月3日下午5:06:52
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

		nameScores.foreach(tp->tp._2._1.forEach(
				tmp->
		            )
				);
				/*new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {  
			private static final long serialVersionUID = 1L;  
			int i=1;  
			@Override  
			public void call(  
					Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t)  
							throws Exception {  
				String string="ID:"+t._1+" , "+"Name:"+t._2._1+" , "+"Score:"+t._2._2;  
				string+="     count:"+i;  
				System.out.println(string);  
				i++;  
			}  
		});  */

		sContext.close();  
	}  
}
