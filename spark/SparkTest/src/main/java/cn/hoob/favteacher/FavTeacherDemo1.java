package cn.hoob.favteacher;
import java.io.Serializable;
import java.net.URL;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.sun.xml.internal.ws.api.pipe.Tube;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

/**
  * 分析最受欢迎得老师
  */
public class FavTeacherDemo1 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("FavTeacherDemo1").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定以后从哪里读取数据
        JavaRDD<String> lines  = jsc.textFile("hdfs://node1:9000/hoob/spark/data/teacher.log");
        //整理数据
        JavaPairRDD<String, Integer> teacherAndOne = lines.mapToPair(line->{
                Integer  index = line.lastIndexOf("/");
                String teacher = line.substring(index + 1);
                return new Tuple2<>(teacher, 1);
        });
        //1.reduceByKey+sortByKey聚合,交换,排序
        JavaPairRDD<String, Integer>reducedRdd=teacherAndOne.reduceByKey((v1,v2)->v1+v2);
        JavaPairRDD<Integer , String>sortRdd=reducedRdd.mapToPair(tp->new Tuple2(tp._2,tp._1)).sortByKey(false);
        JavaPairRDD<String , Integer>sortRdd_=sortRdd.mapToPair(tp->new Tuple2<>(tp._2,tp._1));
        List<Tuple2<String,Integer>> result=sortRdd_.collect();
        //直接计算每个key对应得值
        //Map<String, Long> reducedRdd=teacherAndOne.countByKey();
        System.out.println(reducedRdd);

        jsc.stop();




    }
    public static class OrderedComparator<K> implements Serializable, Comparator<Integer> {
        @Override
        public int compare(Integer o1, Integer o2) {
            return -o1.compareTo(o2);
        }
    }
}
