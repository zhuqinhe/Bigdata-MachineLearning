package cn.hoob.usersort;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/***
 * 用户排序
 * ********/
public class UserSort {
        public static void main(String[] args) {

            SparkConf conf = new SparkConf().setAppName("UserSort1").setMaster("local[*]");
            //创建sparkContext
            JavaSparkContext jsc = new JavaSparkContext(conf);
            //指定以后从哪里读取数据
           // JavaRDD<String> lines = jsc.textFile("hdfs://node1:9000/hoob/spark/data/teacher.log");

            //排序规则：分数第一个数学，第二个语文
            List<String> data= Arrays.asList("Hoob 100 80", "graves 98 90", "jiying 98 89", "huangjiangguo 70 70");

            JavaRDD<String> lines = jsc.parallelize(data);
            JavaRDD<UserScore>userScoreRDD=lines.map(line->{
                String[] info=line.split(" ");
                String name=info[0];
                Double mathScore=Double.parseDouble(info[1]);
                Double chineseScore=Double.parseDouble(info[2]);
                return new UserScore(name,mathScore,chineseScore);
            });
            //sortBy---这个函数是要构造key,key  必须实现了Comparable
            JavaRDD<UserScore>userScoreRDDSortBy=userScoreRDD.sortBy(f->f,true,1);
            System.out.println(userScoreRDDSortBy.collect());

            //利用sortByKey
            JavaRDD<UserScore>sortBykey=userScoreRDD.mapToPair(f->new Tuple2<>(f,1)).sortByKey().map(f->f._1);
            System.out.println(sortBykey.collect());
        }
}
