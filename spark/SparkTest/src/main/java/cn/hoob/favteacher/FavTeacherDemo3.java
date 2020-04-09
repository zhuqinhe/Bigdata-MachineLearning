package cn.hoob.favteacher;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
  * 分析每个学科最受欢迎的老师的课
  */
public class FavTeacherDemo3 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("FavTeacherDemo3").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定以后从哪里读取数据
        JavaRDD<String> lines  = jsc.textFile("hdfs://node1:9000/hoob/spark/data/teacher.log");
        //设置检查目录
        jsc.setCheckpointDir("./FavTeacherDemo3");
        //整理数据
        //http://bigdata.rdd.cn/hoob
        JavaPairRDD<Tuple2<String,String>, Integer> teacherAndOne = lines.mapToPair(line->{
                Integer  index = line.lastIndexOf("/");
                String teacher = line.substring(index + 1);
                String httpHost = line.substring(0, index);
                String subject = new URL(httpHost).getHost().split("[.]")[0];
                return new Tuple2<>(new Tuple2<>(subject,teacher), 1);
        });
        //配置科目
        List<String>subjects= Arrays.asList("bigdata", "javaee", "php");
        //聚合统计出 以学科和老师为key的数据
        JavaPairRDD<Tuple2<String,String>, Integer> reducedRdd=teacherAndOne.reduceByKey((v1,v2)->v1+v2);
        //设置检查点
        reducedRdd.checkpoint();
        //打印存入检查点的数据
        System.out.println(reducedRdd.collect());
        //转换，把数据格式打散重组变成学科为key,老师和统计值的
        JavaPairRDD<String,Tuple2<Integer,String>>treducedRdd=reducedRdd.mapToPair(tp->new Tuple2<>(tp._1._1,new Tuple2<>(tp._2,tp._1._2)));
        System.out.println(treducedRdd.collect());
        //再以学科为key，分组，求出每个学科最老师最受欢迎的(每个学科的数据再一组了)
        JavaPairRDD<String, Iterable<Tuple2<Integer, String>>> groupRdd=treducedRdd.groupByKey();
        //输出聚合后的数据
        System.out.println(groupRdd.collect());
        //遍历配置的科目，每次去一个科目的数据出来处理，大数量量时节省内存
        for(String sub:subjects){
            JavaPairRDD<String, Iterable<Tuple2<Integer, String>>> filterRdd=groupRdd.filter(tp->sub.equalsIgnoreCase(tp._1));
            //土办法也是办法
           /* JavaPairRDD<String, Iterable<Tuple2<Integer, String>>>top1Rdd=filterRdd.mapToPair(tuple->{
                List<Tuple2<Integer, String>> list = new ArrayList<Tuple2<Integer, String>>();
                Iterator<Tuple2<Integer, String>> it = tuple._2.iterator();
                while(it.hasNext()) {
                    Tuple2<Integer, String> top = it.next();
                    //比较
                    if(list.size()<1){
                        list.add(top);
                    }else{
                        if(list.get(0)._1<top._1){
                            list.clear();
                            list.add(top);
                        }
                    }
                }
                return new Tuple2<String, Iterable<Tuple2<Integer, String>>>(tuple._1,list);
            });*/
           //有科目为key，转化成统计值为key

            JavaPairRDD<String, Iterable<Tuple2<Integer, String>>>top1Rdd=filterRdd.mapToPair(tuple->{
                List<Tuple2<Integer, String>> list = new ArrayList<Tuple2<Integer, String>>();
                Iterator<Tuple2<Integer, String>> it = tuple._2.iterator();
                while(it.hasNext()) {
                    Tuple2<Integer, String> top = it.next();
                    //比较
                    if(list.size()<1){
                        list.add(top);
                    }else{
                        if(list.get(0)._1<top._1){
                            list.clear();
                            list.add(top);
                        }
                    }
                }
                return new Tuple2<String, Iterable<Tuple2<Integer, String>>>(tuple._1,list);
            });
            System.out.println(top1Rdd.collect());
        }



        jsc.stop();




    }
}
