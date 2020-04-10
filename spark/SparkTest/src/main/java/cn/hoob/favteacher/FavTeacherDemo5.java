package cn.hoob.favteacher;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.net.URL;
import java.util.*;

/**
 * 分析每个学科最受欢迎的老师的课
 */
public class FavTeacherDemo5 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("FavTeacherDemo5").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定以后从哪里读取数据
        JavaRDD<String> lines  = jsc.textFile("hdfs://node1:9000/hoob/spark/data/teacher.log");
        //整理数据
        //http://bigdata.rdd.cn/hoob
        JavaPairRDD<Tuple2<String,String>, Integer> teacherAndOne = lines.mapToPair(line->{
            Integer  index = line.lastIndexOf("/");
            String teacher = line.substring(index + 1);
            String httpHost = line.substring(0, index);
            String subject = new URL(httpHost).getHost().split("[.]")[0];
            return new Tuple2<>(new Tuple2<>(subject,teacher), 1);
        });
        System.out.println(teacherAndOne.collect());
        //配置科目
        List<String>subjects=teacherAndOne.map(tuple->tuple._1._1).distinct().collect();
        //gou构建分区器
        SubjectParitioner sp=new SubjectParitioner(subjects);
        //聚合统计出 以学科和老师为key的数据，reduce后的rdd重新定义其分区
        JavaPairRDD<Tuple2<String,String>, Integer> reducedRdd=teacherAndOne.reduceByKey(sp,(v1,v2)->v1+v2);
        System.out.println(reducedRdd.collect());
        //假设topN=2
        int topN=1;
        JavaRDD<Tuple2<Tuple2<String,String>, Integer>>onePartionRdd=reducedRdd.mapPartitions(it->{
            List<Tuple2<Tuple2<String,String>, Integer>>list=new ArrayList<>();
            while (it.hasNext()){
                Tuple2<Tuple2<String,String>, Integer>t= it.next();
                //自定义比较器
                if(list.size()<topN){
                    list.add(t);
                }else{
                    //升序，第一个就是最小的
                    Collections.sort(list,new Comparator<Tuple2<Tuple2<String,String>, Integer>>() {
                        @Override
                        public int compare(Tuple2<Tuple2<String,String>, Integer> o1, Tuple2<Tuple2<String,String>, Integer> o2) {
                            int i = o1._2 - o2._2;
                            return i;
                        }
                    });
                    Tuple2<Tuple2<String,String>, Integer>tt=list.get(0);
                    //比最小的大，淘汰最小的
                    if(t._2>tt._2){
                        list.remove(0);
                        list.add(t);
                    }
                }

            }
            return list.iterator();
        });

        System.out.println(onePartionRdd.collect());



        jsc.stop();




    }
    /**
     * 自定义分区器
     * **/
    public static class SubjectParitioner extends Partitioner {
        private Map<String, Integer> subs = new HashMap<>();

        public Map<String, Integer> getSubs() {
            return subs;
        }

        public void setSubs(Map<String, Integer> subs) {
            this.subs = subs;
        }

        public SubjectParitioner(List<String> subss) {
            if (subss != null) {
                for (int i = 0; i < subss.size(); i++) {
                    this.subs.put(subss.get(i), i);
                }
            }
        }

        @Override
        public int numPartitions() {
            return subs.size();
        }

        @Override
        public int getPartition(Object key) {
            Tuple2<String, String> t = (Tuple2<String, String>) key;
            return subs.get(t._1);
        }
    }
}
