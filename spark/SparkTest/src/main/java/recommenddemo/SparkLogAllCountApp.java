package recommenddemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Comparator;

/***
 * 基于内容维度统计，用于热门推荐，及补充用户行为推荐，
 * 新用户时可以随机推荐热门内容
 * ****/
public class SparkLogAllCountApp {
    public static void main(String[] args) throws SQLException {


        System.setProperty("HADOOP_USER_NAME", "root");

        SparkConf conf = new SparkConf().setAppName("SparkLogAllCountApp").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //读取访问日志
        JavaRDD<String> logs = jsc.textFile("hdfs://node1:9000/recommend_source/");

        //整理数据,过滤进入详情页的记录
        JavaRDD<String> filterLogs = logs.filter(line -> line.contains("epg/rest/SPM/api/v1/media/detail"));
        //拆分解析记录
        MySQLUtlis.initSeriesdata();
        JavaPairRDD<String, Integer> prcessedLog = filterLogs.mapToPair(line -> {
            String[] ss = line.split("\\|");
            String time = ss[0];
            String userid = ss[2];
            String turl = ss[4];
            String contentId = "";
            String urls[] = turl.split("\\&");
            for (String tmps : urls) {
                if (tmps.contains("contentid")) {
                    String[] tcontentid = tmps.split("\\=");
                    contentId = tcontentid[1];
                }
            }
            return new Tuple2<>(contentId, 1);
        });
        //聚合,contentId作为key聚合
        JavaPairRDD<String, Integer> reducedRdd = prcessedLog.reduceByKey((m, n) -> m + n);

        //交换，再排序
        //JavaPairRDD<Integer,DataModel>temp=reducedLog.mapToPair(tp -> tp.swap());
        //reducedRdd.sortByKey();
        reducedRdd.saveAsTextFile("hdfs://node1:9000/recommend_processed_allcount/");
        jsc.stop();
    }
}
