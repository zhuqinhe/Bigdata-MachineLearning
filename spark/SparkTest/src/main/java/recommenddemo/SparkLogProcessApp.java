package recommenddemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 对日志数据清洗，剥离出我们需要的信息
 */
public class SparkLogProcessApp {
    public static void main(String[] args) throws SQLException {


        System.setProperty("HADOOP_USER_NAME", "root");

        SparkConf conf = new SparkConf().setAppName("SparkLogProcessApp").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //读取访问日志
        JavaRDD<String> logs = jsc.textFile("hdfs://node1:9000/recommend_source/");

        //整理数据,过滤进入详情页的记录
        JavaRDD<String> filterLogs = logs.filter(line -> line.contains("epg/rest/SPM/api/v1/media/detail"));
        //拆分解析记录
        MySQLUtlis.initSeriesdata();
        JavaPairRDD<DataModel, Integer> prcessedLog = filterLogs.mapToPair(line -> {
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
            DataModel model = new DataModel();
            model.setContentid(contentId);
            model.setCount(0);
            model.setTime(time);
            model.setUserid(userid);
            //加载查询db,把用户id，和contentId兑换成阿拉伯自增的id
            model.setUid(Integer.parseInt(userid.replace("U00","")));
            model.setSeries(MySQLUtlis.getSeriesId(contentId));
            return new Tuple2<>(model, 1);
        });
        //聚合,自定义根据用户userid+contentId作为key聚合
        JavaPairRDD<DataModel, Integer> reducedRdd = prcessedLog.reduceByKey((m, n) -> m + n);
        //补充对象上的count统计值,回填数据尽量完整性
        JavaPairRDD<DataModel, Integer> pairRDD = reducedRdd.mapToPair(line -> {
            line._1.setCount(line._2);
            return line;
        });
        //交换，再排序
        //JavaPairRDD<Integer,DataModel>temp=reducedLog.mapToPair(tp -> tp.swap());
        //reducedLog.sortByKey();
        //观察前100的数据
        //System.out.println(pairRDD.top(100,new MyComparator()));
        JavaRDD<DataModel> saveRdd = pairRDD.map(line -> line._1);
        //清理后格式化的日志，存hdfs(以备待用)
        //saveRdd.saveAsObjectFile("hdfs://node1:9000/recommend_processed_object/");
        JavaRDD<String>resultrdd=saveRdd.map(line->line.getUid()+","+line.getSeries()+","+line.getCount()+","+line.getTime());
        resultrdd.saveAsTextFile("hdfs://node1:9000/recommend_processed_string/");
        jsc.stop();
    }

    //自定义比较器
    public static class MyComparator implements Serializable, Comparator<Tuple2<DataModel, Integer>> {
        @Override
        public int compare(Tuple2<DataModel, Integer> o1, Tuple2<DataModel, Integer> o2) {
            return -o2._1.compareTo(o1._1);
        }
    }
}
