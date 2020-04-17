package cn.hoob.aceessprocess;

import org.apache.avro.ipc.specific.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Date;
import java.util.List;

/**
 * 分析最受欢迎得老师
 */
public class SparkAccessProcessDataSet1 {
    public static void main(String[] args) {


        SparkSession sparkSession= SparkSession.builder().appName("SparkAccessProcessDataSet1").master("local[*]").getOrCreate();

        //读取IP规则
        Dataset<String> ips  = sparkSession.read().textFile("hdfs://node1:9000/hoob/spark/data/ip.txt");
        //整理数据ip
        JavaRDD<String>stringrdd=ips.toJavaRDD();

        JavaRDD<IpRules>transformrdd=stringrdd.map(ip-> {
            String[] ipss = ip.split("[|]");
            Long l1 = Long.parseLong(ipss[2]);
            Long l2 = Long.parseLong(ipss[3]);
            return new IpRules(l1, l2, ipss[6]);
        });
        Encoder<IpRules> IpRulesEncoder = Encoders.bean(IpRules.class);
        Dataset<IpRules> iprulesDataSet=sparkSession.createDataset(transformrdd.rdd(),IpRulesEncoder);
        //iprulesDataSet.show();
        //创建视图
        iprulesDataSet.createOrReplaceTempView("ipRules");
        //读取ip数据
        JavaRDD<String>stringLogsRDD=sparkSession.read().textFile("hdfs://node1:9000/hoob/spark/data/access.log").toJavaRDD();
        JavaRDD<Long>ipLongRDD=stringLogsRDD.map(
                line-> {
                    String[] lines = line.split("[|]");
                    String ip = lines[1];
                    Long ipLong = IpUtils.iptoInt(ip);
                    return ipLong;
                }
        );
        Dataset<Long> ipLongDateSet=sparkSession.createDataset(ipLongRDD.rdd(),Encoders.LONG());
        //ipLongDateSet.show();
        ipLongDateSet.createOrReplaceTempView("ips");

        Dataset result=sparkSession.sql("SELECT province, count(*) counts FROM ips JOIN ipRules ON (value >= minNum AND value <= maxNum) GROUP BY province ORDER BY counts DESC");
        result.show();
        sparkSession.stop();
    }
}
