package cn.hoob.aceessprocess;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple3;

import java.util.List;

/**
 * 自定义UTF
 */
public class SparkAccessProcessUDF {
    public static void main(String[] args) {


        SparkSession sparkSession= SparkSession.builder().appName("SparkAccessProcessDataSet").master("local[*]").getOrCreate();

        //读取IP规则
        Dataset<String> ips  = sparkSession.read().textFile("hdfs://node1:9000/hoob/spark/data/ip.txt");
        //整理数据ip


        JavaRDD<Tuple3<Long,Long,String>> iprules = ips.toJavaRDD().map(ip->{
            String[]  ipss = ip.split("[|]");
            Long l1=Long.parseLong(ipss[2]);
            Long l2=Long.parseLong(ipss[3]);
            return new Tuple3<>(l1,l2,ipss[6]);
           // return new IpRules(l1,l2,ipss[6]);
        });
        //将分散在多个Executor中的部分IP规则收集到Driver端
        List<Tuple3<Long,Long,String>>ipruleList=iprules.collect();
        //将Driver端的数据广播到Executor
        //广播变量的引用（还在Driver端）
        //这里又回到sparkSession 如何获取JavaSparkContext
        Broadcast<List<Tuple3<Long,Long,String>>> rules=  JavaSparkContext.fromSparkContext(
                sparkSession.sparkContext()).broadcast(ipruleList);
        //注册一个JavaUTF
        sparkSession.udf().register("getProvince",line->{
            String[] lines=line.toString().split("[|]");
            //System.out.println(line);
            String ip=lines[1];
            Long ipLong=IpUtils.iptoInt(ip);
            List<Tuple3<Long,Long,String>>ipruless= rules.value();
            String province = "未知";
            Integer index=IpUtils.getProvince(ipLong,ipruless);
            if(index!=-1){
                province=ipruless.get(index)._3();
            }
            return province;
        }, DataTypes.StringType);

        //读取ip数据
        Dataset<String>stringLogsRDD=sparkSession.read().textFile("hdfs://node1:9000/hoob/spark/data/access.log");
        stringLogsRDD.createOrReplaceTempView("ips");
        Dataset result=sparkSession.sql("SELECT getProvince(value) as province, count(*) counts FROM ips GROUP BY province ORDER BY counts DESC");
        result.show();
        sparkSession.stop();
    }
}
