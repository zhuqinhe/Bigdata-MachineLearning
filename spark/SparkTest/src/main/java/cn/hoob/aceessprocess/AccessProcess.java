package cn.hoob.aceessprocess;
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
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.Tuple3;

/**
  * 分析最受欢迎得老师
  */
public class AccessProcess {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("AccessProcess").setMaster("local");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //读取IP
        JavaRDD<String> ips  = jsc.textFile("hdfs://node1:9000/hoob/spark/data/ip.txt");
        //整理数据ip
        JavaRDD<Tuple3<Long,Long,String>> iprules = ips.map(ip->{
                String[]  ipss = ip.split("[|]");
                Long l1=Long.parseLong(ipss[2]);
                Long l2=Long.parseLong(ipss[3]);
                return new Tuple3<>(l1,l2,ipss[6]);
        });
        //将分散在多个Executor中的部分IP规则收集到Driver端
        List<Tuple3<Long,Long,String>>iprule=iprules.collect();
        //将Driver端的数据广播到Executor
        //广播变量的引用（还在Driver端）
        Broadcast<List<Tuple3<Long,Long,String>>> broadcastRef=  jsc.broadcast(iprule);

        //读取访问日子
        JavaRDD<String> access  = jsc.textFile("hdfs://node1:9000/hoob/spark/data/access.log");
        JavaPairRDD<String,Integer>proviceAndOne=access.mapToPair(line->{
            String[] lines=line.split("[|]");
            String ip=lines[1];
            Long ipLong=IpUtils.iptoInt(ip);
            List<Tuple3<Long,Long,String>>ipruless= broadcastRef.value();
            String province = "未知";
            Integer index=IpUtils.getProvince(ipLong,ipruless);
            if(index!=-1){
                province=ipruless.get(index)._3();
            }
            return new Tuple2<String,Integer>(province,1);
        });

        //直接计算每个key对应得值
        JavaPairRDD reducedRdd=proviceAndOne.reduceByKey(((v1,v2)->v1+v2));
        System.out.println(reducedRdd.collect());

        jsc.stop();
    }
}
