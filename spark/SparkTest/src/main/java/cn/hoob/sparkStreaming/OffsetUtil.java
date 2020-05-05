package cn.hoob.sparkStreaming;

import kafka.common.TopicAndPartition;
import lombok.val;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.hash.Hash;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.util.*;

public class OffsetUtil {

 /*
  手动维护offset的工具类
  首先在MySQL创建如下表
    CREATE TABLE `t_offset` (
      `topic` varchar(255) NOT NULL,
      `partition` int(11) NOT NULL,
      `groupid` varchar(255) NOT NULL,
      `offset` bigint(20) DEFAULT NULL,
      PRIMARY KEY (`topic`,`partition`,`groupid`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
   */

    public static Map<TopicPartition, Long> getOffsetMapByMsql(String groupid, String topic) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "root");
        PreparedStatement pstmt = connection.prepareStatement("select * from t_offset where groupid=? and topic=?");
        pstmt.setString(1, groupid);
        pstmt.setString(2, topic);
        ResultSet rs = pstmt.executeQuery();
        Map<TopicPartition, Long> offsetMap = new HashMap<TopicPartition, Long>();
        while (rs.next()) {
            offsetMap.put(new TopicPartition(rs.getString("topic"), rs.getInt("partition")), rs.getLong("offset"));
        }
        rs.close();
        pstmt.close();
        connection.close();
        return offsetMap;

    }

    //将偏移量保存到数据库
    public static void setOffsetRangesByMsql(String groupid, ArrayList<OffsetRange> offsetRange) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "root");
        //replace into表示之前有就替换,没有就插入
        PreparedStatement pstmt = connection.prepareStatement("replace into t_offset (`topic`, `partition`, `groupid`, `offset`) values(?,?,?,?)");

        for (OffsetRange o : offsetRange) {
            pstmt.setString(1, o.topic());
            pstmt.setInt(2, o.partition());
            pstmt.setString(3, groupid);
            pstmt.setLong(4, o.untilOffset());
            pstmt.executeUpdate();
        }
        pstmt.close();
        connection.close();
    }

   //根据topic返回对应的偏移量
    public static Map<TopicPartition, Long> getOffsetMapByReis(Collection<String> topics) {
        Jedis jedis = RedisUtil.getJedis();
        if(topics==null||topics.isEmpty()){
            return null;
        }
        Map<TopicPartition,Long>maptopic=new HashMap<>();
        for(String topic:topics){
            Set<String> keys= jedis.keys(topic+"*");
            if(keys!=null&&!keys.isEmpty()){
                for(String key:keys){
                    String value=jedis.get(key);
                    if(value!=null&&value.length()>0){
                        String [] tt=key.split("_");
                        maptopic.put(new TopicPartition(topic,Integer.parseInt(tt[1])),Long.parseLong(value));
                    }
                }
            }
            return maptopic;
        }
        return null;

    }
   public static  void setOffsetMapByRedis(OffsetRange[] offsetRanges){
        // 获取jedis 连接对象 ，
       Jedis jedis = RedisUtil.getJedis();
      // OffsetRange[] offsetRanges = ((HasOffsetRanges) stringJavaRDD.rdd()).offsetRanges();
       //每次操作之前  ，保存此次操作前的偏移量， 如果当前任务失败， 我们可以回到开始的偏移量 重新计算，
       for (OffsetRange o : offsetRanges) {
           //key   topic_partition
           jedis.set(o.topic()+"_"+o.partition(),String.valueOf(o.untilOffset()));
       }
   }

}
