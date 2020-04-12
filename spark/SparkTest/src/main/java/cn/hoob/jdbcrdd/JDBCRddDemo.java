package cn.hoob.jdbcrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.rdd.RDD;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
/***
 * rdd连接mysql
 * **/
public class JDBCRddDemo {

    public static void main(String[] args) throws SQLException {

        SparkConf conf = new SparkConf().setAppName("UserSort1").setMaster("local[*]");
        //创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        //指定以后从哪里读取数据
        // JavaRDD<String> lines = jsc.textFile("hdfs://node1:9000/hoob/spark/data/teacher.log");
        JavaRDD<UserScore> userScore= JdbcRDD.create(
                sc,
                new JdbcRDD.ConnectionFactory() {
                    @Override
                    public Connection getConnection() throws SQLException {
                        return  DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8",
                                "appuser", "Mysql123+");
                    }
                },
                "SELECT * FROM userscore WHERE id >= ? AND id < ?",
                1,
                5,
                1,//分区数量
                //函数
                  /*  new Function<ResultSet, UserScore>() {
                        @Override
                        public UserScore call(ResultSet rs) throws Exception {
                            Long id = rs.getLong(1);
                            String name = rs.getString(2);
                            Double mathScore = rs.getDouble(3);
                            Double chineseScore = rs.getDouble(4);
                            return new UserScore( id,name,mathScore,chineseScore);
                        }
                    }*/
                //匿名函数
                rs -> {
                    Long id = rs.getLong(1);
                    String name = rs.getString(2);
                    Double mathScore = rs.getDouble(3);
                    Double chineseScore = rs.getDouble(4);
                    return new UserScore( id,name,mathScore,chineseScore);
                }
        );

      System.out.println(userScore.collect());
    }
}
