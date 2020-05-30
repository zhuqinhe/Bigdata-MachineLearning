package recommenddemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

/**
 * 对日志数据清洗，剥离出我们需要的信息
 */
public class SparkLogToHiveAndCreateTableApp {
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");
        String warehouse = "/user/hive/warehouse";
        SparkSession sparkSession = SparkSession.builder().appName("SparkLogToHiveAndCreateTableApp").
                master("local[*]")
                //.config("spark.sql.warehouse.dir", warehouse)
                .config("spark.sql.shuffle.partitions", "2").
                        enableHiveSupport().
                        getOrCreate();


        //将所有处理好的日志数据加载到    stored as textfile    spark默认的是parquet
        sparkSession.sql("drop table if exists logs_all");
        sparkSession.sql("create table if not exists logs_all(userId int,seriesId int,count int,time String) row format delimited fields terminated by ','  stored as textfile");
        sparkSession.sql("load data inpath 'hdfs://node1:9000/recommend_processed_string' overwrite into table logs_all");
        // sparkSession.sql("select count(*) from logs_all1 ").show();

        //数据分割，7份用来训练模型，3份用来测试用
        long count = sparkSession.sql("select count(*) from logs_all ").first().getLong(0);
        long trainingcount = (long) (count * 0.7);
        long testcount = (long) (count * 0.3);
        Dataset trainingDataset = sparkSession.sql("select userId,seriesId,count from logs_all order by time asc limit " + trainingcount);
        trainingDataset.write().mode(SaveMode.Overwrite).parquet("hdfs://node1:9000/recommend_processed_/trainingData");
        sparkSession.sql("drop table if exists logs_training");
        sparkSession.sql("create table if not exists logs_training(userId int,seriesId int,count int) stored as parquet");
        sparkSession.sql("load data inpath 'hdfs://node1:9000/recommend_processed_/trainingData' overwrite into table logs_training");



        Dataset testDataset = sparkSession.sql("select userId,seriesId,count from logs_all order by time desc limit " + testcount);
        testDataset.write().mode(SaveMode.Overwrite).parquet("hdfs://node1:9000/recommend_processed_/testData");
        sparkSession.sql("drop table if exists logs_test");
        sparkSession.sql("create table if not exists logs_test(userId int,seriesId int,count int) stored as parquet");
        sparkSession.sql("load data inpath 'hdfs://node1:9000/recommend_processed_/testData' overwrite into table logs_test");
        sparkSession.stop();
    }
}
