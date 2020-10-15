package cn.hoob.structured_streaming;

import cn.hoob.structured_streaming.testForeachWriter.TestMysqlForeachWriter;
import cn.hoob.structured_streaming.testForeachWriter.TestRedisForeachWriter;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Arrays;

/**
 * @author zhuqinhe
 * complete：完整模式，全部输出，必须有聚合才可以使用，
 * append：追加模式，只输出那些将来永远不可能再更新的数据。没有聚合的时候，append和update是一样的，有聚合的时候，一定要有水印才能使用append。
 * update：只输出更新的模式，只输出变化的部分，也就是哪一条数据发生了变化，就输出哪一条数据
 * Foreach 自定义输出
 */
public class WordCountSocketStructuredStreamingMysqlForeachWriter {
    public static void main(String[] args) throws Exception {
        //获取sparkSession
        SparkSession sparkSession= SparkSession.builder().master("local[*]").appName("WordCountSocketStructuredStreaming").getOrCreate();
        Dataset<Row> streamingdata = sparkSession.readStream()  //读一个流数据，lines其实就是一个输入表
                .format("socket") //指定这个流的格式
                .option("host", "hdp1")   //指定socket的地址和端口号
                .option("port", 10000)
                .load();
        Dataset<String>strdata = streamingdata.as(Encoders.STRING()); //类型转换
        //Dataset获取RDD 等本质对应的时iterator
        Dataset<String> flatmStrData = strdata.flatMap((FlatMapFunction<String, String>)
                        line -> Arrays.asList(line.split(",")).iterator(),
                Encoders.STRING());
         //createOrReplaceTempView  sql的也可以采用写SQL的方式
        //flatmStrData.createOrReplaceTempView("words");
        //Dataset<Row> countData =sparkSession.sql("select value,count(value) as count from words group by value ");
        Dataset<Row> countData = flatmStrData.groupBy("value").count();
        TestMysqlForeachWriter writer=new TestMysqlForeachWriter();
        StreamingQuery result = countData.writeStream()
                .foreach(writer)
                .outputMode("update")
                .trigger(Trigger.ProcessingTime("60 seconds"))
                .start();
        result.awaitTermination();
        sparkSession.stop();
    }
}
