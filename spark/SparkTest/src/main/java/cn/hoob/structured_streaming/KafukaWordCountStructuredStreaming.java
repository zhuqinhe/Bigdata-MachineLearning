package cn.hoob.structured_streaming;

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
 */
public class KafukaWordCountStructuredStreaming {
    public static void main(String[] args) throws Exception {
        //获取sparkSession
        SparkSession sparkSession= SparkSession.builder().master("local[*]").appName("KafukaWordCountStructuredStreaming").getOrCreate();
        Dataset<String> streamingdata = sparkSession
                .readStream()  //读一个流数据，lines其实就是一个输入表
                .format("kafka") //数据源的格式
                .option("kafka.bootstrap.servers", "hdp1:9092,hdp2:9092,hdp3:9092")
                .option("subscribe", "hoob_topic") // 也可以订阅多个主题: "topic1,topic2"
                //.option("startingOffsets","earliest")//开始的offset，也就是从哪里开始消费topic，earliest是最开始，传{"topic1":{"0":12}}这种格式也可以
                //.option("endingOffsets","latest")//结束的offset，latest是最后的
                .load()
                .selectExpr("cast(value as string)") //这个算子可以写SQL表达式，写SQL转换类型成string
                .as(Encoders.STRING()); //转成DataSet<String>  类型转换row 转化成string

        //Dataset<String>streamingdata = streamingdata.as(Encoders.STRING()); //类型转换
        //Dataset获取RDD 等本质对应的时iterator
        Dataset<String> flatmStrData = streamingdata.flatMap((FlatMapFunction<String, String>)
                        line -> Arrays.asList(line.split(",")).iterator(),
                Encoders.STRING());
        // Dataset<Row> countData = flatmStrData.groupBy("value").count();
        //createOrReplaceTempView  sql的也可以采用写SQL的方式
        flatmStrData.createOrReplaceTempView("words");
        Dataset<Row> countData =sparkSession.sql("select value,count(value) as count from words group by value ");
        StreamingQuery result = countData.writeStream().format("console")  //指定输出到哪，这里输出到控制台
                .outputMode("update") //输出模式，输出模式有三种：complete append  update
                .trigger(Trigger.ProcessingTime("60 seconds")) //多长时间触发一次，如果不写的话就是尽快处理
                .option("truncate", false) //那些很长的字段显示不完全的也不省略了，设置全部显示出来
                .start();
        result.awaitTermination();//阻止当前线程退出
        sparkSession.stop();
    }
}
