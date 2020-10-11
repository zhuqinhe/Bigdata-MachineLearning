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
 * 以固定的速率生成固定格式的数据, 用来测试 Structured Streaming 的性能
 * complete：完整模式，全部输出，必须有聚合才可以使用，
 * append：追加模式，只输出那些将来永远不可能再更新的数据。没有聚合的时候，append和update是一样的，有聚合的时候，一定要有水印才能使用append。
 * update：只输出更新的模式，只输出变化的部分，也就是哪一条数据发生了变化，就输出哪一条数据
 */
public class RateSourceStructuredStreaming {
    public static void main(String[] args) throws Exception {
        //获取sparkSession
        SparkSession sparkSession= SparkSession.builder().master("local[*]").appName("WordCountSocketStructuredStreaming").getOrCreate();
        Dataset<Row> streamingdata = sparkSession.readStream()  //读一个流数据，lines其实就是一个输入表
                .format("rate") // 设置数据源为 rate
                .option("rowPerSecond",100)// 设置每秒产生的数据的条数, 默认是 1
                .option("rampUpTime",1) // 设置多少秒到达指定速率 默认为 0
                .option("numPartitions",3)  // 设置分区数  默认是 spark 的默认并行度
                .load();

        StreamingQuery result = streamingdata.writeStream().format("console")  //指定输出到哪，这里输出到控制台
                .outputMode("update") //输出模式，输出模式有三种：complete append  update
                .option("truncate",false)//很长的字段显示不完全的也不省略了，设置全部显示出来
                .trigger(Trigger.ProcessingTime("60 seconds")) //多长时间触发一次，如果不写的话就是尽快处理
                .start();
        result.awaitTermination();//阻止当前线程退出
        sparkSession.stop();
    }
}
