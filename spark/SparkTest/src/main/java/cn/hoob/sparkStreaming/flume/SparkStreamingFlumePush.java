package cn.hoob.sparkStreaming.flume;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author zhuqinhe
 * lume作为日志实时采集的框架，可以与SparkStreaming实时处理框架进行对接，flume实时产生数据，sparkStreaming做实时处理。
 * Spark Streaming对接FlumeNG有两种方式，一种是FlumeNG将消息Push推给Spark Streaming，
 * 还有一种是Spark Streaming从flume 中Poll拉取数据
 * spark-streaming-flume-sink_*.jar放入到flume的lib目录下
 * (如不匹配)从spark安装目录的jars文件夹下找到scala-library-*.jar 包 替换掉flume的lib目录下自带的scala-library-*.jar
 *两个jar包上传至 flume/lib下，并删除旧的avro jar包
 * 
 *flume  conf
a1.sources = r1
a1.sinks = k1
a1.channels = c1
#source
a1.sources.r1.type = spooldir
a1.sources.r1.spoolDir = /root/apps/testdata/flume_log/flume-push-spark
a1.sources.r1.fileHeader = true
#channel
a1.channels.c1.type =memory
a1.channels.c1.capacity = 20000
a1.channels.c1.transactionCapacity=5000
#sinks
a1.sinks.k1.type = avro
# spark 程序的ip地址
a1.sinks.k1.hostname=172.16.13.29
a1.sinks.k1.port = 10000
a1.sinks.k1.batchSize= 2000

#绑定
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
 *
 *
 */
public class SparkStreamingFlumePush {
    public static void main(String[] args) throws Exception {
        //创建SparkConf对象
        SparkConf conf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[*]");
        //创建JavaStreamingContext对象
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));

        //push 模式，flume push
        JavaReceiverInputDStream<SparkFlumeEvent> pollingStream = FlumeUtils.createStream(jsc, "172.16.13.29", 10000);

        //event是flume中传输数据的最小单元，event中数据结构：{"headers":"xxxxx","body":"xxxxxxx"}
        JavaDStream<String> flumeDatas = pollingStream.flatMap(
                new FlatMapFunction<SparkFlumeEvent, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Iterator<String> call(SparkFlumeEvent event) throws Exception {
                        String line = new String(event.event().getBody().array());
                        return Arrays.asList(line.split(" ")).iterator();
                    }
                });

        JavaPairDStream<String, Integer> pairs = flumeDatas.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((v1,v2)->v1+v2);

        wordCounts.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
