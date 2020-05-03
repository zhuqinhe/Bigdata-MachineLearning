package cn.hoob.kafuka;


import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.codec.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * StreamingContext  wordcount
 */
public class KafkaDirectWordCount {
    public static void main(String[] args) throws Exception {
        //创建SparkConf对象
        SparkConf conf=new SparkConf()
                .setAppName("KafkaDirectWordCount")
                .setMaster("local[*]");
        //创建JavaStreamingContext对象
        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(5));
        //kafka的brokers
        String brokers="node2:9092";
        //创建Kafka参数Map
        Map<String,Object> kafkaParams=new HashMap<>();
        kafkaParams.put("metadata.broker.list",brokers);
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "hoobtest2");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("auto.offset.reset", "latest");

        //创建Kafka的topics ，里面可以填多个topic
        Collection<String> topics=Arrays.asList("hoobtest");

        //创建DStream
        JavaInputDStream<ConsumerRecord<Object, Object>> lines = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));

        //拆分Kafka topic里面的数据
        JavaDStream<String> linesSplit=lines.flatMap(new FlatMapFunction<ConsumerRecord<Object, Object>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<Object, Object> line) throws Exception {
                return Arrays.asList(line.value().toString().split(" ")).iterator();
            }
        });

        //单词映射成（word，1）的形式

        JavaPairDStream<String,Integer> word=linesSplit.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String everyWord) throws Exception {
                return new Tuple2<String,Integer>(everyWord,1);
            }
        });
        JavaPairDStream<String,Integer> wordsCount=word.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        wordsCount.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }






}
