package cn.hoob.sparkStreaming.kafka;


import cn.hoob.sparkStreaming.utils.OffsetUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.*;

/**
 * StreamingContext  wordcount
 */
public class KafkaDirectWordCountOffsetByMysql {
    public static void main(String[] args) throws Exception {
        //创建SparkConf对象
        SparkConf conf=new SparkConf()
                .setAppName("KafkaDirectWordCountOffsetByRedis")
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
        // latest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        // earliest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        // none  当各分区下都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
        kafkaParams.put("auto.offset.reset", "latest");//
        //true 偶然false  控制kafka是否自动提交偏移量
        kafkaParams.put("enable.auto.commit",false);//


        //创建Kafka的topics ，里面可以填多个topic
        Collection<String> topics=Arrays.asList("hoobtest");
        //Topic分区
        //Map<TopicPartition, Long> offsets = new HashMap<>();
        //offsets.put(new TopicPartition("topic1", 0), 2L);
        // JavaInputDStream<ConsumerRecord<Object, Object>> lines = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
        //        ConsumerStrategies.Subscribe(topics, kafkaParams,offsets));
        //检查redis里面是否存有对应的便宜量没有就从头开始读有就接着偏移量继续读
        Map<TopicPartition, Long>offsets= OffsetUtil.getOffsetMapByMsql("hoobtest2",topics.toArray()[0].toString());
        //创建DStream
        JavaInputDStream<ConsumerRecord<Object, Object>> stream =null;
        if(offsets==null||offsets.isEmpty()){
            System.out.println("read data from start");
            stream  = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topics, kafkaParams));
        }else{
            System.out.println("read data from offset");
            stream  = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topics, kafkaParams,offsets));
        }


        JavaInputDStream<ConsumerRecord<Object, Object>> finalStream = stream;
        stream.foreachRDD(rdd -> {
                    OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

                    //拆分Kafka topic里面的数据
                    rdd.flatMap(new FlatMapFunction<ConsumerRecord<Object, Object>, String>() {
                        @Override
                        public Iterator<String> call(ConsumerRecord<Object, Object> line) throws Exception {
                            return Arrays.asList(line.value().toString().split(" ")).iterator();
                        }
                    }).flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
                        @Override
                        public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                            List<Tuple2<String, Integer>> list = new ArrayList<>();
                            String[] split = s.split(" ");

                            for (String string : split) {
                                list.add(new Tuple2<>(string, 1));
                            }
                            return list.iterator();
                        }
                    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                        @Override
                        public Integer call(Integer v1, Integer v2) throws Exception {
                            return v1 + v2;
                        }
                    });
                    // some time later, after outputs have completed
                    //((CanCommitOffsets) finalStream.inputDStream()).commitAsync(offsetRanges);
                    OffsetUtil.setOffsetRangesByMsql("hoobtest2",offsetRanges);

                }
        );
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }






}
