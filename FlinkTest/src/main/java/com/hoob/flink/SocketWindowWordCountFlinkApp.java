package com.hoob.flink;

import com.hoob.flink.model.WordWithCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author zhuqinhe
 */
public class SocketWindowWordCountFlinkApp {

    public static void main(String[] args) throws Exception {

        //获取执行环节
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取连接socket输入数据
        DataStream<String> text = env.socketTextStream("hdp1", 12315, "\n");

        //解析数据、对数据进行分组、窗口函数和统计个数
        DataStream<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            private static final long serialVersionUID = 6800597108091365154L;

            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                for (String word : value.split(",")) {
                    out.collect(new WordWithCount(word, 1));
                }
            }
        })
                .keyBy("word")
                .timeWindow(Time.minutes(5))
               .reduce((v1, v2) -> new WordWithCount(v1.getWord(), v2.getCount() + v2.getCount()));
           /*    .reduce(new ReduceFunction<WordWithCount>() {
               @Override
               public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                return new WordWithCount(value1.word, value1.count + value2.count);
               }
               });*/

        windowCounts.print().setParallelism(1);
        env.execute("Socket Window WordCount");
    }
}