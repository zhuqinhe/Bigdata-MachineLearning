package cn.hoob.structured_streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author zhuqinhe
 */
public class FileSourceStructuredStreaming {
    public static void main(String[] args) throws Exception {
        //获取sparkSession
        SparkSession sparkSession= SparkSession.builder().master("local[*]").appName("FileSourceStructuredStreaming").getOrCreate();
        //构建StructType
        List<StructField> fieldList = new ArrayList<>();
        fieldList.add(DataTypes.createStructField("number", DataTypes.StringType, false));
        fieldList.add(DataTypes.createStructField("name", DataTypes.StringType, false));
        fieldList.add(DataTypes.createStructField("type", DataTypes.StringType, false));
        fieldList.add(DataTypes.createStructField("contentKey", DataTypes.StringType, false));
        fieldList.add(DataTypes.createStructField("contentNum", DataTypes.StringType, false));
        fieldList.add(DataTypes.createStructField("num", DataTypes.DoubleType, false));
        StructType rowSchema = DataTypes.createStructType(fieldList);

        Dataset<Row> streamingdata = sparkSession.readStream()  //读一个流数据，lines其实就是一个输入表
               .format("csv")
                .schema(rowSchema)
                .load("D:\\ProgramFiles\\BigData\\data\\cvs")//必须是给个目录，不能是文件
                .groupBy("name")
                .sum("num");
        //或者直接这样
        streamingdata=sparkSession.readStream().schema(rowSchema)
                .csv("D:\\ProgramFiles\\BigData\\data\\cvs")
                .groupBy("name")
                .sum("num");
        streamingdata.writeStream()
                .format("console")
                .outputMode("update")
                .trigger(Trigger.ProcessingTime(1000))//触发器 数字表示毫秒值，0表示立即处理
                .start()
                .awaitTermination();
        sparkSession.stop();
    }
}
