package cn.hoob.sparksql;

import org.apache.avro.ipc.specific.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Dateset<T> 转化 DateSet<ROW>
 */
public class SQLWordCountStructType {
    public static void main(String[] args) throws Exception {

        //创建SparkSession
        SparkSession sparkSession= SparkSession.builder().appName("SQLWordCount").master("local[*]").getOrCreate();

        //(指定以后从哪里)读数据，是lazy
        //Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
        //dataset只有一列，默认这列叫value
        Dataset<String> dataset= sparkSession.read().textFile("hdfs://node1:9000/wordcount/input");
        //由于Dataset接口没有提供Iterator，无法实现相关逻辑，这里换成rdd来实现
        JavaRDD<String> wordsrdd= dataset.toJavaRDD().flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        //构建StructType
        List<StructField> fieldList = new ArrayList<>();
        fieldList.add(DataTypes.createStructField("word", DataTypes.StringType, false));
        StructType rowSchema = DataTypes.createStructType(fieldList);
        //构建detaset
        Dataset<String> wordsDateset = sparkSession.createDataset(wordsrdd.rdd(),Encoders.STRING());

        //RowEncoder
        ExpressionEncoder<Row> rowEncoder = RowEncoder.apply(rowSchema);
        //Dataset<T> 转化成JavaRDD<ROW>
        Dataset<Row> wordsDatesetROW = wordsDateset.map(
                (MapFunction<String, Row>) word -> {
                    List<Object> objectList = new ArrayList<>();
                    objectList.add(word);
                    return RowFactory.create(objectList.toArray());
                },
                rowEncoder
        );


        //注册视图
        wordsDatesetROW.createOrReplaceTempView("words");

        //执行SQL（Transformation，lazy）
        Dataset<Row> dateframe= sparkSession.sql("SELECT  word, COUNT(*) counts FROM words GROUP BY word ORDER BY counts DESC");

        //执行Action
        dateframe.show(10);

        sparkSession.stop();

    }
}
