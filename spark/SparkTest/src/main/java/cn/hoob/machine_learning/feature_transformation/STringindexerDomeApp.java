package cn.hoob.machine_learning.feature_transformation;


import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.types.DataTypes.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
  STringindexer
 算法介绍：
 StringIndexer将字符串标签编码为标签指标。
 指标取值范围为[0,numLabels]，按照标签出现频率排序，所以出现最频繁的标签其指标为0。
 如果输入列为数值型，我们先将之映射到字符串然后再对字符串的值进行指标。
 如果下游的管道节点需要使用字符串－指标标签，则必须将输入和钻还为字符串－指标列名。
 **/
public class STringindexerDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("STringindexerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(0, "a"),
                RowFactory.create(1, "b"),
                RowFactory.create(2, "c"),
                RowFactory.create(3, "a"),
                RowFactory.create(4, "a"),
                RowFactory.create(5, "c")
        );
        StructType schema = new StructType(new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("category", StringType, false)
        });
        Dataset<Row> df = sparkSession.createDataFrame(data, schema);
        StringIndexer indexer = new StringIndexer()
                .setInputCol("category")
                .setOutputCol("categoryIndex");
        Dataset<Row> indexed = indexer.fit(df).transform(df);
        indexed.show();
    }
}
