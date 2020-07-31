package cn.hoob.machine_learning.feature_transformation;


import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 VectorAssembler
 算法介绍：
 VectorAssembler是一个转换器，它将给定的若干列合并为一列向量。它可以将原始特征和一系列通过其他转换器得到的特征合并为单一的特征向量，
 来训练如逻辑回归和决策树等机器学习算法。
 VectorAssembler可接受的输入列类型：数值型、布尔型、向量型。输入列的值将按指定顺序依次添加到一个新向量中
 **/
public class VectorAssemblerDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("VectorAssemblerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        StructType schema = createStructType(new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("hour", IntegerType, false),
                createStructField("mobile", DoubleType, false),
                createStructField("userFeatures", new VectorUDT(), false),
                createStructField("clicked", DoubleType, false)
        });
        Row row = RowFactory.create(0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0);
        Dataset<Row> dataset = sparkSession.createDataFrame(Arrays.asList(row), schema);

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"hour", "mobile", "userFeatures"})
                .setOutputCol("features");

        Dataset<Row> output = assembler.transform(dataset);
        System.out.println(output.select("features", "clicked").first());
    }
}
