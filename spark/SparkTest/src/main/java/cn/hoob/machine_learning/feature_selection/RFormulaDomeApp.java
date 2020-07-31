package cn.hoob.machine_learning.feature_selection;


import avro.shaded.com.google.common.collect.Lists;
import org.apache.spark.ml.attribute.Attribute;
import org.apache.spark.ml.attribute.AttributeGroup;
import org.apache.spark.ml.attribute.NumericAttribute;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.feature.VectorSlicer;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import static org.apache.spark.sql.types.DataTypes.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 RFormula
 算法介绍：
 RFormula通过R模型公式来选择列。支持R操作中的部分操作，包括‘~’, ‘.’, ‘:’, ‘+’以及‘-‘，基本操作如下：
 1. ~分隔目标和对象
 2. +合并对象，“+ 0”意味着删除空格
 3. :交互（数值相乘，类别二值化）
 4. . 除了目标外的全部列
 假设a和b为两列：
 1. y ~ a + b表示模型y ~ w0 + w1 * a +w2 * b其中w0为截距，w1和w2为相关系数。
 2. y ~a + b + a:b – 1表示模型y ~ w1* a + w2 * b + w3 * a * b，其中w1，w2，w3是相关系数。
 RFormula产生一个向量特征列以及一个double或者字符串标签列。如果类别列是字符串类型，
 它将通过StringIndexer转换为double类型。如果标签列不存在，则输出中将通过规定的响应变量创造一个标签列
 **/
public class RFormulaDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("RFormulaDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        StructType schema = createStructType(new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("country", StringType, false),
                createStructField("hour", IntegerType, false),
                createStructField("clicked", DoubleType, false)
        });

        List<Row> data = Arrays.asList(
                RowFactory.create(7, "US", 18, 1.0),
                RowFactory.create(8, "CA", 12, 0.0),
                RowFactory.create(9, "NZ", 15, 0.0)
        );

        Dataset<Row> dataset = sparkSession.createDataFrame(data, schema);
        RFormula formula = new RFormula()
                .setFormula("clicked ~ country + hour")
                .setFeaturesCol("features")
                .setLabelCol("label");
        Dataset<Row> output = formula.fit(dataset).transform(dataset);
        output.select("features", "label").show();

    }
}
