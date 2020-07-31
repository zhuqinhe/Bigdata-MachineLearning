package cn.hoob.machine_learning.feature_transformation;

import org.apache.spark.ml.feature.ElementwiseProduct;
import org.apache.spark.ml.feature.SQLTransformer;
import org.apache.spark.ml.linalg.Vector;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 SQLTransformer
 算法介绍：
 SQLTransformer工具用来转换由SQL定义的陈述。目前仅支持SQL语法如"SELECT ...FROM __THIS__ ..."，其中"__THIS__"代表输入数据的基础表。
 选择语句指定输出中展示的字段、元素和表达式，
 支持Spark SQL中的所有选择语句。用户可以基于选择结果使用Spark SQL建立方程或者用户自定义函数。SQLTransformer支持语法示例如下：

 1. SELECTa, a + b AS a_b FROM __THIS__

 2. SELECTa, SQRT(b) AS b_sqrt FROM __THIS__ where a > 5

 3. SELECTa, b, SUM(c) AS c_sum FROM __THIS__ GROUP BY a, b
 **/
public class SQLTransformerDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("SQLTransformerDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(0, 1.0, 3.0),
                RowFactory.create(2, 2.0, 5.0)
        );
        StructType schema = new StructType(new StructField [] {
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("v1", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("v2", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        SQLTransformer sqlTrans = new SQLTransformer().setStatement(
                "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__");

        sqlTrans.transform(df).show();
    }
}
