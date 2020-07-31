package cn.hoob.machine_learning.feature_transformation;

import org.apache.spark.ml.feature.Binarizer;
import org.apache.spark.ml.feature.ElementwiseProduct;
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
 ElementwiseProduct
 算法介绍：
 ElementwiseProduct按提供的“weight”向量，返回与输入向量元素级别的乘积。
 即是说，按提供的权重分别对输入数据进行缩放，得到输入向量v以及权重向量w的Hadamard积
 **/
public class ElementwiseProductDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("ElementwiseProductDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();


        List<Row> data = Arrays.asList(
                RowFactory.create("a", Vectors.dense(1.0, 2.0, 3.0)),
                RowFactory.create("b", Vectors.dense(4.0, 5.0, 6.0))
        );

        List<StructField> fields = new ArrayList<>(2);
        fields.add(DataTypes.createStructField("id", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("vector", new VectorUDT(), false));

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> dataFrame = sparkSession.createDataFrame(data, schema);

        Vector transformingVector = Vectors.dense(0.0, 1.0, 2.0);

        ElementwiseProduct transformer = new ElementwiseProduct()
                .setScalingVec(transformingVector)
                .setInputCol("vector")
                .setOutputCol("transformedVector");

        transformer.transform(dataFrame).show();
    }
}
