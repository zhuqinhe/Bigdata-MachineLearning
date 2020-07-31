package cn.hoob.machine_learning.feature_selection;


import avro.shaded.com.google.common.collect.Lists;
import org.apache.spark.ml.attribute.Attribute;
import org.apache.spark.ml.attribute.AttributeGroup;
import org.apache.spark.ml.attribute.NumericAttribute;
import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.ml.feature.VectorSlicer;
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
import java.util.Arrays;
import java.util.List;

/**
 ChiSqSelector
 算法介绍：
 ChiSqSelector代表卡方特征选择。
 它适用于带有类别特征的标签数据。
 ChiSqSelector根据类别的独立卡方2检验来对特征排序，然后选取类别标签主要依赖的特征。
 它类似于选取最有预测能力的特征
 **/
public class ChiSqSelectorDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("ChiSqSelectorDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
                RowFactory.create(8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
                RowFactory.create(9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
                new StructField("clicked", DataTypes.DoubleType, false, Metadata.empty())
        });

        Dataset<Row> df = sparkSession.createDataFrame(data, schema);

        ChiSqSelector selector = new ChiSqSelector()
                .setNumTopFeatures(1)
                .setFeaturesCol("features")
                .setLabelCol("clicked")
                .setOutputCol("selectedFeatures");

        Dataset<Row> result = selector.fit(df).transform(df);
        result.show();
    }
}
