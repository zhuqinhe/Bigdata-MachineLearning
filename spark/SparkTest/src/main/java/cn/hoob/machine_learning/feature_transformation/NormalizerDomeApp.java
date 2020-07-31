package cn.hoob.machine_learning.feature_transformation;


import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
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
 Normalizer（正则化）
 算法介绍：
 Normalizer是一个转换器，它可以将多行向量输入转化为统一的形式。参数为p（默认值：2）来指定正则化中使用的p-norm。
 正则化操作可以使输入数据标准化并提高后期学习算法的效果。
 下面的例子展示如何读入一个libsvm格式的数据，然后将每一行转换为 以及 形式
 **/
public class NormalizerDomeApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("IndexToStringDomeApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        Dataset<Row> dataFrame = sparkSession.read().format("libsvm").
                load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main\\data\\mllib\\sample_libsvm_data.txt");

        Normalizer normalizer = new Normalizer()
                .setInputCol("features")
                .setOutputCol("normFeatures")
                .setP(1.0);

        Dataset<Row> l1NormData = normalizer.transform(dataFrame);
        l1NormData.show();

        Dataset<Row> lInfNormData =
                normalizer.transform(dataFrame, normalizer.p().w(Double.POSITIVE_INFINITY));
        lInfNormData.show();
    }
}
