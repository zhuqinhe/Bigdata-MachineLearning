package cn.hoob.machine_learning.regressor.GeneralizedLinearRegression;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.regression.GeneralizedLinearRegression;
import org.apache.spark.ml.regression.GeneralizedLinearRegressionModel;
import org.apache.spark.ml.regression.GeneralizedLinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Arrays;

/**
 * 广义线性回归
 * <p>
 * 算法介绍：
 * 与线性回归假设输出服从高斯分布不同，广义线性模型（GLMs）指定线性模型的因变量 服从指数型分布。
 * Spark的GeneralizedLinearRegression接口允许指定GLMs包括线性回归、泊松回归、逻辑回归等来处理多种预测问题。
 * 目前 spark.ml仅支持指数型分布家族中的一部分类型，如下：
 * 家族    因变量类型      支持类型
 * 高斯    连续型          Identity*, Log, Inverse
 * 二项    二值型          Logit*, Probit, CLogLog
 * 泊松    计数型          Log*, Identity, Sqrt
 * 伽马    连续型          Inverse*, Idenity, Log
 * 注意目前Spark在 GeneralizedLinearRegression仅支持最多4096个特征，如果特征超过4096个将会引发异常。对于线性回归和逻辑回归，如果模型特征数量会不断增长，则可通过 LinearRegression 和LogisticRegression来训练。
 * GLMs要求的指数型分布可以为正则或者自然形式.
 * Spark的GeneralizedLinearRegression接口提供汇总统计来诊断GLM模型的拟合程度，包括残差、p值、残差、Akaike信息准则及其它。
 * <p>
 * 参数：
 * family:类型：字符串型。含义：模型中使用的误差分布类型。
 * featuresCol:类型：字符串型。含义：特征列名。
 * fitIntercept:类型：布尔型。含义：是否训练拦截对象。
 * labelCol:类型：字符串型。含义：标签列名。
 * link:类型：字符串型。含义：连接函数名，描述线性预测器和分布函数均值之间关系。
 * linkPredictiongCol:类型：字符串型。含义：连接函数（线性预测器列名）。
 * maxIter:类型：整数型。含义：最多迭代次数（>=0）。
 * predictionCol:类型：字符串型。含义：预测结果列名。
 * regParam:类型：双精度型。含义：正则化参数（>=0）。
 * solver:类型：字符串型。含义：优化的求解算法。
 * tol:类型：双精度型。含义：迭代算法的收敛性。
 * weightCol:类型：字符串型。含义：列权重。
 **/
public class GeneralizedLinearRegressionModelApp {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession.builder().appName("GBTClassificationModelApp").
                master("local[*]")
                .config("spark.sql.shuffle.partitions", "2").enableHiveSupport().getOrCreate();

        // Load training data
        Dataset<Row> dataset = sparkSession.read().format("libsvm").
                load("D:\\ProgramFiles\\GitData\\hadoop\\spark\\SparkTest\\src\\main\\data\\mllib\\sample_linear_regression_data.txt");

        GeneralizedLinearRegression glr = new GeneralizedLinearRegression()
                .setFamily("gaussian")
                .setLink("identity")
                .setMaxIter(10)
                .setRegParam(0.3);

//      Fit the model
        GeneralizedLinearRegressionModel model = glr.fit(dataset);

//      Print the coefficients and intercept for generalized linear regression model
        System.out.println("Coefficients: " + model.coefficients());
        System.out.println("Intercept: " + model.intercept());

//      Summarize the model over the training set and print out some metrics
        GeneralizedLinearRegressionTrainingSummary summary = model.summary();
        System.out.println("Coefficient Standard Errors: "
                + Arrays.toString(summary.coefficientStandardErrors()));
        System.out.println("T Values: " + Arrays.toString(summary.tValues()));
        System.out.println("P Values: " + Arrays.toString(summary.pValues()));
        System.out.println("Dispersion: " + summary.dispersion());
        System.out.println("Null Deviance: " + summary.nullDeviance());
        System.out.println("Residual Degree Of Freedom Null: " + summary.residualDegreeOfFreedomNull());
        System.out.println("Deviance: " + summary.deviance());
        System.out.println("Residual Degree Of Freedom: " + summary.residualDegreeOfFreedom());
        System.out.println("AIC: " + summary.aic());
        System.out.println("Deviance Residuals: ");
        summary.residuals().show();
    }
}
