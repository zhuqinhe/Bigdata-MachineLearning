package cn.hoob.structured_streaming.testForeachWriter;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.beans.PropertyVetoException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author zhuqinhe
 */
public class TestMysqlForeachWriter extends ForeachWriter implements Serializable {
    private static final String url;
    private static final String user;
    private static final String password;
    private static final boolean isUseConnectionPools;
    private static final int minPoolSize;
    private static final int maxPoolSize;
    private static final int checkoutTimeout;
    private static final int maxStatements;
    private static final int acquireIncrement;
    private static final ComboPooledDataSource ds;
    public Connection connection;
    static {
        ds = new ComboPooledDataSource();
        try {
            ds.setDriverClass("com.mysql.jdbc.Driver");
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }
        url = "jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
        user = "appuser";
        password = "Mysql123+";

        isUseConnectionPools = true;
        minPoolSize = 20;
        maxPoolSize = 1000;
        checkoutTimeout = 5000;
        maxStatements = 1000;
        acquireIncrement = 2;
        ds.setUser(user);
        ds.setPassword(password);
        ds.setJdbcUrl(url);
        ds.setMinPoolSize(minPoolSize);
        ds.setMaxPoolSize(maxPoolSize);
        ds.setMaxStatements(maxStatements);
        ds.setAcquireIncrement(acquireIncrement);
        ds.setCheckoutTimeout(checkoutTimeout);

        ds.setPreferredTestQuery("SELECT 1");
        ds.setIdleConnectionTestPeriod(18000);
        ds.setTestConnectionOnCheckout(true);

    }

    public static synchronized Connection getConnection() throws Exception {
        return (Connection) ds.getConnection();
    }


    @Override
    public boolean open(long partitionId, long version) {
        try {
            connection = getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public void process(Object value) {
        GenericRowWithSchema genericRowWithSchema = (GenericRowWithSchema) value;

        System.out.println(((GenericRowWithSchema) value).get(0).toString()+"-----------"+
                ((GenericRowWithSchema) value).get(1).toString());
        StructType schema=genericRowWithSchema.schema();
        String[] names=schema.fieldNames();
        Object[] values=genericRowWithSchema.values();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO `word_count` ( " +
                    "`word`, `count`) " +
                    "VALUES ( ?, ?) ");
            pstmt.setString(1,values[0].toString());
            pstmt.setLong(2, (Long) values[1]);
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void close(Throwable errorOrNull) {

        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}