package recommenddemo;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.spark.mllib.recommendation.Rating;

import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/*****
 * 慎用直连，rdd中大量数据查询，直接爆炸，改用连接池
 *
 * ******/
public class MySQLUtlis {

    private static final String url;
    private static final String user;
    private static final String password;
    private static final boolean isUseConnectionPools;
    private static final int minPoolSize;
    private static final int maxPoolSize;
    private static final int checkoutTimeout;
    private static final int maxStatements;
    private static final int acquireIncrement;
    //	private static final int maxIdleTime;
    private static final ComboPooledDataSource ds;

    public static Map<String, Integer> seriesData = new HashMap<String, Integer>();


    static {

        // String _url = CommonUtil.properties.getProperty("jdbc.url");
        //  String _user = CommonUtil.properties.getProperty("jdbc.user");//"appuser";
        //  String _password = CommonUtil.properties.getProperty("jdbc.password");//"appuser";
        //  String _isUseConnectionPools = CommonUtil.properties.getProperty("isUseConnectionPools");//"true";
        // String _minPoolSize = CommonUtil.properties.getProperty("minPoolSize");
        // String _maxPoolSize = CommonUtil.properties.getProperty("maxPoolSize");
        //  String _checkoutTimeout = CommonUtil.properties.getProperty("checkoutTimeout");
        // String _maxStatements = CommonUtil.properties.getProperty("maxStatements");
        //String _maxIdleTime = CommonUtil.properties.getProperty("maxIdleTime");

        url = "jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
        user = "appuser";
        password = "Mysql123+";

        isUseConnectionPools = true;
        minPoolSize = 20;
        maxPoolSize = 1000;
        checkoutTimeout = 5000;
        maxStatements = 1000;
        acquireIncrement = 2;
//		maxIdleTime = _maxIdleTime==null||_maxIdleTime.trim().length() == 0||!_maxIdleTime.matches("\\d+")?
//				18000:Integer.parseInt(_maxIdleTime);
        if (isUseConnectionPools) {
            ds = new ComboPooledDataSource();
            try {
                ds.setDriverClass("com.mysql.jdbc.Driver");
            } catch (PropertyVetoException e) {
                e.printStackTrace();
            }

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
            //ds.setMaxIdleTime(maxIdleTime);
        } else {
            ds = null;
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();

            }
        }

    }

    private static Connection getConnection() throws SQLException {
        if (isUseConnectionPools) {
            return ds.getConnection();
        }

        return DriverManager.getConnection(url, user, password);
    }

    public static Integer getSeriesId(String contentId) throws SQLException {
        if (seriesData.containsKey(contentId)) {
            return seriesData.get(contentId);
        }
        Integer id = null;
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            connection = getConnection();

            pstmt = connection.prepareStatement("select id  from series_id_contentid where contentId=?");

            pstmt.setString(1, contentId);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                id = rs.getInt("id");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (pstmt != null) {
                pstmt.close();
            }
            if (connection != null) {
                connection.close();
            }

        }
        return id;
    }

    public static String getSeriesContentId(int sid) throws SQLException {

        String contentId = null;
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            connection = getConnection();

            pstmt = connection.prepareStatement("select contentId from series_id_contentid where id=?");

            pstmt.setInt(1, sid);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                contentId = rs.getString("contentId");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (pstmt != null) {
                pstmt.close();
            }
            if (connection != null) {
                connection.close();
            }

        }
        return contentId;
    }

    public static Integer getUserid(String userid) throws SQLException {
        Integer id = null;
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            //  connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC", "appuser", "Mysql123+");
            connection = getConnection();

            pstmt = connection.prepareStatement("select id  from get_userid where userid=?");

            pstmt.setString(1, userid);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                id = rs.getInt("id");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (pstmt != null) {
                pstmt.close();
            }
            if (connection != null) {
                connection.close();
            }

        }
        return id;
    }

    public static void initSeriesdata() throws SQLException {
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC", "appuser", "Mysql123+");

            pstmt = connection.prepareStatement("select id,contentId  from series_id_contentid ");


            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                seriesData.put(rs.getString("contentId"), rs.getInt("id"));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (pstmt != null) {
                pstmt.close();
            }
            if (connection != null) {
                connection.close();
            }

        }
    }

    public static void processRecommendData(Rating[] datas) throws SQLException {
        //replace into表示之前有就替换,没有就插入
        Connection connection = getConnection();
        PreparedStatement pstmt = connection.prepareStatement("INSERT INTO recommender (uid,sid,userid,contentId)VALUES(?,?,?,?)ON DUPLICATE KEY UPDATE userid = VALUES(userid),contentId = VALUES(contentId)");
        if (datas == null || datas.length == 0) {
            return;
        }
        for (Rating r : datas) {
            int uid = r.user();
            int sid = r.product();
            String userid = "U00" + uid;
            String contentId = MySQLUtlis.getSeriesContentId(sid);
            pstmt.setInt(1, uid);
            pstmt.setInt(2, sid);
            pstmt.setString(3, userid);
            pstmt.setString(4, contentId);

            pstmt.executeUpdate();
        }


    }

    //测试本地redis服务是否正常
    public static void main(String[] args) throws Exception {
        System.out.println(MySQLUtlis.getSeriesId("00000001000000100001000000000010"));
        System.out.println(MySQLUtlis.getUserid("U00100001"));
    }
}
