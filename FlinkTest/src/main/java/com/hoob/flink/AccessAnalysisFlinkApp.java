package com.hoob.flink;

import com.hoob.flink.common.Config;
import com.hoob.flink.common.Constants;
import com.hoob.flink.flink.datasource.AccessInputDataSource;
import com.hoob.flink.flink.filter.AccessLogUserInfoFilter;
import com.hoob.flink.flink.filter.AccessUserModelFilter;
import com.hoob.flink.flink.map.ConverAccessUserToHbaseModelall;
import com.hoob.flink.flink.reduce.AccessUserStatCountReduceFunction;
import com.hoob.flink.flink.reduce.AccessUserStatReduceFunction;
import com.hoob.flink.flink.sink.HbaseSinkForall;
import com.hoob.flink.flink.watermarks.AccessUserEventTimeExtractor;
import com.hoob.flink.model.hdfs.AccessUserInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hoob.flink.flink.map.ConverLogToOttAccessUserInfo;

/**
 * 业务日志处理
 *
 */
public class AccessAnalysisFlinkApp {
	private static final Logger LOGGER = LoggerFactory.getLogger(AccessAnalysisFlinkApp.class);

	/**
	 * 初始化
	 */
	public static void init() {
		try {
			LOGGER.info("init flink app config");
			Log4jLister.init();
			Constants.initConfig();
			Config.initConfig();
		} catch (Exception e) {
			LOGGER.error("init flink app config failed");
			LOGGER.error(e.getMessage(), e);
			System.exit(-1);
		}
	}

	/**
	 * 执行方法
	 * 
	 * @throws Exception
	 */
	public static void runTask() throws Exception {
		try {

			Log4jLister.init();
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			//设置并行度
			env.setParallelism(Constants.appConfig.getParallelism());
			//设置窗口时间使用事件时间
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.registerCachedFile("/opt/hoob/NE/flink/etc/hbase-site.xml", "hbase-site.xml");
			LOGGER.info("start flink app ottAccessLogStat");

			// 接入OTT数据流
			SingleOutputStreamOperator<AccessUserInfo> ottBaseStream = env
					.addSource(AccessInputDataSource.getKafkaDataSource())
					.name("ottSource").rebalance()
					.filter(new AccessLogUserInfoFilter())
					.map(new ConverLogToOttAccessUserInfo())
					.rebalance()
					.filter(new AccessUserModelFilter());


			SingleOutputStreamOperator<AccessUserInfo> ottUserInfo = ottBaseStream.rebalance()
					.assignTimestampsAndWatermarks(new AccessUserEventTimeExtractor(Time.minutes(1)));

			ottUserInfo.name("ott-ne-user-online").keyBy("userId")
					.timeWindow(Time.minutes(5))
					.reduce(new AccessUserStatReduceFunction())
					.rebalance()
					.keyBy("supportKey")
					.timeWindow(Time.minutes(5))
					.reduce(new AccessUserStatCountReduceFunction())
					.rebalance()
					.map(new ConverAccessUserToHbaseModelall())
					.addSink(new HbaseSinkForall(Constants.hbaseConfig)).name("ott-ne-online-sink");

			env.execute("ott_user_online");
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage(), e);
		}
	}

	/**
	 * 主入口
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		init();
		runTask();
	}

}
