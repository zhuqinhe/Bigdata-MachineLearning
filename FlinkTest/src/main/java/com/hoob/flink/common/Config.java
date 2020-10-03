package com.hoob.flink.common;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hoob.flink.utils.ConfFactory;

/**
 * @author zhuqinhe
 */
public class Config {
	private static Logger log = LoggerFactory.getLogger(Config.class);
	public static String APP_ETC_PATH = "/opt/hoob/NE/etc/";

	public static FlinkConfig flinkConfig;

	/**
	 * 初始化配置
	 * 
	 * @throws FileNotFoundException
	 */
	public static void initConfig() throws FileNotFoundException {
		log.info("Config initConfig ...");
		String filePath = APP_ETC_PATH + "config.properties";
		File file = new File(filePath);
		if (!file.exists()) {
			flinkConfig = new FlinkConfig();
			log.info("file [{}] is not exist", filePath);
		} else {
			com.typesafe.config.Config config = null;
			config = ConfFactory.getConfig(filePath);
			flinkConfig = new FlinkConfig(config);
			String kafakaGroupId = config.getString("kafka.group.id");
			if (!StringUtils.isBlank(kafakaGroupId.toString())) {
				Constants.kafkaConfig.setGroupId(kafakaGroupId.toString());
			}
		}
	}

	/**
	 * flink配置
	 * 
	 * @author Administrator
	 *
	 */
	public static class FlinkConfig {
		private int flinkGlobalParallelism;

		/**
		 * 构造方法
		 */
		public FlinkConfig() {
			flinkGlobalParallelism = 4;
		}

		/**
		 * 构造器
		 */
		public FlinkConfig(com.typesafe.config.Config flink_config) {
			if (flink_config == null) {
				flinkGlobalParallelism = 4;
			} else {
				flinkGlobalParallelism = flink_config.getInt("flink.global.parallelism");
			}
		}

		public int getFlinkGlobalParallelism() {
			return flinkGlobalParallelism;
		}

	}

}
