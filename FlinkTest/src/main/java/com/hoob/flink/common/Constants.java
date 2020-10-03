package com.hoob.flink.common;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hoob.flink.common.model.AppConfig;
import com.hoob.flink.common.model.HbaseConfig;
import com.hoob.flink.common.model.KafkaConfig;
import com.hoob.flink.utils.ConfFactory;
import com.typesafe.config.Config;

public class Constants {

	private static Logger log = LoggerFactory.getLogger(Constants.class);

	public static String daas_etc_path = "/opt/hoob/NE/flink/etc/";
	public static KafkaConfig kafkaConfig;
	public static AppConfig appConfig;
	public static HbaseConfig hbaseConfig;

	/**
	 * 初始化
	 * 
	 * @throws IOException
	 */
	public static void initConfig() throws IOException {
		log.info("Constants initConfig ...");
		Config config = ConfFactory.getConfig();
		Config topicConfig = ConfFactory.getConfig("/opt/hoob/NE/flink/etc/config.properties");
		kafkaConfig = new KafkaConfig(config.getConfig("kafka"), topicConfig);
		hbaseConfig = new HbaseConfig(config.getConfig("hbase"));
		appConfig = new AppConfig(topicConfig);
		log.info(kafkaConfig.toString());
	}

}
