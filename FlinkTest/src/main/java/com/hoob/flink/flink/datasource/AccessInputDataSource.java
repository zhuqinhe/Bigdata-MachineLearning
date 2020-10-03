package com.hoob.flink.flink.datasource;

import java.util.Arrays;
import java.util.Properties;
import com.hoob.flink.common.Constants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据输入源
 * @author zhuqinhe
 */
public class AccessInputDataSource {
	private static Logger log = LoggerFactory.getLogger(AccessInputDataSource.class);

	public static FlinkKafkaConsumer<String> getKafkaDataSource() {

		String[] kafkaTopics = Constants.kafkaConfig.getTopic().split(",");
		// 设置输入，从kafka读取数据作为flink的输入
		FlinkKafkaConsumer<String> kakkaDataSource = new FlinkKafkaConsumer<>(Arrays.asList(kafkaTopics),
				new SimpleStringSchema(), getKafkaPorperties());
		return kakkaDataSource;
	}

	public static FlinkKafkaConsumer<String> getKafkaConfigServerSource() {

		String[] kafkaTopics = Constants.kafkaConfig.getKafkaConfigServerTopic().split(",");
		// 设置输入，从kafka读取数据作为flink的输入
		FlinkKafkaConsumer<String> kakkaDataSource = new FlinkKafkaConsumer<>(Arrays.asList(kafkaTopics),
				new SimpleStringSchema(), getKafkaPorperties());
		return kakkaDataSource;
	}

	public static Properties getKafkaPorperties() {
		Properties kafkaProps = new Properties();
		try {
			String bootstrapServers = Constants.kafkaConfig.getKafkaBootstrapServer();
			kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Constants.kafkaConfig.getGroupId());
			kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			kafkaProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
			kafkaProps.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "20000");
			kafkaProps.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "19000");
			kafkaProps.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			System.exit(-1);
		}
		return kafkaProps;
	}

}
