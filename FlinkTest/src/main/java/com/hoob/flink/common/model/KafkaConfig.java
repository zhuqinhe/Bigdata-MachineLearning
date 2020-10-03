package com.hoob.flink.common.model;

import com.typesafe.config.Config;

public class KafkaConfig {
	private String kafkaBootstrapServer;
	private String topic;
	private String groupId;
	private String kafkaConfigServerTopic = "config_server";

	/**
	 * kafka配置构造器
	 * 
	 * @param kafkaConfig
	 * @param topicConfig
	 */
	public KafkaConfig(Config kafkaConfig, Config topicConfig) {
		// kafkaIp = kafkaConfig.getString("kafka.ip");
		// kafkaPort = kafkaConfig.getInt("kafka.port");
		kafkaBootstrapServer = kafkaConfig.getString("kafka.bootstrap.server");
		topic = topicConfig.getString("kafka.topic");
		groupId = topicConfig.getString("kafka.group.id");
	}

	public String getKafkaBootstrapServer() {
		return kafkaBootstrapServer;
	}

	public void setKafkaBootstrapServer(String kafkaBootstrapServer) {
		this.kafkaBootstrapServer = kafkaBootstrapServer;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	@Override
	public String toString() {
		return "KafkaConfig{" +
				"kafkaBootstrapServer='" + kafkaBootstrapServer + '\'' +
				", topic='" + topic + '\'' +
				", groupId='" + groupId + '\'' +
				", kafkaConfigServerTopic='" + kafkaConfigServerTopic + '\'' +
				'}';
	}

	public String getKafkaConfigServerTopic() {
		return kafkaConfigServerTopic;
	}

	public void setKafkaConfigServerTopic(String kafkaConfigServerTopic) {
		this.kafkaConfigServerTopic = kafkaConfigServerTopic;
	}

}
