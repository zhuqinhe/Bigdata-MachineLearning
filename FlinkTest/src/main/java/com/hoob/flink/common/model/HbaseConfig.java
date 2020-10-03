package com.hoob.flink.common.model;

import java.io.Serializable;

import com.typesafe.config.Config;

public class HbaseConfig implements Serializable {
	private String hbaseZookeeperQuorum = null;
	private int hbaseZookeeperPropertyClientPort = 0;

	public String getHbaseZookeeperQuorum() {
		return hbaseZookeeperQuorum;
	}

	public void setHbaseZookeeperQuorum(String hbaseZookeeperQuorum) {
		this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
	}

	public int getHbaseZookeeperPropertyClientPort() {
		return hbaseZookeeperPropertyClientPort;
	}

	public void setHbaseZookeeperPropertyClientPort(int hbaseZookeeperPropertyClientPort) {
		this.hbaseZookeeperPropertyClientPort = hbaseZookeeperPropertyClientPort;
	}

	/**
	 * init
	 * 
	 * @param config
	 */
	public HbaseConfig(Config config) {
		String quorum = config.getString("hbase.zookeeper.quorum");
		int port = config.getInt("hbase.zookeeper.property.clientPort");
		this.hbaseZookeeperQuorum = quorum;
		this.hbaseZookeeperPropertyClientPort = port;

	}

}
