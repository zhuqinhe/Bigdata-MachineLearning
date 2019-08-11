package com.hoob.hadoop;

import org.apache.hadoop.conf.Configuration;

public class TestConfiguration {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.addResource("test_load_config.xml");
		System.out.println(conf.get("name"));
	}

}
