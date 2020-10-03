package com.hoob.flink.utils;

import java.io.File;
import java.io.FileNotFoundException;

import com.hoob.flink.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @author zhuqinhe
 */
public abstract class ConfFactory {

	private static Config config;

	private static Logger log = LoggerFactory.getLogger(ConfFactory.class);

	public static Config getConfig() throws FileNotFoundException {
		return getConfig(Constants.daas_etc_path + "config.conf");
	}

	public static Config getConfig(String path) throws FileNotFoundException {
		log.info("load configFile[{}]", path);
		File file = new File(path);
		if (!file.exists()) {
			log.error(path + " is NOT exists!");
			throw new FileNotFoundException("file[" + path + "] not exists");
		}
		config = ConfigFactory.parseFile(file).resolve();
		return config;
	}

}
