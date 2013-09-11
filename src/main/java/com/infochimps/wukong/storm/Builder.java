package com.infochimps.wukong.storm;

import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class Builder {

    static Logger LOG = Logger.getLogger(StateBuilder.class);    

    public Builder() {
    }

    public Boolean valid() {
	return true;
    }

    public void logInfo() {
    }
    
    static public String usage() {
	return "";
    }
    
    public static String ZOOKEEPER_HOSTS		= "wukong.zookeeper.hosts";
    public static String DEFAULT_ZOOKEEPER_HOSTS	= "localhost";
    public String zookeeperHosts() {
	return prop(ZOOKEEPER_HOSTS, DEFAULT_ZOOKEEPER_HOSTS);
    }

    public static String KAFKA_HOSTS			= "wukong.kafka.hosts";
    public static String DEFAULT_KAFKA_HOSTS		= "localhost";
    public List<String> kafkaHosts() {
	ArrayList<String> kh = new ArrayList();
	for (String host : prop(KAFKA_HOSTS, DEFAULT_KAFKA_HOSTS).split(",")) {
	    kh.add(host);
	}
	return kh;
    }
    
    public String prop(String key, String defaultValue) {
	if (System.getProperty(key) == null) {
	    System.setProperty(key, defaultValue);
	}
	return prop(key);
    }

    public String prop(String key) {
	return System.getProperty(key);
    }
    
} 
