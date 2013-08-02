package com.infochimps.wukong.storm;

import java.io.File;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

import com.infochimps.wukong.storm.TopologyBuilder;

public class TopologySubmitter {

    private static Logger LOG = Logger.getLogger(TopologySubmitter.class);
    
    private TopologyBuilder builder;
    private Config          config;

    public static void main(String[] args) throws Exception {
	setPropertiesFromArgsBecauseStupidlyHard(args);
	TopologySubmitter submitter = new TopologySubmitter();
	submitter.setConfig();
	submitter.validate();
	submitter.submit();
	System.exit(0);
    }

    public static void setPropertiesFromArgsBecauseStupidlyHard(String[] args) {
	int numArgs      = args.length;
	int argIndex     = 0;
	boolean isOption = false;
	while (argIndex < numArgs) {
	    String arg = args[argIndex];
	    if (isOption) {
		setPropertyFromArgBecauseStupidlyHard(arg);
		isOption = false;
	    } else {
		if (arg.matches("-D.+")) {
		    setPropertyFromArgBecauseStupidlyHard(arg.substring(2));
		} else if (arg.matches("-D")) {
		    isOption = true;
		} else {
		    LOG.error("Malformed option: " + arg);
		}
	    }
	    argIndex += 1;
	}
    }

    private static void setPropertyFromArgBecauseStupidlyHard(String arg) {
	String[] parts = arg.split("=");
	if (parts.length >= 2) {
	    String key   = parts[0];
	    String value = arg.substring(key.length() + 1);
	    System.setProperty(key, value);
	} else {
	    LOG.error("Invalid property: " + arg);
	}
    }

    private String prop(String key, String defaultValue) {
	if (System.getProperty(key) == null) {
	    System.setProperty(key, defaultValue);
	}
	return prop(key);
    }

    private String prop(String key) {
	return System.getProperty(key);
    }
    
    public TopologySubmitter() {
	this.builder = new TopologyBuilder();
	this.config  = new Config();
    }

    private void validate() {
	if (!builder.valid()) {
	    System.out.println(usage());
	    System.exit(1);
	}
    }

    public String usage() {
	return "usage: storm jar " + fullyQualifiedClassPath() + " -DOPTION=VALUE ..." + TopologyBuilder.usageArgs();
    }
    
    public File fullyQualifiedClassPath() {
	return new File(TopologySubmitter.class.getProtectionDomain().getCodeSource().getLocation().getPath());
    }
    
    public void setConfig() {
	setDebug();
	setMaxSpoutPending();
	setMaxTaskParallelism();
	setMessageTimeoutSecs();
	setNumAckers();
	setNumWorkers();
	setOptimize();
	setStatsSampleRate();
    }

    public void submit() {
	try {
	    StormSubmitter.submitTopology(builder.topologyName(), config, builder.topology());
	} catch (AlreadyAliveException e) {
	    LOG.error("Topology " + builder.topologyName() + " is already running", e);
	    System.exit(2);
	} catch (InvalidTopologyException e) {
	    LOG.error("Topology " + builder.topologyName() + " is invalid", e);
	    System.exit(3);
	}
    }

    public void setDebug() {
	String value = prop(Config.TOPOLOGY_DEBUG);
	if (! (value == null)) {
	    LOG.info("Setting " + Config.TOPOLOGY_DEBUG + " to " + value);
	    config.setDebug(Boolean.parseBoolean(value));
	}
    }

    public void setMaxSpoutPending() {
	String value = prop(Config.TOPOLOGY_MAX_SPOUT_PENDING);
	if (! (value == null)) {
	    LOG.info("Setting " + Config.TOPOLOGY_MAX_SPOUT_PENDING + " to " + value);
	    config.setMaxSpoutPending(Integer.parseInt(value));
	}
    }

    public void setMaxTaskParallelism() {
	String value = prop(Config.TOPOLOGY_MAX_TASK_PARALLELISM);
	if (! (value == null)) {
	    LOG.info("Setting " + Config.TOPOLOGY_MAX_TASK_PARALLELISM + " to " + value);
	    config.setMaxTaskParallelism(Integer.parseInt(value));
	}
    }

    public void setMessageTimeoutSecs() {
	String value = prop(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS);
	if (! (value == null)) {
	    LOG.info("Setting " + Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS + " to " + value);
	    config.setMessageTimeoutSecs(Integer.parseInt(value));
	}
    }

    public void setNumAckers() {
	String value = prop(Config.TOPOLOGY_ACKER_EXECUTORS);
	if (! (value == null)) {
	    LOG.info("Setting " + Config.TOPOLOGY_ACKER_EXECUTORS + " to " + value);
	    config.setNumAckers(Integer.parseInt(value));
	}
    }

    public void setNumWorkers() {
	String value = prop(Config.TOPOLOGY_WORKERS);
	if (! (value == null)) {
	    LOG.info("Setting " + Config.TOPOLOGY_WORKERS + " to " + value);
	    config.setNumWorkers(Integer.parseInt(value));
	}
    }

    public void setOptimize() {
	String value = prop(Config.TOPOLOGY_OPTIMIZE);
	if (! (value == null)) {
	    LOG.info("Setting " + Config.TOPOLOGY_OPTIMIZE + " to " + value);
	    config.setDebug(Boolean.parseBoolean(value));
	}
    }

    public void setStatsSampleRate() {
	String value = prop(Config.TOPOLOGY_STATS_SAMPLE_RATE);
	if (! (value == null)) {
	    LOG.info("Setting " + Config.TOPOLOGY_STATS_SAMPLE_RATE + " to " + value);
	    config.setStatsSampleRate(Integer.parseInt(value));
	}
    }
}
