package com.infochimps.wukong.storm;

import java.io.File;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

import com.infochimps.wukong.storm.WukongTopologyBuilder;

public class WukongTopologySubmitter {

    private static Logger LOG = Logger.getLogger(WukongTopologySubmitter.class);
    private WukongTopologyBuilder builder;

    public static void main(String[] args) throws Exception {
	setPropertiesFromArgsBecauseStupidlyHard(args);
	WukongTopologySubmitter submitter = new WukongTopologySubmitter();
	submitter.validate();
	submitter.submit();
	System.exit(0);
    }

    public static void setPropertiesFromArgsBecauseStupidlyHard(String[] args) {
	for (String arg : args) {
	    if (arg.startsWith("-D")) {
		String[] parts = arg.substring(2).split("=");
		if (parts.length == 2) {
		    System.setProperty(parts[0].trim(), parts[1].trim());
		}
	    }
	}
    }

    public WukongTopologySubmitter() {
	this.builder = new WukongTopologyBuilder();
    }

    private void validate() {
	if (!builder.valid()) {
	    System.out.println(usage());
	    System.exit(1);
	}
    }

    private String usage() {
	return "usage: storm jar " + fullyQualifiedClassPath() + " " + WukongTopologyBuilder.usageArgs();
    }
    
    private File fullyQualifiedClassPath() {
	return new File(WukongTopologySubmitter.class.getProtectionDomain().getCodeSource().getLocation().getPath());
    }
    
    private Config config() {
	return new Config();
    }

    private void submit() {
	try {
	    StormSubmitter.submitTopology(builder.topologyName(), config(), builder.topology());
	} catch (AlreadyAliveException e) {
	    LOG.error("Topology " + builder.topologyName() + " is already running", e);
	    System.exit(2);
	} catch (InvalidTopologyException e) {
	    LOG.error("Topology " + builder.topologyName() + " is invalid", e);
	    System.exit(3);
	}
    }
}
