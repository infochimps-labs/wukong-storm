package com.infochimps.wukong.storm;

import org.apache.log4j.Logger;

import com.infochimps.storm.wukong.WuFunction;

public class DataflowBuilder extends Builder {

    static Logger LOG = Logger.getLogger(DataflowBuilder.class);

    private SpoutBuilder spoutBuilder;

    public DataflowBuilder(SpoutBuilder spoutBuilder) {
	this.spoutBuilder = spoutBuilder;
    }

    @Override
    public Boolean valid() {
	if (dataflowName() == null) {
	    LOG.error("Must set a dataflow name using the " + DATAFLOW_NAME + " property");
	    return false;
	};
	return true;
    }

    @Override
    public void logInfo() {
	LOG.info("DATAFLOW: Launching Wukong dataflow <" + dataflowName() + "> with parallelism " + dataflowParallelism() + " in environment <" + dataflowEnv() + ">" );
    }

    public static String usage() {
	String s = "DATAFLOW OPTIONS\n"
	    + "\n"
	    + "The following options can be applied to the dataflow connecting the spout to the state:\n"
	    + "\n"
	    + "  " + String.format("%10s", DATAFLOW_NAME) + "  Name of the Wukong dataflow to launch (Required)\n"
	    + "  " + String.format("%10s", DATAFLOW_ENV) + "  Wukong environment (Default: " + DEFAULT_DATAFLOW_ENV + ")\n"
	    + "  " + String.format("%10s", BOLT_COMMAND) + "  The command-line to execute within a Storm bolt (Required)\n"
	    + "  " + String.format("%10s", DATAFLOW_DIRECTORY) + "  The directory within which to execute the command-line (Default: " + DEFAULT_DATAFLOW_DIRECTORY + ")\n"
	    + "  " + String.format("%10s", DATAFLOW_PARALLELISM) + "  Parallelism hint for Wukong dataflow Trident function (Default: same as --input_parallelism)\n";
	return s;
    }
    
    public WuFunction dataflow() {
	return new WuFunction(dataflowName(), subprocessDirectory(), dataflowEnv());
    }

    public static String DATAFLOW_DIRECTORY             = "wukong.directory";
    public static String DEFAULT_DATAFLOW_DIRECTORY     = System.getProperty("user.dir");
    public String subprocessDirectory() {
	return prop(DATAFLOW_DIRECTORY, DEFAULT_DATAFLOW_DIRECTORY);
    }

    public static String DATAFLOW_NAME			= "wukong.dataflow";
    public String dataflowName() {
	return prop(DATAFLOW_NAME);
    }

    // This is actually used directly by WuFunction but it's listed
    // here for completeness since it is set by the Ruby code.
    public static String BOLT_COMMAND                   = "wukong.command";

    public static String DATAFLOW_ENV			= "wukong.environment";
    public static String DEFAULT_DATAFLOW_ENV	        = "development";
    public String dataflowEnv() {
	return prop(DATAFLOW_ENV, DEFAULT_DATAFLOW_ENV);
    }
    
    public static String DATAFLOW_PARALLELISM		= "wukong.parallelism";
    public int dataflowParallelism() {
	return Integer.parseInt(prop(DATAFLOW_PARALLELISM, Integer.toString(spoutBuilder.inputParallelism())));
    }
    
}
