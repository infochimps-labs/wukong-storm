package com.infochimps.wukong.storm;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

import org.apache.log4j.Logger;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import storm.trident.Stream;
import storm.trident.TridentTopology;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;

public class TopologyBuilder extends Builder {

    private static class CombineMetadata extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
	    String  content    = tuple.getStringByField("content");
            String  metadata   = tuple.getStringByField("metadata");
            Integer lineNumber = tuple.getIntegerByField("linenumber");
	    LOG.debug(String.format("%s\t%s\t%s", metadata, content, lineNumber));
            collector.emit(new Values(String.format("%s\t%s\t%s", metadata, content, lineNumber)));
        }
    }

    private SpoutBuilder    spoutBuilder;
    private DataflowBuilder dataflowBuilder;
    private StateBuilder    stateBuilder;

    static Logger LOG = Logger.getLogger(TopologyBuilder.class);

    public TopologyBuilder() {
	this.spoutBuilder    = new SpoutBuilder();
	this.dataflowBuilder = new DataflowBuilder(spoutBuilder);
	this.stateBuilder    = new StateBuilder();
    }

    @Override
    public Boolean valid() {
	if (topologyName() == null) {
	    LOG.error("Must set a topology name using the " + TOPOLOGY_NAME + " property");
	    return false;
	}
	if (!spoutBuilder.valid()) { return false; }
	if (!dataflowBuilder.valid()) { return false; }
	if (!stateBuilder.valid()) { return false; }
	return true;
    }

    @Override
    public void logInfo() {
	LOG.info("\n");
	spoutBuilder.logInfo();
	dataflowBuilder.logInfo();
	stateBuilder.logInfo();
    }
    
    public StormTopology topology() {
	TridentTopology top = new TridentTopology();

	Stream spoutOutput = top.newStream(topologyName(), spoutBuilder.spout())
	    .parallelismHint(spoutBuilder.inputParallelism());

	Stream possiblyShuffledSpoutOutput;
	if (needToShuffleSpoutOutput()) {
	    possiblyShuffledSpoutOutput = spoutOutput.shuffle();
	} else {
	    possiblyShuffledSpoutOutput = spoutOutput;
	}

	Stream dataflowInput;
	if (spoutBuilder.isBlobSpout()) {
	    dataflowInput = possiblyShuffledSpoutOutput.each(new Fields("content", "metadata", "linenumber"), new CombineMetadata(), new Fields("str"));
	} else {
	    dataflowInput = possiblyShuffledSpoutOutput;
	}
	
	Stream dataflowOutput = dataflowInput.each(new Fields("str"), dataflowBuilder.dataflow(), new Fields("_wukong"))
	    .parallelismHint(dataflowBuilder.dataflowParallelism());

	dataflowOutput.partitionPersist(stateBuilder.state(), new Fields("_wukong"), stateBuilder.updater());
	
	return top.build();
    }

    public static String usage() {
	String s = "\n"
	    + "Dynamically assemble and launch a parametrized Storm topology that\n"
	    + "embeds Wukong dataflow(s).  The current overall \"shape\" of the\n"
	    + "topology is\n"
	    + "\n"
	    + "  spout -> wukong dataflow -> state\n"
	    + "\n"
	    + "The available spouts read from Kafka or S3.  The only available state\n"
	    + "is Kafka.\n"
	    + "\n"
	    + "TOPOLOGY OPTIONS\n"
	    + "\n"
	    + "The following options can be used for any topology:\n"
	    + "\n"
	    + "  " + String.format("%10s", TOPOLOGY_NAME) + "  Name of the Storm topology that will be launched  (Required)\n"
	    + "  " + String.format("%10s", KAFKA_HOSTS) + "  Comma-separated list of Kafka host (and optional port) pairs  (Default: " + DEFAULT_KAFKA_HOSTS + ")\n"
	    + "  " + String.format("%10s", ZOOKEEPER_HOSTS) + "  Comma-separated list of Zookeeper host (and optional port) pairs  (Default: " + DEFAULT_ZOOKEEPER_HOSTS + ")\n"
	    + "\n"
	    + SpoutBuilder.usage()
	    + "\n"
	    + DataflowBuilder.usage()
	    + "\n"
	    + StateBuilder.usage()
	    + "\n";
	return s;
    }

    private Boolean needToShuffleSpoutOutput() {
	return (dataflowBuilder.dataflowParallelism() > spoutBuilder.inputParallelism());
    }

    public static String TOPOLOGY_NAME                  = "wukong.topology";
    public String topologyName() {
	return prop(TOPOLOGY_NAME);
    }
    
}
