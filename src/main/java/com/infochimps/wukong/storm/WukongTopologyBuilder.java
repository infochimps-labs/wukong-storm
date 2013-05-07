package com.infochimps.wukong.storm;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import storm.trident.TridentTopology;
import storm.trident.spout.IOpaquePartitionedTridentSpout;

import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.KafkaConfig;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.StringScheme;

import com.infochimps.storm.trident.KafkaState;

public class WukongTopologyBuilder {
    
    static Logger LOG = Logger.getLogger(WukongTopologyBuilder.class);

    public WukongTopologyBuilder() {
    }

    public StormTopology topology() {
	LOG.info("Reading from Kafka topic <" + inputTopic() + "> with parallelism " + inputParallelism() + " starting from " + inputOffset() + " in batches of " + inputBatch());
	LOG.info("Building topology <" + topologyName() + "> with parallelism " + dataflowParallelism() );
	LOG.info("Will run command: " + Arrays.toString(subprocessArgs()));
	LOG.info("Writing to Kafka topic <" + outputTopic() + ">");
	
	TridentTopology top = new TridentTopology();
	top.newStream(prop(topologyName()), spout())
	    .parallelismHint(inputParallelism())
	    .shuffle()
	    .each(new Fields("str"), new SubprocessFunction(subprocessDirectory(), subprocessEnvironment(), subprocessArgs()), new Fields("_wukong"))
	    .parallelismHint(dataflowParallelism())
	    .shuffle()
	    .partitionPersist(state(), new Fields("_wukong"), new KafkaState.Updater());
	return top.build();
    }

    public KafkaState.Factory state() {
	return new KafkaState.Factory(outputTopic(), zookeeperHosts());
    }
    
    public OpaqueTridentKafkaSpout spout() {
	return new OpaqueTridentKafkaSpout(spoutConfig());
    }

    private TridentKafkaConfig spoutConfig() {
	TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(KafkaConfig.StaticHosts.fromHostString(kafkaHosts(), inputPartitions()), inputTopic());
	kafkaConfig.scheme = new StringScheme();
	kafkaConfig.fetchSizeBytes = inputBatch();
	kafkaConfig.forceStartOffsetTime(inputOffset());
	return kafkaConfig;
    }
    
    public Boolean valid() {
	if (topologyName()    == null) { return false; };
	if (inputTopic()      == null) { return false; };
	if (outputTopic()     == null) { return false; };
	if (subprocessArgs()  == null) { return false; };
	if (subprocessArgs().length == 0) { return false; };
	return true;
    }

    public static String usageArgs() {
	return "-D " + TOPOLOGY_NAME + "=TOPOLOGY_NAME -D " + INPUT_TOPIC + "=INPUT_TOPIC -D " + OUTPUT_TOPIC + "=OUTPUT_TOPIC -D " + COMMAND + "='command to run'";
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
    
    public static String KAFKA_HOSTS			= "wukong.kafka.hosts";
    public static String DEFAULT_KAFKA_HOSTS		= "localhost";
    public List<String> kafkaHosts() {
	ArrayList<String> kh = new ArrayList();
	for (String host : prop(KAFKA_HOSTS, DEFAULT_KAFKA_HOSTS).split(",")) {
	    kh.add(host);
	}
	return kh;
    }
    
    public static String ZOOKEEPER_HOSTS		= "wukong.zookeeper.hosts";
    public static String DEFAULT_ZOOKEEPER_HOSTS	= "localhost";
    public String zookeeperHosts() {
	return prop(ZOOKEEPER_HOSTS, DEFAULT_ZOOKEEPER_HOSTS);
    }
    
    public static String TOPOLOGY_NAME                  = "wukong.topology";
    public String topologyName() {
	return prop(TOPOLOGY_NAME);
    }
    
    public static String DATAFLOW_DIRECTORY             = "wukong.directory";
    public static String DEFAULT_DATAFLOW_DIRECTORY     = System.getProperty("user.dir");
    public String subprocessDirectory() {
	return prop(DATAFLOW_DIRECTORY, DEFAULT_DATAFLOW_DIRECTORY);
    }

    public Map<String,String> subprocessEnvironment() {
	// FIXME
	return new HashMap();
    }

    public static String COMMAND			= "wukong.command";
    public String[] subprocessArgs() {
	return prop(COMMAND, "").split(" +");
    }
    
    public static String DATAFLOW_PARALLELISM		= "wukong.parallelism";
    public static String DEFAULT_DATAFLOW_PARALLELISM	= "1";
    public int dataflowParallelism() {
	return Integer.parseInt(prop(DATAFLOW_PARALLELISM, DEFAULT_DATAFLOW_PARALLELISM));
    }
    
    public static String INPUT_TOPIC			= "wukong.input.topic";
    public String inputTopic() {
	return prop(INPUT_TOPIC);
    }
    
    public static String INPUT_OFFSET			= "wukong.input.offset";
    public static String DEFAULT_INPUT_OFFSET		= "-2";
    public int inputOffset() {
	return Integer.parseInt(prop(INPUT_OFFSET, DEFAULT_INPUT_OFFSET));
    }
    
    public static String INPUT_PARTITIONS		= "wukong.input.partitions";
    public static String DEFAULT_INPUT_PARTITIONS	= "1";
    public int inputPartitions() {
	return Integer.parseInt(prop(INPUT_PARTITIONS, DEFAULT_INPUT_PARTITIONS));
    }
    
    public static String INPUT_BATCH			= "wukong.input.batch";
    public static String DEFAULT_INPUT_BATCH		= "1048576";
    public int inputBatch() {
	return Integer.parseInt(prop(INPUT_BATCH, DEFAULT_INPUT_BATCH));
    }
    
    public static String INPUT_PARALLELISM		= "wukong.input.parallelism";
    public static String DEFAULT_INPUT_PARALLELISM	= "1";
    public int inputParallelism() {
	return Integer.parseInt(prop(INPUT_PARALLELISM, DEFAULT_INPUT_PARALLELISM));
    }
    
    public static String OUTPUT_TOPIC			= "wukong.output.topic";
    public String outputTopic() {
	return prop(OUTPUT_TOPIC);
    }
}
