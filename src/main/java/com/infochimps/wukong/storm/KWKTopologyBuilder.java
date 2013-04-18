package com.infochimps.wukong.storm;

import java.util.Map;
import java.util.HashMap;
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

public class KWKTopologyBuilder {
    
    static Logger LOG = Logger.getLogger(KWKTopologyBuilder.class);

    public static String DATAFLOW_NAME			= "dataflow.name";
    public String dataflowName() {
	return prop(DATAFLOW_NAME);
    }
    
    public static String DATAFLOW_DIRECTORY             = "dataflow.directory";
    public static String DEFAULT_DATAFLOW_DIRECTORY     = System.getProperty("user.dir");
    public String dataflowDirectory() {
	return prop(DATAFLOW_DIRECTORY, DEFAULT_DATAFLOW_DIRECTORY);
    }

    public Map<String,String> dataflowEnvironment() {
	return new HashMap();
    }
    
    public static String DATAFLOW_ARGS			= "dataflow.args";
    public static String DEFAULT_DATAFLOW_ARGS		= "";
    public String[] dataflowArgs() {
	return prop(DATAFLOW_ARGS, DEFAULT_DATAFLOW_ARGS).split(" +");
    }
    
    public static String DATAFLOW_PARALLELISM		= "dataflow.parallelism";
    public static String DEFAULT_DATAFLOW_PARALLELISM	= "1";
    public int dataflowParallelism() {
	return Integer.parseInt(prop(DATAFLOW_PARALLELISM, DEFAULT_DATAFLOW_PARALLELISM));
    }

    public static String KAFKA_HOSTS			= "dataflow.kafka.hosts";
    public static String DEFAULT_KAFKA_HOSTS		= "localhost";
    public List<String> kafkaHosts() {
	ArrayList<String> kh = new ArrayList();
	for (String host : prop(KAFKA_HOSTS, DEFAULT_KAFKA_HOSTS).split(",")) {
	    kh.add(host);
	}
	return kh;
    }
    
    public static String ZOOKEEPER_HOSTS		= "dataflow.zookeeper.hosts";
    public static String DEFAULT_ZOOKEEPER_HOSTS	= "localhost";
    public String zookeeperHosts() {
	return prop(ZOOKEEPER_HOSTS, DEFAULT_ZOOKEEPER_HOSTS);
    }
    
    public static String INPUT_TOPIC			= "dataflow.input.topic";
    public String inputTopic() {
	return prop(INPUT_TOPIC);
    }
    
    public static String INPUT_OFFSET			= "dataflow.input.offset";
    public static String DEFAULT_INPUT_OFFSET		= "-2";
    public int inputOffset() {
	return Integer.parseInt(prop(INPUT_OFFSET, DEFAULT_INPUT_OFFSET));
    }
    
    public static String INPUT_PARTITIONS		= "dataflow.input.partitions";
    public static String DEFAULT_INPUT_PARTITIONS	= "1";
    public int inputPartitions() {
	return Integer.parseInt(prop(INPUT_PARTITIONS, DEFAULT_INPUT_PARTITIONS));
    }
    
    public static String INPUT_BATCH			= "dataflow.input.batch";
    public static String DEFAULT_INPUT_BATCH		= "1048576";
    public int inputBatch() {
	return Integer.parseInt(prop(INPUT_BATCH, DEFAULT_INPUT_BATCH));
    }
    
    public static String INPUT_PARALLELISM		= "dataflow.input.parallelism";
    public static String DEFAULT_INPUT_PARALLELISM	= "1";
    public int inputParallelism() {
	return Integer.parseInt(prop(INPUT_PARALLELISM, DEFAULT_INPUT_PARALLELISM));
    }
    
    public static String OUTPUT_TOPIC			= "dataflow.output.topic";
    public String outputTopic() {
	return prop(OUTPUT_TOPIC);
    }
    
    public static String OUTPUT_PARALLELISM		= "dataflow.output.parallelism";
    public static String DEFAULT_OUTPUT_PARALLELISM	= "1";
    public int outputBatch() {
	return Integer.parseInt(prop(OUTPUT_PARALLELISM, DEFAULT_OUTPUT_PARALLELISM));
    }

    public KWKTopologyBuilder() {
    }

    public Boolean valid() {
	if (dataflowName() == null) { return false; };
	if (inputTopic()   == null) { return false; };
	if (outputTopic()  == null) { return false; };
	return true;
    }

    public static String usageArgs() {
	return "-D " + DATAFLOW_NAME + "=DATAFLOW_NAME -D " + INPUT_TOPIC + "=INPUT_TOPIC -D " + OUTPUT_TOPIC + "=OUTPUT_TOPIC";
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

    private TridentKafkaConfig spoutConfig() {
	TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(KafkaConfig.StaticHosts.fromHostString(kafkaHosts(), inputPartitions()), inputTopic());
	kafkaConfig.scheme = new StringScheme();
	kafkaConfig.fetchSizeBytes = inputBatch();
	kafkaConfig.forceStartOffsetTime(inputOffset());
	return kafkaConfig;
    }
    
    public OpaqueTridentKafkaSpout spout() {
	return new OpaqueTridentKafkaSpout(spoutConfig());
    }

    public KafkaState.Factory state() {
	return new KafkaState.Factory(outputTopic(), zookeeperHosts());
    }

    public StormTopology topology() {
	TridentTopology top = new TridentTopology();
	top.newStream(prop(dataflowName()), spout())
	    .parallelismHint(inputParallelism())
	    .shuffle()
	    .each(new Fields("str"), new SubprocessFunction(dataflowDirectory(), dataflowEnvironment(), dataflowArgs()), new Fields("_wukong"))
	    .parallelismHint(dataflowParallelism())
	    .shuffle()
	    .partitionPersist(state(), new Fields("_wukong"), new KafkaState.Updater());
	return top.build();
    }
}
