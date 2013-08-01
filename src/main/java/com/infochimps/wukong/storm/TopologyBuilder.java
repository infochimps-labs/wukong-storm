package com.infochimps.wukong.storm;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.spout.IOpaquePartitionedTridentSpout;

import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.KafkaConfig;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.StringScheme;
import backtype.storm.spout.SchemeAsMultiScheme;

import com.infochimps.storm.trident.KafkaState;
import com.infochimps.storm.wukong.WuFunction;

public class TopologyBuilder {
    
    static Logger LOG = Logger.getLogger(TopologyBuilder.class);

    public TopologyBuilder() {
    }

    public StormTopology topology() {
	logTopologyInfo();
	TridentTopology top = new TridentTopology();

	Stream input = top.newStream(topologyName(), spout())
	    .parallelismHint(inputParallelism());

	Stream scaledInput;
	if (dataflowParallelism() > inputParallelism()) {
	    scaledInput = input.shuffle();
	} else {
	    scaledInput = input;
	}

	Stream wukongOutput = scaledInput.each(new Fields("str"), dataflow(), new Fields("_wukong"))
	    .parallelismHint(dataflowParallelism());

	wukongOutput.partitionPersist(state(), new Fields("_wukong"), new KafkaState.Updater());
	
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
	kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	kafkaConfig.fetchSizeBytes = inputBatch();
	kafkaConfig.forceStartOffsetTime(inputOffset());
	return kafkaConfig;
    }

    private WuFunction dataflow() {
	return new WuFunction(dataflowName(), subprocessDirectory(), dataflowEnv());
    }

    public Boolean valid() {
	if (topologyName()    == null) { return false; };
	if (dataflowName()    == null) { return false; };
	if (inputTopic()      == null) { return false; };
	if (outputTopic()     == null) { return false; };
	return true;
    }

    public static String usageArgs() {
	return "-D " + TOPOLOGY_NAME + "=TOPOLOGY_NAME -D " + DATAFLOW_NAME + "=DATAFLOW_NAME -D " + INPUT_TOPIC + "=INPUT_TOPIC -D " + OUTPUT_TOPIC + "=OUTPUT_TOPIC";
    }

    private void logTopologyInfo() {
	logSpoutInfo();
	logDataflowInfo();
	logStateInfo();
    }
    
    private void logSpoutInfo() {
	LOG.info("SPOUT: Reading from offset " + inputOffset() + " of Kafka topic <" + inputTopic() + "> in batches of " + inputBatch() + " with parallelism " + inputParallelism());
    }

    private void logDataflowInfo() {
	LOG.info("WUKONG: Launching dataflow <" + dataflowName() + "> with parallelism " + dataflowParallelism() + " in environment <" + dataflowEnv() + ">" );
    }

    private void logStateInfo() {
	LOG.info("STATE: Writing to Kafka topic <" + outputTopic() + ">");
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

    public static String DATAFLOW_NAME			= "wukong.dataflow";
    public String dataflowName() {
	return prop(DATAFLOW_NAME);
    }

    public static String DATAFLOW_ENV			= "wukong.environment";
    public static String DEFAULT_DATAFLOW_ENV	        = "development";
    public String dataflowEnv() {
	return prop(DATAFLOW_ENV, DEFAULT_DATAFLOW_ENV);
    }
    
    public static String DATAFLOW_PARALLELISM		= "wukong.parallelism";
    public int dataflowParallelism() {
	return Integer.parseInt(prop(DATAFLOW_PARALLELISM, Integer.toString(inputParallelism())));
    }

    public static String INPUT_TOPIC			= "wukong.input.topic";
    public String inputTopic() {
	return prop(INPUT_TOPIC);
    }
    
    public static String INPUT_OFFSET			= "wukong.input.offset";
    public static String DEFAULT_INPUT_OFFSET		= "-1";
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

    public static String OUTPUT_TOPIC_FIELD		= "wukong.output.topic.field";
    public static String DEFAULT_OUTPUT_TOPIC_FIELD	= "_topic";
    public String outputTopicField() {
	return prop(OUTPUT_TOPIC_FIELD, DEFAULT_OUTPUT_TOPIC_FIELD);
    }
    
}
